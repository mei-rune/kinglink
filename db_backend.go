package kinglink

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/runner-mei/log"
	_ "github.com/ziutek/mymysql/godrv"
)

// A job object that is persisted to the database.
// Contains the work object as a YAML field.
type dbBackend struct {
	dbDrv     string
	dbURL     string
	tablename string
	isOwer    bool
	conn      *sql.DB

	minPriority int
	maxPriority int
	maxRunTime  time.Duration

	insertSQLString       string
	clearLocksSQLString   string
	retryNoErrorSQLString string
	retryErrorSQLString   string
	replyErrorSQLString   string
	deleteSQLString       string
}

func (backend *dbBackend) Conn() *sql.DB {
	return backend.conn
}

func (backend *dbBackend) Close() error {
	if backend.isOwer {
		return backend.conn.Close()
	}
	return nil
}

// When a worker is exiting, make sure we don't have any locked jobs.
func (backend *dbBackend) clearLocks(workerName string) error {
	// "UPDATE "+backend.tablename+" SET locked_by = NULL, locked_at = NULL WHERE locked_by = $1"
	_, e := backend.conn.Exec(backend.clearLocksSQLString, workerName)
	return I18nError(backend.dbDrv, e)
}

func (backend *dbBackend) log(ctx context.Context, sqlstring string, args []interface{}, err error) {
	if err != nil {
		log.For(ctx).Info(backend.insertSQLString, log.Stringer("args", log.SQLArgs(args)), log.Error(err))
	} else {
		log.For(ctx).Info(backend.insertSQLString, log.Stringer("args", log.SQLArgs(args)))
	}
}

func (backend *dbBackend) Enqueue(ctx context.Context, job *Job) error {
	queue := &job.Queue
	if job.Queue == "" {
		queue = nil
	}

	runAt := &job.RunAt
	if job.RunAt.IsZero() {
		runAt = nil
	}
	deadline := &job.Deadline
	if job.Deadline.IsZero() {
		deadline = nil
	}

	_, err := backend.conn.ExecContext(ctx, backend.insertSQLString, job.Priority, job.MaxRetry, queue, job.UUID, job.Type, &job.Payload, job.Timeout, deadline, runAt)
	backend.log(ctx, backend.insertSQLString, []interface{}{job.Priority, job.MaxRetry, queue, job.UUID, job.Type, &job.Payload, job.Timeout, deadline, runAt}, err)
	return err
}

const fieldsSqlString = " id, priority, max_retry, retried, queue, uuid, type, payload, timeout, deadline, run_at, locked_at, locked_by, failed_at, last_error, created_at, updated_at "

func (backend *dbBackend) readJobFromRow(row interface {
	Scan(dest ...interface{}) error
}) (*Job, error) {
	job := &Job{}
	var queue sql.NullString
	var uuid sql.NullString
	var retried sql.NullInt64
	var timeout sql.NullInt64
	var deadline NullTime
	var runAt NullTime
	var lockedAt NullTime
	var lockedBy sql.NullString
	var failedAt NullTime
	var lastError sql.NullString
	var createdAt NullTime
	var updatedAt NullTime

	e := row.Scan(
		&job.ID,
		&job.Priority,
		&job.MaxRetry,
		&retried,
		&queue,
		&uuid,
		&job.Type,
		&job.Payload,
		&timeout,
		&deadline,
		&runAt,
		&lockedAt,
		&lockedBy,
		&failedAt,
		&lastError,
		&createdAt,
		&updatedAt)
	if nil != e {
		return nil, errors.New("scan job failed from the database, " + I18nString(backend.dbDrv, e.Error()))
	}

	if queue.Valid {
		job.Queue = queue.String
	}
	if retried.Valid {
		job.Retried = int(retried.Int64)
	}
	if uuid.Valid {
		job.UUID = uuid.String
	}
	if timeout.Valid {
		job.Timeout = int(timeout.Int64)
	}
	if deadline.Valid {
		job.Deadline = deadline.Time
	}
	if runAt.Valid {
		job.RunAt = runAt.Time
	}
	if lockedAt.Valid {
		job.LockedAt = lockedAt.Time
	}
	if lockedBy.Valid {
		job.LockedBy = lockedBy.String
	}
	if failedAt.Valid {
		job.FailedAt = failedAt.Time
	}
	if lastError.Valid {
		job.LastError = lastError.String
	}
	if createdAt.Valid {
		job.CreatedAt = createdAt.Time
	}
	if updatedAt.Valid {
		job.UpdatedAt = updatedAt.Time
	}
	return job, nil
}

func (backend *dbBackend) Retry(ctx context.Context, id interface{}, retried int, nextTime time.Time, payload interface{}, err string) error {
	if len(err) == 0 {
		_, e := backend.conn.ExecContext(ctx, backend.retryNoErrorSQLString, retried, nextTime, payload, id)
		backend.log(ctx, backend.retryNoErrorSQLString, []interface{}{retried, nextTime, payload, id}, e)
		return e
	}

	if len(err) > 2000 {
		err = err[:1900] + "\r\n===========================\r\n**error message is overflow**"
	}
	_, e := backend.conn.ExecContext(ctx, backend.retryErrorSQLString, retried, nextTime, payload, err, id)
	backend.log(ctx, backend.retryNoErrorSQLString, []interface{}{retried, nextTime, payload, err, id}, e)
	return e
}

func (backend *dbBackend) Fail(ctx context.Context, id interface{}, err string) error {
	if len(err) > 2000 {
		err = err[:1900] + "\r\n===========================\r\n**error message is overflow**"
	}
	_, e := backend.conn.ExecContext(ctx, backend.replyErrorSQLString, err, id)
	backend.log(ctx, backend.replyErrorSQLString, []interface{}{err, id}, e)
	return e
}

func (backend *dbBackend) Destroy(ctx context.Context, id interface{}) error {
	_, e := backend.conn.ExecContext(ctx, backend.deleteSQLString, id)
	backend.log(ctx, backend.deleteSQLString, []interface{}{id}, e)

	if nil != e && sql.ErrNoRows != e {
		return I18nError(backend.dbDrv, e)
	}
	return nil
}

type pgBackend struct {
	dbBackend
}

func (backend *pgBackend) Fetch(ctx context.Context, name string, queues []string) (*Job, error) {
	var sb strings.Builder
	sb.WriteString("UPDATE ")
	sb.WriteString(backend.tablename)
	sb.WriteString(" SET locked_at = $1, locked_by = $2 WHERE id in (SELECT id FROM ")
	sb.WriteString(backend.tablename)
	sb.WriteString(" WHERE ((run_at IS NULL OR run_at < $3) AND (locked_at IS NULL OR locked_at < $4) OR locked_by = $5) AND failed_at IS NULL")

	if backend.minPriority > 0 {
		sb.WriteString(" AND priority >= ")
		sb.WriteString(strconv.FormatInt(int64(backend.minPriority), 10))
	}
	if backend.maxPriority > 0 {
		sb.WriteString(" AND priority <= ")
		sb.WriteString(strconv.FormatInt(int64(backend.maxPriority), 10))
	}
	switch len(queues) {
	case 0:
	case 1:
		sb.WriteString(" AND queue = '")
		sb.WriteString(queues[0])
		sb.WriteString("'")
	default:
		sb.WriteString(" AND queue in (")
		for i, s := range queues {
			if 0 != i {
				sb.WriteString(", '")
			} else {
				sb.WriteString("'")
			}

			sb.WriteString(s)
			sb.WriteString("'")
		}
		sb.WriteString(")")
	}
	sb.WriteString(" ORDER BY priority ASC, run_at ASC  LIMIT 1) RETURNING ")
	sb.WriteString(fieldsSqlString)

	now := time.Now()
	// fmt.Println(sb.String(), now, name, now, now.Truncate(backend.maxRunTime), name)
	rows, e := backend.conn.QueryContext(ctx, sb.String(), now, name, now, now.Truncate(backend.maxRunTime), name)
	backend.log(ctx, sb.String(), []interface{}{now, name, now, now.Truncate(backend.maxRunTime)}, e)
	if nil != e {
		if sql.ErrNoRows == e {
			return nil, nil
		}
		return nil, errors.New("execute query sql failed while fetch job from the database, " + I18nString(backend.dbDrv, e.Error()))
	}
	defer rows.Close()

	for rows.Next() {
		return backend.readJobFromRow(rows)
	}
	return nil, nil
}

func NewBackend(opts *Options) (Backend, error) {
	if opts.Tablename == "" {
		opts.Tablename = "tpt_kl_jobs"
	}
	if opts.DbDrv == "pq" || opts.DbDrv == "postgres" || opts.DbDrv == "postgresql" {
		return newPgBackend(opts)
	}
	return nil, errors.New("db driver name '" + opts.DbDrv + "' is unknown")
}

func newPgBackend(opts *Options) (Backend, error) {
	if opts.MaxRunTime == 0 {
		opts.MaxRunTime = defaultMaxRunTime
	}

	backend := &pgBackend{
		dbBackend: dbBackend{
			dbDrv:       opts.DbDrv,
			dbURL:       opts.DbURL,
			tablename:   opts.Tablename,
			isOwer:      false,
			conn:        opts.Conn,
			minPriority: opts.MinPriority,
			maxPriority: opts.MaxPriority,
			maxRunTime:  opts.MaxRunTime,
			insertSQLString: "INSERT INTO " + opts.Tablename + "(priority, max_retry, retried, queue, uuid, type, payload, timeout, deadline, run_at, locked_at, locked_by, failed_at, last_error, created_at, updated_at)" +
				" VALUES($1, $2, 0, $3, $4, $5, $6, $7, $8, $9, NULL, NULL, NULL, NULL, now(), now())",
			clearLocksSQLString: "UPDATE " + opts.Tablename + " SET locked_by = NULL, locked_at = NULL WHERE locked_by = $1",
			retryNoErrorSQLString: "UPDATE " + opts.Tablename +
				" SET retried = $1, run_at = $2, payload=$3, failed_at=NULL, last_error=NULL, locked_at=NULL, locked_by=NULL, updated_at = now() WHERE id = $4",
			retryErrorSQLString: "UPDATE " + opts.Tablename +
				" SET retried = $1, run_at = $2, payload=$3, failed_at=NULL, last_error=$4, locked_at=NULL, locked_by=NULL, updated_at=now() WHERE id = $5",
			replyErrorSQLString: "UPDATE " + opts.Tablename +
				" SET locked_at = NULL, locked_by = NULL, last_error=$1, failed_at = now(), updated_at = now() WHERE id = $2",
			deleteSQLString: "DELETE FROM " + opts.Tablename + " WHERE id = $1",
		},
	}

	if backend.conn == nil {
		conn, e := sql.Open(opts.DbDrv, opts.DbURL)
		if nil != e {
			return nil, e
		}

		backend.isOwer = true
		backend.conn = conn
	}

	script := `DROP TABLE IF EXISTS ` + backend.tablename + `;
	        CREATE TABLE IF NOT EXISTS ` + backend.tablename + ` (
				  id                SERIAL PRIMARY KEY,
				  priority          int DEFAULT 0,
		      max_retry         int DEFAULT 0,
				  retried           int DEFAULT 0,
				  queue             varchar(200),
				  uuid              varchar(50),
				  type              varchar(50),
				  payload           text  NOT NULL,
				  timeout           int DEFAULT 0,
				  deadline          timestamp with time zone,
				  run_at            timestamp with time zone,
				  locked_at         timestamp with time zone,
				  locked_by         varchar(200),
				  failed_at         timestamp with time zone,
				  last_error        varchar(2000),
				  created_at        timestamp with time zone NOT NULL,
				  updated_at        timestamp with time zone NOT NULL
				);`
	_, e := backend.conn.Exec(script)
	if nil != e {
		return nil, errors.New("init '" + backend.tablename + "' error: " + e.Error())
	}

	return backend, nil
}
