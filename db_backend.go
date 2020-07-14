package kinglink

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
	dbDrv            string
	dbURL            string
	runningTablename string
	resultTablename  string
	viewTablename    string
	isOwer           bool
	conn             *sql.DB

	minPriority int
	maxPriority int
	maxRunTime  time.Duration

	readQueuingSQLString    string
	insertSQLString         string
	clearLocksSQLString     string
	retryNoErrorSQLString   string
	retryErrorSQLString     string
	replyErrorSQLString     string
	copySQLString           string
	deleteSQLString         string
	readResultSQLString     string
	deleteResultSQLString   string
	deleteResultWithTimeout string
	readStateSQLString      string
	queryStateSQLString     string
	clearAllQueuing         string
	clearAllResult          string
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
	// "UPDATE "+backend.RunningTablename+" SET locked_by = NULL, locked_at = NULL WHERE locked_by = $1"
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

func (backend *dbBackend) ClearAll(ctx context.Context) error {
	conn := DbConnectionFromContext(ctx)
	if conn == nil {
		conn = backend.conn
	}

	_, err := conn.ExecContext(ctx, backend.clearAllQueuing)
	if err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, backend.clearAllResult)
	if err != nil {
		return err
	}
	return nil
}

const stateFields = `id, priority, max_retry, retried, queue, uuid, type, payload,
				  timeout, deadline, run_at, locked_at, run_by, last_error, created_at, updated_at`

func (backend *dbBackend) readStateFromRow(row interface {
	Scan(dest ...interface{}) error
}, job *JobState) (*JobState, error) {
	if job == nil {
		job = &JobState{}
	}
	var queue sql.NullString
	var uuid sql.NullString
	var retried sql.NullInt64
	var timeout sql.NullInt64
	var deadline NullTime
	var runAt NullTime
	var lockedAt NullTime
	// var lockedBy sql.NullString
	var runBy sql.NullString
	var lastError sql.NullString
	var createdAt NullTime
	var updatedAt NullTime

	e := row.Scan(
		&job.UUID,
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
		&runBy,
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
		job.Timeout = time.Duration(timeout.Int64).String()
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
	if runBy.Valid {
		job.RunBy = runBy.String
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

func (backend *dbBackend) GetState(ctx context.Context, id interface{}) (*JobState, error) {
	if s, ok := id.(string); ok {
		i64, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, errors.New("argument type error: id must is int")
		}
		id = i64
	}

	row := backend.conn.QueryRowContext(ctx, backend.readResultSQLString, id)
	jobResult, err := backend.readStateFromRow(row, nil)
	if err != nil {
		log.For(ctx).Info(backend.readStateSQLString, log.Stringer("args", log.SQLArgs{id}), log.Error(err))
		return nil, err
	}

	log.For(ctx).Info(backend.readStateSQLString, log.Stringer("args", log.SQLArgs{id}))
	return jobResult, nil
}

func (backend *dbBackend) GetStates(ctx context.Context, queues []string, offset, limit int) ([]JobState, error) {
	var sb strings.Builder
	sb.WriteString(backend.queryStateSQLString)
	var args = make([]interface{}, 0, len(queues))
	if len(queues) > 0 {
		if len(queues) == 1 {
			sb.WriteString(" WHERE queue = $1")
			args = append(args, queues[0])
		} else {
			sb.WriteString(" WHERE queue in (")
			for idx := range queues {
				if idx != 0 {
					sb.WriteString(",")
				}
				sb.WriteString("$")
				args = append(args, queues[idx])
				sb.WriteString(strconv.Itoa(len(args)))
			}
			sb.WriteString(")")
		}
	}

	if offset > 0 {
		if limit < 0 {
			limit = 0
		}
	}

	if limit > 0 || offset > 0 {
		sb.WriteString(" LIMIT ")
		sb.WriteString(strconv.Itoa(limit))

		sb.WriteString(" OFFSET ")
		sb.WriteString(strconv.Itoa(offset))
	}

	rows, err := backend.conn.QueryContext(ctx, sb.String(), args...)
	if err != nil {
		log.For(ctx).Error(backend.readStateSQLString, log.Stringer("args", log.SQLArgs(args)), log.Error(err))
		return nil, err
	}

	defer rows.Close()

	var jobResults = make([]JobState, 16)
	count := 0
	for rows.Next() {
		if len(jobResults) >= count {
			copyed := make([]JobState, 2*len(jobResults))
			copy(copyed, jobResults)
			jobResults = copyed
		}

		_, err := backend.readStateFromRow(rows, &jobResults[count])
		if err != nil {
			log.For(ctx).Error(backend.readStateSQLString, log.Stringer("args", log.SQLArgs(args)), log.Error(err))
			return nil, err
		}
		count++
	}
	jobResults = jobResults[:count]

	if rows.Err() != nil {
		log.For(ctx).Error(backend.readStateSQLString, log.Stringer("args", log.SQLArgs(args)), log.Error(rows.Err()))
		return nil, rows.Err()
	}

	log.For(ctx).Info(backend.readStateSQLString, log.Stringer("args", log.SQLArgs(args)))
	return jobResults, nil
}

func (backend *dbBackend) DeleteResult(ctx context.Context, id interface{}) error {
	conn := DbConnectionFromContext(ctx)
	if conn == nil {
		conn = backend.conn
	}

	if s, ok := id.(string); ok {
		i64, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return errors.New("argument type error: id must is int")
		}
		id = i64
	}

	_, err := conn.ExecContext(ctx, backend.deleteResultSQLString, id)
	if err != nil {
		log.For(ctx).Info(backend.deleteResultSQLString, log.Stringer("args", log.SQLArgs{id}), log.Error(err))
		return err
	}

	log.For(ctx).Info(backend.deleteResultSQLString, log.Stringer("args", log.SQLArgs{id}))
	return nil
}

func (backend *dbBackend) ClearWithTimeout(ctx context.Context, minutes int) error {
	conn := DbConnectionFromContext(ctx)
	if conn == nil {
		conn = backend.conn
	}
	sqlstr := fmt.Sprintf(backend.deleteResultWithTimeout, minutes)
	_, err := conn.ExecContext(ctx, sqlstr)
	if err != nil {
		log.For(ctx).Info(sqlstr, log.Stringer("args", log.SQLArgs{}), log.Error(err))
		return err
	}

	log.For(ctx).Info(sqlstr, log.Stringer("args", log.SQLArgs{}))
	return nil
}

func (backend *dbBackend) GetResult(ctx context.Context, id interface{}) (*JobResult, error) {
	conn := DbConnectionFromContext(ctx)
	if conn == nil {
		conn = backend.conn
	}

	if s, ok := id.(string); ok {
		i64, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, errors.New("argument type error: id must is int")
		}
		id = i64
	}

	row := conn.QueryRowContext(ctx, backend.readResultSQLString, id)
	jobResult, err := backend.readResultFromRow(row)
	if err != nil {
		log.For(ctx).Info(backend.readResultSQLString, log.Stringer("args", log.SQLArgs{id}), log.Error(err))
		return nil, err
	}

	log.For(ctx).Info(backend.readResultSQLString, log.Stringer("args", log.SQLArgs{id}))
	return jobResult, nil
}

const resultFields = "id, priority, queue, uuid, type, payload, retried, run_by, last_error, created_at, updated_at"

func (backend *dbBackend) readResultFromRow(row interface {
	Scan(dest ...interface{}) error
}) (*JobResult, error) {
	jobResult := &JobResult{}
	var queue sql.NullString
	var uuid sql.NullString
	var retried sql.NullInt64
	var runBy sql.NullString
	var completedAt NullTime
	var lastError sql.NullString
	var createdAt NullTime
	var updatedAt NullTime

	e := row.Scan(
		&jobResult.ID,
		&jobResult.Priority,
		&queue,
		&uuid,
		&jobResult.Type,
		&jobResult.Payload,
		&retried,
		&runBy,
		&completedAt,
		&lastError,
		&createdAt,
		&updatedAt)
	if nil != e {
		return nil, errors.New("scan result failed from the database, " + I18nString(backend.dbDrv, e.Error()))
	}
	if queue.Valid {
		jobResult.Queue = queue.String
	}
	if uuid.Valid {
		jobResult.UUID = uuid.String
	}
	if retried.Valid {
		jobResult.Retried = int(retried.Int64)
	}
	if runBy.Valid {
		jobResult.RunBy = runBy.String
	}
	if completedAt.Valid {
		jobResult.CompletedAt = completedAt.Time
	}
	if lastError.Valid {
		jobResult.LastError = lastError.String
	}
	if createdAt.Valid {
		jobResult.CreatedAt = createdAt.Time
	}
	if updatedAt.Valid {
		jobResult.UpdatedAt = updatedAt.Time
	}
	return jobResult, nil
}

func (backend *dbBackend) Enqueue(ctx context.Context, job *Job) (interface{}, error) {
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

	conn := DbConnectionFromContext(ctx)
	if conn == nil {
		conn = backend.conn
	}

	var id int64
	err := conn.QueryRowContext(ctx, backend.insertSQLString, job.Priority, job.MaxRetry, queue, job.UUID, job.Type, &job.Payload, job.Timeout, deadline, runAt).Scan(&id)
	if err != nil {
		args := []interface{}{job.Priority, job.MaxRetry, queue, job.UUID, job.Type, &job.Payload, job.Timeout, deadline, runAt}
		log.For(ctx).Info(backend.insertSQLString, log.Stringer("args", log.SQLArgs(args)), log.Error(err))
		return nil, err
	}

	args := []interface{}{job.Priority, job.MaxRetry, queue, job.UUID, job.Type, &job.Payload, job.Timeout, deadline, runAt}
	log.For(ctx).Info(backend.insertSQLString, log.Stringer("args", log.SQLArgs(args)))
	return id, nil
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

func (backend *dbBackend) Get(ctx context.Context, id interface{}) (*Job, error) {
	conn := DbConnectionFromContext(ctx)
	if conn == nil {
		conn = backend.conn
	}

	if s, ok := id.(string); ok {
		i64, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, errors.New("argument type error: id must is int")
		}
		id = i64
	}

	row := conn.QueryRowContext(ctx, backend.readQueuingSQLString, id)
	return backend.readJobFromRow(row)
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

	return backend.withTx(ctx, func(ctx context.Context, tx DBRunner) error {
		_, e := tx.ExecContext(ctx, backend.replyErrorSQLString, err, id)
		backend.log(ctx, backend.replyErrorSQLString, []interface{}{err, id}, e)
		if e != nil {
			return I18nError(backend.dbDrv, e)
		}
		return backend.Destroy(ctx, id)
	})
}

func (backend *dbBackend) Destroy(ctx context.Context, id interface{}) error {
	return backend.withTx(ctx, func(ctx context.Context, tx DBRunner) error {
		_, e := tx.ExecContext(ctx, backend.copySQLString, id)
		backend.log(ctx, backend.deleteSQLString, []interface{}{id}, e)
		if nil != e {
			if e == sql.ErrNoRows {
				return nil
			}
			return I18nError(backend.dbDrv, e)
		}
		_, e = tx.ExecContext(ctx, backend.deleteSQLString, id)
		backend.log(ctx, backend.deleteSQLString, []interface{}{id}, e)
		if nil != e && sql.ErrNoRows != e {
			return I18nError(backend.dbDrv, e)
		}
		return nil
	})
}

func (backend *dbBackend) withTx(ctx context.Context, cb func(ctx context.Context, tx DBRunner) error) error {
	var isOwner = false
	tx := TxFromContext(ctx)
	if tx == nil {
		innertx, e := backend.conn.Begin()
		if e != nil {
			return I18nError(backend.dbDrv, e)
		}
		isOwner = true
		tx = innertx
		ctx = WithTx(ctx, tx)
	}

	err := cb(ctx, tx)
	if err != nil {
		if isOwner {
			return tx.Rollback()
		}
	} else {
		if isOwner {
			return tx.Commit()
		}
	}
	return nil
}

type pgBackend struct {
	dbBackend
}

func (backend *pgBackend) Fetch(ctx context.Context, name string, queues []string) (*Job, error) {
	var sb strings.Builder
	sb.WriteString("UPDATE ")
	sb.WriteString(backend.runningTablename)
	sb.WriteString(" SET locked_at = $1, locked_by = $2 WHERE id in (SELECT id FROM ")
	sb.WriteString(backend.runningTablename)
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
	if opts.RunningTablename == "" {
		opts.RunningTablename = "tpt_kl_jobs"
	}
	if opts.ResultTablename == "" {
		opts.ResultTablename = "tpt_kl_results"
	}
	if opts.ViewTablename == "" {
		opts.ViewTablename = "tpt_kl_views"
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
			dbDrv:                opts.DbDrv,
			dbURL:                opts.DbURL,
			runningTablename:     opts.RunningTablename,
			resultTablename:      opts.ResultTablename,
			viewTablename:        opts.ViewTablename,
			isOwer:               false,
			conn:                 opts.Conn,
			minPriority:          opts.MinPriority,
			maxPriority:          opts.MaxPriority,
			maxRunTime:           opts.MaxRunTime,
			readQueuingSQLString: "SELECT " + fieldsSqlString + " FROM " + opts.RunningTablename + " WHERE id = #{id}",
			insertSQLString: "INSERT INTO " + opts.RunningTablename + "(priority, max_retry, retried, queue, uuid, type, payload, timeout, deadline, run_at, locked_at, locked_by, failed_at, last_error, created_at, updated_at)" +
				" VALUES($1, $2, 0, $3, $4, $5, $6, $7, $8, $9, NULL, NULL, NULL, NULL, now(), now()) RETURNING id",
			clearLocksSQLString: "UPDATE " + opts.RunningTablename + " SET locked_by = NULL, locked_at = NULL WHERE locked_by = $1",
			retryNoErrorSQLString: "UPDATE " + opts.RunningTablename +
				" SET retried = $1, run_at = $2, payload=$3, failed_at=NULL, last_error=NULL, locked_at=NULL, locked_by=NULL, updated_at = now() WHERE id = $4",
			retryErrorSQLString: "UPDATE " + opts.RunningTablename +
				" SET retried = $1, run_at = $2, payload=$3, failed_at=NULL, last_error=$4, locked_at=NULL, locked_by=NULL, updated_at=now() WHERE id = $5",
			replyErrorSQLString: "UPDATE " + opts.RunningTablename +
				" SET last_error=$1, failed_at = now(), updated_at = now() WHERE id = $2",
			deleteSQLString: "DELETE FROM " + opts.RunningTablename + " WHERE id = $1",
			copySQLString: `INSERT INTO ` + opts.ResultTablename + ` SELECT id, priority, retried, queue, uuid, type, payload, locked_by as run_by, last_error, created_at, updated_at FROM ` +
				opts.RunningTablename + ` WHERE id = $1`,
			readResultSQLString:     "SELECT " + resultFields + " FROM " + opts.ResultTablename + " WHERE id = $1",
			deleteResultSQLString:   "DELETE FROM " + opts.ResultTablename + " WHERE id = $1",
			deleteResultWithTimeout: "DELETE FROM " + opts.ResultTablename + " WHERE (updated_at + - interval '%d Minutes') <  now()",
			readStateSQLString:      "SELECT " + stateFields + " FROM " + opts.ViewTablename + " WHERE id = $1",
			queryStateSQLString:     "SELECT " + stateFields + " FROM " + opts.ViewTablename,
			clearAllQueuing:         "DELETE FROM " + opts.RunningTablename,
			clearAllResult:          "DELETE FROM " + opts.ResultTablename,
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

	script := `DROP VIEW IF EXISTS ` + backend.viewTablename + `;` +
		`DROP TABLE IF EXISTS ` + backend.runningTablename + `;` +
		`DROP TABLE IF EXISTS ` + backend.resultTablename + `;` +
		`CREATE UNLOGGED TABLE IF NOT EXISTS ` + backend.runningTablename + ` (
				  id                BIGSERIAL PRIMARY KEY,
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
				);` +
		`CREATE UNLOGGED TABLE IF NOT EXISTS ` + backend.resultTablename + ` (
				  id                bigint PRIMARY KEY,
				  priority          int DEFAULT 0,
				  retried           int DEFAULT 0,
				  queue             varchar(200),
				  uuid              varchar(50),
				  type              varchar(50),
				  payload           text  NOT NULL,
				  run_by            varchar(200),
				  last_error        varchar(2000),
				  created_at        timestamp with time zone NOT NULL,
				  updated_at        timestamp with time zone NOT NULL
				);` +
		`CREATE OR REPLACE VIEW ` + backend.viewTablename + ` AS
		  SELECT id,
				  priority,
		      max_retry,
				  retried,
				  queue,
				  uuid,
				  type,
				  payload,
				  timeout,
				  deadline,
				  run_at,
				  locked_at,
				  NULL AS run_by,
				  last_error,
				  created_at,
				  updated_at
      FROM ` + backend.runningTablename + `
   UNION
		 SELECT id,
				  priority,
		      0 as max_retry,
				  retried,
				  queue,
				  uuid,
				  type,
				  payload,
				  0 as timeout,
				  NULL AS deadline,
				  NULL AS run_at,
				  NULL AS locked_at,
				  run_by,
				  last_error,
				  created_at,
				  updated_at
   FROM ` + backend.resultTablename + `;`

	_, e := backend.conn.Exec(script)
	if nil != e {
		fmt.Println(script)
		return nil, errors.New("init tables error: " + e.Error())
	}

	return backend, nil
}
