package core

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

type DbOptions struct {
	DbDrv            string
	DbURL            string
	RunningTablename string
	ResultTablename  string
	ViewTablename    string

	Conn *sql.DB
}

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

	minPriority   int
	maxPriority   int
	maxRunTime    time.Duration
	maxRunTimeSQL string

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
	cancelQueuingSQLString  string
	cancelResultSQLString   string
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

	logger := log.For(ctx)

	_, err := conn.ExecContext(ctx, backend.clearAllQueuing)
	if err != nil {
		logger.Info(backend.clearAllQueuing, log.Error(err))
		return err
	}
	logger.Info(backend.clearAllQueuing)

	_, err = conn.ExecContext(ctx, backend.clearAllResult)
	if err != nil {
		logger.Info(backend.clearAllResult, log.Error(err))
		return err
	}
	logger.Info(backend.clearAllResult)
	return nil
}

const stateFields = `id, priority, max_retry, retried, queue, uuid, type, payload,
				  timeout, deadline, run_at, locked_at, run_by, last_error, completed_at, created_at, updated_at`

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
	var completedAt NullTime
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
		&runBy,
		&lastError,
		&completedAt,
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
		job.UniqueKey = uuid.String
	}
	if timeout.Valid {
		job.Timeout = strconv.FormatInt(timeout.Int64, 10) + "s"
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
		job.LastAt = updatedAt.Time
	}
	if completedAt.Valid {
		job.CompletedAt = completedAt.Time
		if job.LastError != "" {
			job.Status = StatusFail
		} else {
			job.Status = StatusOK
		}
	} else {
		if job.LastError != "" {
			job.Status = StatusFailAndRequeueing
		} else if lockedAt.Valid {
			job.Status = StatusRunning
		} else {
			job.Status = StatusQueueing
		}
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

	row := backend.conn.QueryRowContext(ctx, backend.readStateSQLString, id)
	jobResult, err := backend.readStateFromRow(row, nil)
	if err != nil {
		log.For(ctx).Info(backend.readStateSQLString, log.Stringer("args", log.SQLArgs{id}), log.Error(err))
		return nil, err
	}

	log.For(ctx).Info(backend.readStateSQLString, log.Stringer("args", log.SQLArgs{id}))
	return jobResult, nil
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

	var uuid sql.NullString
	if s := strings.TrimSpace(job.UUID); s != "" {
		uuid.String = s
		uuid.Valid = true
	}

	var id int64
	err := conn.QueryRowContext(ctx, backend.insertSQLString, job.Priority, job.MaxRetry, queue, uuid, job.Type, &job.Payload, job.Timeout, deadline, runAt).Scan(&id)
	if err != nil {
		args := []interface{}{job.Priority, job.MaxRetry, queue, job.UUID, job.Type, &job.Payload, job.Timeout, deadline, runAt}
		log.For(ctx).Info(backend.insertSQLString, log.Stringer("args", log.SQLArgs(args)), log.Error(err))
		return nil, err
	}

	args := []interface{}{job.Priority, job.MaxRetry, queue, uuid, job.Type, &job.Payload, job.Timeout, deadline, runAt}
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

func (backend *dbBackend) Retry(ctx context.Context, id interface{}, retried int, nextTime time.Time, payload *Payload, err string) error {
	if len(err) == 0 {
		_, e := backend.conn.ExecContext(ctx, backend.retryNoErrorSQLString, retried, nextTime, payload, id)
		if e != nil {
			log.For(ctx).Info(backend.retryNoErrorSQLString, log.Stringer("args", log.SQLArgs{retried, nextTime, payload, id}), log.Error(e))
			return e
		}

		log.For(ctx).Info(backend.retryNoErrorSQLString, log.Stringer("args", log.SQLArgs{retried, nextTime, payload, id}))
		return nil
	}

	if len(err) > 2000 {
		err = err[:1900] + "\r\n===========================\r\n**error message is overflow**"
	}
	_, e := backend.conn.ExecContext(ctx, backend.retryErrorSQLString, retried, nextTime, payload, err, id)
	if e != nil {
		log.For(ctx).Info(backend.retryErrorSQLString, log.Stringer("args", log.SQLArgs{retried, nextTime, payload, err, id}), log.Error(e))
		return e
	}
	log.For(ctx).Info(backend.retryErrorSQLString, log.Stringer("args", log.SQLArgs{retried, nextTime, payload, err, id}))
	return nil
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
		return backend.copyResult(ctx, id, tx)
	})
}

func (backend *dbBackend) Success(ctx context.Context, id interface{}) error {
	return backend.withTx(ctx, func(ctx context.Context, tx DBRunner) error {
		return backend.copyResult(ctx, id, tx)
	})
}

func (backend *dbBackend) copyResult(ctx context.Context, id interface{}, tx DBRunner) error {
	_, e := tx.ExecContext(ctx, backend.copySQLString, id)
	if nil != e {
		log.For(ctx).Info(backend.copySQLString, log.Stringer("args", log.SQLArgs{id}), log.Error(e))
		if e == sql.ErrNoRows {
			return nil
		}
		return I18nError(backend.dbDrv, e)
	}
	log.For(ctx).Info(backend.copySQLString, log.Stringer("args", log.SQLArgs{id}))

	_, e = tx.ExecContext(ctx, backend.deleteSQLString, id)
	if nil != e && sql.ErrNoRows != e {
		log.For(ctx).Info(backend.deleteSQLString, log.Stringer("args", log.SQLArgs{id}), log.Error(e))
		return I18nError(backend.dbDrv, e)
	}
	log.For(ctx).Info(backend.deleteSQLString, log.Stringer("args", log.SQLArgs{id}))
	return nil
}

func (backend *dbBackend) CancelList(ctx context.Context, idList []interface{}) error {
	return backend.withTx(ctx, func(ctx context.Context, tx DBRunner) error {
		for _, id := range idList {
			err := backend.cancel(ctx, tx, id)
			if err != nil {
				if err != ErrJobNotFound {
					return err
				}
			}
		}
		return nil
	})
}

func (backend *dbBackend) Cancel(ctx context.Context, id interface{}) error {
	conn := DbConnectionFromContext(ctx)
	if conn == nil {
		conn = backend.conn
	}

	return backend.cancel(ctx, conn, id)
}

func (backend *dbBackend) cancel(ctx context.Context, conn DBRunner, id interface{}) error {
	if s, ok := id.(string); ok {
		i64, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return errors.New("argument type error: id must is int")
		}
		id = i64
	}

	result, err := conn.ExecContext(ctx, backend.cancelQueuingSQLString, id)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected > 0 {
		return nil
	}

	result, err = conn.ExecContext(ctx, backend.cancelResultSQLString, id)
	if err != nil {
		return err
	}

	rowsAffected, err = result.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected > 0 {
		return nil
	}
	return ErrJobNotFound
}

func (backend *dbBackend) withTx(ctx context.Context, cb func(ctx context.Context, tx DBRunner) error) error {
	var isOwner = false
	tx := TxFromContext(ctx)
	if tx == nil {
		var conn *sql.DB
		if db := DbConnectionFromContext(ctx); db == nil {
			if tmp, ok := db.(*sql.DB); ok {
				conn = tmp
			} else {
				conn = backend.conn
			}
		} else {
			conn = backend.conn
		}

		innertx, e := conn.Begin()
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

func (backend *pgBackend) GetStates(ctx context.Context, queues []string, offset, limit int) ([]JobState, error) {
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

	sqlStr := sb.String()
	rows, err := backend.conn.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		log.For(ctx).Error(sqlStr, log.Stringer("args", log.SQLArgs(args)), log.Error(err))
		return nil, err
	}

	defer rows.Close()

	var jobResults = make([]JobState, 16)
	count := 0
	for rows.Next() {
		if len(jobResults) <= count {
			copyed := make([]JobState, 2*len(jobResults))
			copy(copyed, jobResults)
			jobResults = copyed
		}

		_, err := backend.readStateFromRow(rows, &jobResults[count])
		if err != nil {
			log.For(ctx).Error(sqlStr, log.Stringer("args", log.SQLArgs(args)), log.Error(err))
			return nil, err
		}
		count++
	}
	jobResults = jobResults[:count]

	if rows.Err() != nil {
		log.For(ctx).Error(sqlStr, log.Stringer("args", log.SQLArgs(args)), log.Error(rows.Err()))
		return nil, rows.Err()
	}

	log.For(ctx).Info(sqlStr, log.Stringer("args", log.SQLArgs(args)))
	return jobResults, nil
}

func (backend *pgBackend) Fetch(ctx context.Context, name string, queues []string) (*Job, error) {
	var sb strings.Builder
	sb.WriteString("UPDATE ")
	sb.WriteString(backend.runningTablename)
	sb.WriteString(" SET locked_at = now(), locked_by = $1 WHERE id in (SELECT id FROM ")
	sb.WriteString(backend.runningTablename)
	sb.WriteString(" WHERE ((run_at IS NULL OR run_at < now()) AND (locked_at IS NULL OR (locked_at < (now() - interval '")
	sb.WriteString(backend.maxRunTimeSQL)
	sb.WriteString("') AND locked_by = $2))) AND failed_at IS NULL")

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

	queryStr := sb.String()
	// fmt.Println(sb.String(), now, name, now, now.Truncate(backend.maxRunTime), name)
	rows, e := backend.conn.QueryContext(ctx, queryStr, name, name)
	if nil != e {
		log.For(ctx).Info(queryStr, log.Stringer("args", log.SQLArgs{name, name}), log.Error(e))

		if sql.ErrNoRows == e {
			return nil, nil
		}
		return nil, errors.New("execute query sql failed while fetch job from the database, " + I18nString(backend.dbDrv, e.Error()))
	}
	defer rows.Close()

	log.For(ctx).Info(queryStr, log.Stringer("args", log.SQLArgs{name, name}), log.Error(e))

	for rows.Next() {
		return backend.readJobFromRow(rows)
	}
	return nil, nil
}

func NewBackend(dbopts *DbOptions, opts *WorkOptions) (Backend, error) {
	if dbopts.RunningTablename == "" {
		dbopts.RunningTablename = "tpt_kl_jobs"
	}
	if dbopts.ResultTablename == "" {
		dbopts.ResultTablename = "tpt_kl_results"
	}
	if dbopts.ViewTablename == "" {
		dbopts.ViewTablename = "tpt_kl_views"
	}
	if dbopts.DbDrv == "pq" || dbopts.DbDrv == "postgres" || dbopts.DbDrv == "postgresql" {
		return newPgBackend(dbopts, opts)
	}
	return nil, errors.New("db driver name '" + dbopts.DbDrv + "' is unknown")
}

func newPgBackend(dbopts *DbOptions, opts *WorkOptions) (Backend, error) {
	if opts.MaxRunTime == 0 {
		opts.MaxRunTime = defaultMaxRunTime
	}
	maxRunTimeSQL := strconv.FormatInt(int64(opts.MaxRunTime.Seconds()), 10) + " Seconds"

	backend := &pgBackend{
		dbBackend: dbBackend{
			dbDrv:                dbopts.DbDrv,
			dbURL:                dbopts.DbURL,
			runningTablename:     dbopts.RunningTablename,
			resultTablename:      dbopts.ResultTablename,
			viewTablename:        dbopts.ViewTablename,
			isOwer:               false,
			conn:                 dbopts.Conn,
			minPriority:          opts.MinPriority,
			maxPriority:          opts.MaxPriority,
			maxRunTime:           opts.MaxRunTime,
			maxRunTimeSQL:        maxRunTimeSQL,
			readQueuingSQLString: "SELECT " + fieldsSqlString + " FROM " + dbopts.RunningTablename + " WHERE id = #{id}",
			insertSQLString: "INSERT INTO " + dbopts.RunningTablename + "(priority, max_retry, retried, queue, uuid, type, payload, timeout, deadline, run_at, locked_at, locked_by, failed_at, last_error, created_at, updated_at)" +
				" VALUES($1, $2, 0, $3, $4, $5, $6, $7, $8, $9, NULL, NULL, NULL, NULL, now(), now()) RETURNING id",
			clearLocksSQLString: "UPDATE " + dbopts.RunningTablename + " SET locked_by = NULL, locked_at = NULL WHERE locked_by = $1",
			retryNoErrorSQLString: "UPDATE " + dbopts.RunningTablename +
				" SET retried = $1, run_at = $2, payload=$3, failed_at=NULL, last_error=NULL, locked_at=NULL, locked_by=NULL, updated_at = now() WHERE id = $4",
			retryErrorSQLString: "UPDATE " + dbopts.RunningTablename +
				" SET retried = $1, run_at = $2, payload=$3, failed_at=NULL, last_error=$4, locked_at=NULL, locked_by=NULL, updated_at=now() WHERE id = $5",
			replyErrorSQLString: "UPDATE " + dbopts.RunningTablename +
				" SET last_error=$1, failed_at = now(), updated_at = now() WHERE id = $2",
			deleteSQLString: "DELETE FROM " + dbopts.RunningTablename + " WHERE id = $1",
			copySQLString: `INSERT INTO ` + dbopts.ResultTablename + ` SELECT id, priority, retried, queue, uuid, type, payload, locked_by as run_by, last_error, created_at, updated_at FROM ` +
				dbopts.RunningTablename + ` WHERE id = $1`,
			readResultSQLString:     "SELECT " + resultFields + " FROM " + dbopts.ResultTablename + " WHERE id = $1",
			deleteResultSQLString:   "DELETE FROM " + dbopts.ResultTablename + " WHERE id = $1",
			deleteResultWithTimeout: "DELETE FROM " + dbopts.ResultTablename + " WHERE (updated_at + - interval '%d Minutes') <  now()",
			readStateSQLString:      "SELECT " + stateFields + " FROM " + dbopts.ViewTablename + " WHERE id = $1",
			queryStateSQLString:     "SELECT " + stateFields + " FROM " + dbopts.ViewTablename,
			clearAllQueuing:         "DELETE FROM " + dbopts.RunningTablename,
			clearAllResult:          "DELETE FROM " + dbopts.ResultTablename,
			cancelQueuingSQLString:  "DELETE FROM " + dbopts.RunningTablename + " WHERE id = $1 AND (locked_at IS NULL OR locked_at < (now() - interval '" + maxRunTimeSQL + "')) ",
			cancelResultSQLString:   "DELETE FROM " + dbopts.ResultTablename + " WHERE id = $1",
		},
	}

	if backend.conn == nil {
		conn, e := sql.Open(dbopts.DbDrv, dbopts.DbURL)
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
				  updated_at        timestamp with time zone NOT NULL,

				  UNIQUE(uuid)
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
				  locked_by AS run_by,
				  last_error,
				  NULL AS completed_at,
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
				  updated_at AS completed_at,
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
