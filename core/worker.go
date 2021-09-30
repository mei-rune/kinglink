package core

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"database/sql"
	"errors"
	"time"

	"github.com/runner-mei/log"
)

// var work_error = expvar.NewString("worker")
var (
	ErrJobsEmpty   = errors.New("jobs is empty")
	ErrJobNotFound = errors.New("job isnot found")
	ErrNoDeleteBecaseRunning = errors.New("job cannot cancel, becase it runnning")
	ErrNoContent = "no content"
)

type WithSQL struct {
	Err    error
	SqlStr string
	Args   []interface{}
}

func (w *WithSQL) Error() string { return w.Err.Error() }

func (w *WithSQL) Unwrap() error { return w.Err }

func WrapSQLError(err error, sqlStr string, args []interface{}) error {
	if sqlStr == "" {
		return err
	}

	return &WithSQL{Err: err, SqlStr: sqlStr, Args: args}
}

func ToSQLError(err error) *WithSQL {
	if err == nil {
		return nil
	}
	e, _ := err.(*WithSQL)
	return e
}

func IsErrNoRows(e error) bool {
	if e == sql.ErrNoRows {
		return true
	}

	sqlErr := ToSQLError(e)
	if sqlErr != nil {
		return sqlErr.Err == sql.ErrNoRows
	}
	return false
}

type ErrAgain struct {
	ts time.Time
}

func (e ErrAgain) Error() string {
	return "except run again at " + e.ts.Format(time.RFC3339)
}

func RunAgain(ts time.Time) error {
	return ErrAgain{ts: ts}
}

func toRunAgain(e error) (time.Time, bool) {
	again, ok := e.(ErrAgain)
	if ok {
		return again.ts, true
	}
	return time.Time{}, false
}

func deserializationError(e error) error {
	return errors.New("[deserialization]" + e.Error())
}

func isDeserializationError(e error) bool {
	return strings.Contains(e.Error(), "[deserialization]")
}

type WorkOptions struct {
	NamePrefix  string
	MinPriority int
	MaxPriority int
	MaxRetry    int
	MaxRunTime  time.Duration
	SleepDelay  time.Duration
	Queues      []string
}

const defaultMaxRunTime = 15 * time.Minute

type Worker struct {
	name      string
	options   WorkOptions
	mux       *ServeMux
	backend   WorkBackend
	lastError atomic.Value
}

func NewWorker(options *WorkOptions, mux *ServeMux, backend WorkBackend) (*Worker, error) {
	if options.MaxRunTime == 0 {
		options.MaxRunTime = 15 * time.Minute
	}
	// Every worker has a unique name which by default is the pid of the process. There are some
	// advantages to overriding this with something which survives worker restarts:  Workers can
	// safely resume working on tasks which are locked by themselves. The worker will assume that
	// it crashed before.
	name := options.NamePrefix + "pid:" + strconv.FormatInt(int64(os.Getpid()), 10)
	w := &Worker{
		name:    name,
		options: *options,
		mux:     mux,
		backend: backend,
	}
	return w, nil
}

func (w *Worker) Run(ctx context.Context, threads int, exitOnComplete bool) {
	logger := log.For(ctx).Named("kinglink").With(log.String("worker", w.name))

	var pool WaitGroupThreads
	var waited bool
	pool.Init(threads, func(err error) {
		w.lastError.Store(err.Error())

		logger.Error("run error", log.Error(err))
	})
	defer func() {
		if !waited {
			pool.Wait()
		}
	}()
	var stats stats

	logger.Info("Starting job worker")

	now := time.Now()
	isRunning := true
	for isRunning {

		n, e := w.workOff(ctx, logger, &pool, &stats, 30)
		if e != nil {
			w.lastError.Store(e.Error())
			logger.Error("run error", log.Error(e))
		} else {
			w.lastError.Store("")
			logger.Info("run ok", log.Int("add", n))

			if n == 0 {
				if exitOnComplete {
					isRunning = false
					break
				}
			}
		}

		select {
		case <-ctx.Done():
			isRunning = false
		case <-time.After(w.options.SleepDelay):
		}
	}
	waited = true
	pool.Wait()

	logger.Info("run ok",
		log.Int("success", stats.Success()),
		log.Int("failure", stats.Failure()),
		log.Duration("elapsed", time.Now().Sub(now)))
	logger.Info("No more jobs available. Exiting")
}

// Do num jobs and return stats on success/failure.
// Exit early if interrupted.
func (w *Worker) WorkOff(ctx context.Context, logger log.Logger, threads, num int) (int, int, error) {
	var pool *ErrGroupThreads
	pool, ctx = NewErrGroupThreads(ctx, threads)
	var stats stats

	_, e := w.workOff(ctx, logger, pool, &stats, num)

	if err := pool.Wait(); err != nil {
		if e == nil {
			e = err
		}
	}

	return stats.Success(), stats.Failure(), e
}

// Do num jobs and return stats on success/failure.
// Exit early if interrupted.
func (w *Worker) workOff(ctx context.Context, logger log.Logger, pool Threads, stats *stats, num int) (int, error) {
	for i := 0; i < num; i++ {
		e := w.reserveAndRunOneJob(ctx, logger, pool, stats)
		if nil != e {
			if e == ErrJobsEmpty {
				return i, nil
			}
			return i, e
		}
	}

	return num, nil
}

// Run the next job we can get an exclusive lock on.
// If no jobs are left we return nil
func (w *Worker) reserveAndRunOneJob(ctx context.Context, logger log.Logger, pool Threads, stats *stats) error {
	if e := pool.Acquire(ctx); e != nil {
		return ErrJobsEmpty
	}
	needRelease := true
	defer func() {
		if needRelease {
			pool.Release()
		}
	}()

	job, e := w.backend.Fetch(ctx, w.name, w.options.Queues)
	if nil != e {
		if e.Error() == ErrNoContent {
			return ErrJobsEmpty
		}
		if IsErrNoRows(e) {
			return ErrJobsEmpty
		}
		return e
	}

	if nil == job {
		return ErrJobsEmpty
	}

	ctx = job.createCtx(ctx)
	logger = log.For(ctx, logger).With(log.Int64("id", job.ID),
		log.String("uuid", job.UUID), log.String("type", job.Type))

	pool.Go(func() error {
		ok, e := w.run(ctx, logger, job)
		if ok {
			stats.addSuccess()
		} else {
			stats.addFailure()
		}
		return e
	})
	needRelease = false
	return nil
}

func (w *Worker) run(ctx context.Context, logger log.Logger, job *Job) (bool, error) {
	logger.Info("RUNNING")
	now := time.Now()
	e := w.invokeJob(&Context{Context: ctx, Logger: logger}, job)
	if nil != e {
		if nextTime, need := toRunAgain(e); need {
			logger.Info("COMPLETED and should again", log.Time("nextTime", nextTime))
			e = w.reschedule(ctx, logger, true, job, nextTime, nil)
			return true, e
		} else if isDeserializationError(e) {
			logger.Info("FAILED (deserialization)", log.Int("retried", job.Retried), log.Int("maxRetry", job.getMaxRetry(w.options.MaxRetry)), log.Error(e))
			e = w.failed(ctx, logger, job, e)
		} else {
			e = w.handleFailedJob(ctx, logger, job, e)
		}
		return false, e // work failed
	}

	logger.Info("COMPLETED", log.Duration("elapsed", time.Now().Sub(now)))
	e = w.completeIt(ctx, logger, job)
	return true, e // did work
}

func (w *Worker) invokeJob(ctx *Context, job *Job) (err error) {
	defer func() {
		if e := recover(); nil != e {
			msg := fmt.Sprintf("[panic]%v \r\n%s", e, debug.Stack())
			err = errors.New(msg)
		}
	}()

	_, e := job.Payload.Fields()
	if e != nil {
		return deserializationError(errors.New("payload invalid: " + e.Error()))
	}

	var cancel context.CancelFunc
	ctx.Context, cancel = context.WithTimeout(ctx.Context, job.execTimeout(w.options.MaxRunTime))

	e = w.mux.RunJob(ctx, job)
	if e != context.DeadlineExceeded {
		cancel()
	}
	return e

	// ch := make(chan error, 1)
	// go func() {
	// 	defer func() {
	// 		if e := recover(); nil != e {
	// 			msg := fmt.Sprintf("[panic]%v \r\n%s", e, debug.Stack())
	// 			ch <- errors.New(msg)
	// 		}
	// 	}()

	// 	ch <- w.mux.RunJob(ctx, job)
	// }()

	// var cancel func()
	// ctx, cancel = context.WithCancel(ctx)

	// timer := time.NewTimer(job.execTimeout(w.options.MaxRunTime))
	// select {
	// case err := <-ch:
	// 	timer.Stop()
	// 	return err
	// case <-timer.C:
	// 	cancel()
	// 	return ErrTimeout
	// }
}

func (w *Worker) completeIt(ctx context.Context, logger log.Logger, job *Job) error {
	return w.backend.Success(ctx, job.ID)
}

func (w *Worker) failed(ctx context.Context, logger log.Logger, job *Job, e error) error {
	logger.Info("STOPPED permanently", log.Int("retried", job.Retried), log.Int("maxRetry", job.getMaxRetry(w.options.MaxRetry)), log.Error(e))
	return w.backend.Fail(ctx, job.ID, e.Error())
}

func (w *Worker) handleFailedJob(ctx context.Context, logger log.Logger, job *Job, e error) error {
	logger.Info("FAILED", log.Int("retried", job.Retried), log.Int("maxRetry", job.getMaxRetry(w.options.MaxRetry)), log.Error(e))
	return w.reschedule(ctx, logger, false, job, time.Time{}, e)
}

// Reschedule the job in the future (when a job fails).
// Uses an exponential scale depending on the number of failed attempts.
func (w *Worker) reschedule(ctx context.Context, logger log.Logger, runAgain bool, job *Job, nextTime time.Time, e error) error {
	var err string
	if e != nil {
		err = e.Error()
	}
	if runAgain {
		if nextTime.IsZero() {
			nextTime = job.rescheduleAt()
		}
		return w.backend.Retry(ctx, job.ID, job.Retried, nextTime, &job.Payload, err)
	}

	if attempts := job.Retried + 1; attempts <= job.getMaxRetry(w.options.MaxRetry) {
		if nextTime.IsZero() {
			nextTime = job.rescheduleAt()
		}
		return w.backend.Retry(ctx, job.ID, attempts, nextTime, &job.Payload, err)
	} else {
		return w.failed(ctx, logger, job, e)
	}
}
