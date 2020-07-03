package kinglink

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/runner-mei/log"
)

// var work_error = expvar.NewString("worker")
var ErrJobsEmpty = errors.New("jobs is empty")

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

type Options struct {
	DbDrv     string
	DbURL     string
	Tablename string

	NamePrefix  string
	MinPriority int
	MaxPriority int
	MaxRetry    int
	MaxRunTime  time.Duration
	SleepDelay  time.Duration
	Queues      []string
	ReadAhead   int

	// By default failed jobs are destroyed after too many attempts. If you want to keep them around
	// (perhaps to inspect the reason for the failure), set this to false.
	DestroyFailedJobs bool
	ExitOnComplete    bool

	Conn *sql.DB `json:"-"`
}

const defaultMaxRunTime = 15 * time.Minute

type worker struct {
	name      string
	options   Options
	mux       *ServeMux
	backend   Backend
	lastError atomic.Value
}

func newWorker(options *Options, mux *ServeMux, backend Backend) (*worker, error) {
	if options.MaxRunTime == 0 {
		options.MaxRunTime = 15 * time.Minute
	}
	// Every worker has a unique name which by default is the pid of the process. There are some
	// advantages to overriding this with something which survives worker restarts:  Workers can
	// safely resume working on tasks which are locked by themselves. The worker will assume that
	// it crashed before.
	name := options.NamePrefix + "pid:" + strconv.FormatInt(int64(os.Getpid()), 10)
	w := &worker{
		name:    name,
		options: *options,
		mux:     mux,
		backend: backend,
	}
	return w, nil
}

func (w *worker) Run(ctx context.Context, exitOnComplete bool, shutdown chan struct{}) {
	logger := log.For(ctx).Named("kinglink").With(log.String("worker", w.name))

	logger.Info("Starting job worker")

	isRunning := true
	for isRunning {
		for isRunning {
			now := time.Now()

			success, failure, e := w.workOff(ctx, logger, 10)
			if e != nil {
				w.lastError.Store(e.Error())

				logger.Error("run error", log.Error(e))
				break
			}

			if success == 0 && exitOnComplete {
				isRunning = false
			}

			w.lastError.Store("")

			logger.Info("run ok",
				log.Int("success", success),
				log.Int("failure", failure),
				log.Duration("elapsed", time.Now().Sub(now)))
		}

		select {
		case <-shutdown:
			isRunning = false
		case <-time.After(w.options.SleepDelay):
		}
	}
	logger.Info("No more jobs available. Exiting")
}

// Do num jobs and return stats on success/failure.
// Exit early if interrupted.
func (w *worker) workOff(ctx context.Context, logger log.Logger, num int) (int, int, error) {
	success, failure := 0, 0

	for i := 0; i < num; i++ {
		ok, e := w.reserveAndRunOneJob(ctx, logger)
		if nil != e {
			if e == ErrJobsEmpty {
				return success, failure, nil
			}
			return success, failure, e
		}

		if ok {
			success += 1
		} else {
			failure += 1
		}
	}

	return success, failure, nil
}

// Run the next job we can get an exclusive lock on.
// If no jobs are left we return nil
func (w *worker) reserveAndRunOneJob(ctx context.Context, logger log.Logger) (bool, error) {
	job, e := w.backend.Fetch(ctx, w.name, w.options.Queues)
	if nil != e {
		return false, e
	}

	if nil == job {
		return false, ErrJobsEmpty
	}

	ctx = job.createCtx(ctx)
	logger = log.For(ctx).With(log.Int64("id", job.ID),
		log.String("uuid", job.UUID), log.String("type", job.Type))

	return w.run(ctx, logger, job)
}

func (w *worker) run(ctx context.Context, logger log.Logger, job *Job) (bool, error) {
	logger.Info("RUNNING")
	now := time.Now()
	e := w.invokeJob(ctx, job)
	if nil != e {
		if nextTime, need := toRunAgain(e); need {
			logger.Info("COMPLETED and should again", log.Time("nextTime", nextTime))
			e = w.reschedule(ctx, logger, true, job, nextTime, nil)
			return true, e
		} else if isDeserializationError(e) {
			logger.Info("FAILED (deserialization)", log.Int("retried", job.Retried), log.Int("maxRetry", job.MaxRetry), log.Error(e))
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

func (w *worker) invokeJob(ctx context.Context, job *Job) (err error) {
	defer func() {
		if e := recover(); nil != e {
			msg := fmt.Sprintf("[panic]%v \r\n%s", e, debug.Stack())
			err = errors.New(msg)
		}
	}()

	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, job.execTimeout(w.options.MaxRunTime))
	e := w.mux.RunJob(ctx, job)
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

func (w *worker) completeIt(ctx context.Context, logger log.Logger, job *Job) error {
	return w.backend.Destroy(ctx, job.ID)
}

func (w *worker) failed(ctx context.Context, logger log.Logger, job *Job, e error) error {
	if w.options.DestroyFailedJobs {
		logger.Info("REMOVED permanently", log.Int("retried", job.Retried), log.Int("maxRetry", job.MaxRetry), log.Error(e))
		return w.backend.Destroy(ctx, job.ID)
	}
	logger.Info("STOPPED permanently", log.Int("retried", job.Retried), log.Int("maxRetry", job.MaxRetry), log.Error(e))
	return w.backend.Fail(ctx, job.ID, e.Error())
}

func (w *worker) handleFailedJob(ctx context.Context, logger log.Logger, job *Job, e error) error {
	logger.Info("FAILED", log.Int("retried", job.Retried), log.Int("maxRetry", job.MaxRetry), log.Error(e))
	return w.reschedule(ctx, logger, false, job, time.Time{}, e)
}

// Reschedule the job in the future (when a job fails).
// Uses an exponential scale depending on the number of failed attempts.
func (w *worker) reschedule(ctx context.Context, logger log.Logger, runAgain bool, job *Job, nextTime time.Time, e error) error {
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
