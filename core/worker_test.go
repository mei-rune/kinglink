package core

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	stdlog "log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/runner-mei/log"
)

func workTest(t *testing.T, o *DbOptions, w *WorkOptions, mux *ServeMux, cb func(ctx context.Context, logger log.Logger, w *Worker, backend Backend, dbOpts *DbOptions, conn *sql.DB)) {
	backendTest(t, o, w, func(ctx context.Context, dbopts *DbOptions, wopts *WorkOptions, backend Backend, conn *sql.DB) {
		w, err := NewWorker(wopts, mux, backend)
		if err != nil {
			t.Error(err)
			return
		}

		logger := log.NewStdLogger(stdlog.New(os.Stderr, "", stdlog.LstdFlags|stdlog.Lshortfile))
		logger = log.For(ctx, logger)
		ctx = log.ContextWithLogger(ctx, logger)
		cb(ctx, logger, w, backend, dbopts, conn)
	})
}

func TestWork(t *testing.T) {
	workTest(t, nil, nil, nil, func(ctx context.Context, logger log.Logger, w *Worker, backend Backend, dbOpts *DbOptions, conn *sql.DB) {
		_, _, e := w.WorkOff(ctx, logger, 1, 10)
		if e != nil {
			t.Error(e)
		}
	})
}

func TestWorkWithCancel(t *testing.T) {
	workTest(t, nil, nil, nil, func(ctx context.Context, logger log.Logger, w *Worker, backend Backend, dbOpts *DbOptions, conn *sql.DB) {

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		w.Run(ctx, 1, false)
		w.Run(ctx, 1, true)
	})
}

func TestRunJob(t *testing.T) {
	c := make(chan string, 1)
	mux := NewServeMux()
	mux.Handle("test", HandlerFunc(func(ctx *Context, job *Job) error {
		fields, err := job.Payload.Fields()
		if err != nil {
			return err
		}

		s, ok := fields["a"]
		if !ok {
			return errors.New("a is missing")
		}

		c <- fmt.Sprint(s)
		return nil
	}))
	workTest(t, nil, nil, mux, func(ctx context.Context, logger log.Logger, w *Worker, backend Backend, dbOpts *DbOptions, conn *sql.DB) {
		_, e := Enqueue(ctx, backend, "test", map[string]interface{}{"a": "b"})
		if nil != e {
			t.Error(e)
			return
		}

		success, failure, e := w.WorkOff(ctx, logger, 1, 1)
		if nil != e {
			t.Error(e)
		}
		if success != 1 {
			t.Error("want 1 got", success)
		}
		if failure != 0 {
			t.Error("want 0 got", failure)
		}

		select {
		case s := <-c:
			if s != "b" {
				t.Error("want b got", s)
			}
		case <-time.After(2 * time.Second):
			t.Error("not recv")
		}
	})
}

func assertSQLCount(t *testing.T, conn *sql.DB, sqlstr string, excepted int) {
	t.Helper()

	var count int
	row := conn.QueryRow(sqlstr)
	err := row.Scan(&count)
	if err != nil {
		t.Error(err)
		return
	}

	if excepted != count {
		t.Error("want", excepted, "got", count)
	}
}

func TestRunErrorAndNoRescheduleIt(t *testing.T) {
	mux := NewServeMux()
	mux.Handle("test", HandlerFunc(func(ctx *Context, job *Job) error {
		return errors.New("ok error")
	}))
	workTest(t, nil, nil, mux, func(ctx context.Context, logger log.Logger, w *Worker, backend Backend, dbOpts *DbOptions, conn *sql.DB) {
		_, e := Enqueue(ctx, backend, "test", map[string]interface{}{"a": "b"})
		if nil != e {
			t.Error(e)
			return
		}

		assertSQLCount(t, conn, "select count(*) from "+dbOpts.RunningTablename, 1)

		success, failure, e := w.WorkOff(ctx, logger, 1, 1)
		if e != nil {
			t.Error(e)
		}
		if success != 0 {
			t.Error("want 0 got", success)
		}
		if failure != 1 {
			t.Error("want 1 got", failure)
		}

		assertSQLCount(t, conn, "SELECT COUNT(*) FROM "+dbOpts.ResultTablename+" WHERE last_error IS NOT NULL", 1)
	})
}

func TestRunPanicAndNoRescheduleIt(t *testing.T) {
	mux := NewServeMux()
	mux.Handle("test", HandlerFunc(func(ctx *Context, job *Job) error {
		panic(errors.New("[PANIC]"))
		return errors.New("ok error")
	}))
	workTest(t, nil, nil, mux, func(ctx context.Context, logger log.Logger, w *Worker, backend Backend, dbOpts *DbOptions, conn *sql.DB) {
		_, e := Enqueue(ctx, backend, "test", map[string]interface{}{"a": "b"})
		if nil != e {
			t.Error(e)
			return
		}

		assertSQLCount(t, conn, "select count(*) from "+dbOpts.RunningTablename, 1)

		success, failure, e := w.WorkOff(ctx, logger, 1, 1)
		if e != nil {
			t.Error(e)
		}
		if success != 0 {
			t.Error("want 0 got", success)
		}
		if failure != 1 {
			t.Error("want 1 got", failure)
		}

		assertSQLCount(t, conn, "SELECT COUNT(*) FROM "+dbOpts.ResultTablename+" WHERE last_error IS NOT NULL", 1)
	})
}

func TestRunMaxLenErrorAndNoRescheduleIt(t *testing.T) {
	mux := NewServeMux()
	mux.Handle("test", HandlerFunc(func(ctx *Context, job *Job) error {
		return errors.New(strings.Repeat("aaa", 2000))
	}))
	workTest(t, nil, nil, mux, func(ctx context.Context, logger log.Logger, w *Worker, backend Backend, dbOpts *DbOptions, conn *sql.DB) {
		_, e := Enqueue(ctx, backend, "test", map[string]interface{}{"a": "b"})
		if nil != e {
			t.Error(e)
			return
		}

		assertSQLCount(t, conn, "select count(*) from "+dbOpts.RunningTablename, 1)

		success, failure, e := w.WorkOff(ctx, logger, 1, 1)
		if e != nil {
			t.Error(e)
		}
		if success != 0 {
			t.Error("want 0 got", success)
		}
		if failure != 1 {
			t.Error("want 1 got", failure)
		}

		assertSQLCount(t, conn, "SELECT COUNT(*) FROM "+dbOpts.ResultTablename+" WHERE last_error IS NOT NULL", 1)
	})
}

func TestRunErrorAndRescheduleIt(t *testing.T) {
	mux := NewServeMux()
	mux.Handle("test", HandlerFunc(func(ctx *Context, job *Job) error {
		return errors.New("ok error")
	}))
	workTest(t, nil, nil, mux, func(ctx context.Context, logger log.Logger, w *Worker, backend Backend, dbOpts *DbOptions, conn *sql.DB) {
		_, e := Enqueue(ctx, backend, "test", map[string]interface{}{"a": "b"}, MaxRetry(1))
		if nil != e {
			t.Error(e)
			return
		}

		assertSQLCount(t, conn, "select count(*) from "+dbOpts.RunningTablename, 1)

		success, failure, e := w.WorkOff(ctx, logger, 1, 1)
		if e != nil {
			t.Error(e)
		}
		if success != 0 {
			t.Error("want 0 got", success)
		}
		if failure != 1 {
			t.Error("want 1 got", failure)
		}

		assertSQLCount(t, conn, "SELECT COUNT(*) FROM "+dbOpts.ResultTablename+" WHERE last_error IS NOT NULL", 0)
	})
}

func TestRunMaxLenErrorAndRescheduleIt(t *testing.T) {
	mux := NewServeMux()
	mux.Handle("test", HandlerFunc(func(ctx *Context, job *Job) error {
		return errors.New(strings.Repeat("aaa", 2000))
	}))
	workTest(t, nil, nil, mux, func(ctx context.Context, logger log.Logger, w *Worker, backend Backend, dbOpts *DbOptions, conn *sql.DB) {
		_, e := Enqueue(ctx, backend, "test", map[string]interface{}{"a": "b"}, MaxRetry(1))
		if nil != e {
			t.Error(e)
			return
		}

		assertSQLCount(t, conn, "select count(*) from "+dbOpts.RunningTablename, 1)

		success, failure, e := w.WorkOff(ctx, logger, 1, 1)
		if e != nil {
			t.Error(e)
		}
		if success != 0 {
			t.Error("want 0 got", success)
		}
		if failure != 1 {
			t.Error("want 1 got", failure)
		}

		assertSQLCount(t, conn, "SELECT COUNT(*) FROM "+dbOpts.ResultTablename+" WHERE last_error IS NOT NULL", 0)
	})
}

func TestRunErrorAndFailAfterMaxRetry(t *testing.T) {
	mux := NewServeMux()
	mux.Handle("test", HandlerFunc(func(ctx *Context, job *Job) error {
		return errors.New("ok error")
	}))
	workTest(t, nil, nil, mux, func(ctx context.Context, logger log.Logger, w *Worker, backend Backend, dbOpts *DbOptions, conn *sql.DB) {
		_, e := Enqueue(ctx, backend, "test", map[string]interface{}{"a": "b"}, MaxRetry(2))
		if nil != e {
			t.Error(e)
			return
		}

		assertSQLCount(t, conn, "select count(*) from "+dbOpts.RunningTablename, 1)

		tryRun := 3
		for i := 0; i < tryRun; i++ {
			success, failure, e := w.WorkOff(ctx, logger, 1, 3)
			if e != nil {
				t.Error(e)
			}
			if success != 0 {
				t.Error("want 0 got", success)
			}
			if failure != 1 {
				t.Error("want 1 got", failure)
			}

			if i == (tryRun - 1) {
				break
			}

			var runAt time.Time
			e = conn.QueryRow("SELECT run_at FROM " + dbOpts.RunningTablename).Scan(&runAt)
			if nil != e {
				t.Error(e)
				return
			}

			if i < tryRun-1 {
				assetTime(t, "runAt", runAt, time.Now().Add((time.Duration(i)*10+5)*time.Second))
			}

			_, e = conn.Exec("UPDATE "+dbOpts.RunningTablename+" SET run_at = $1", time.Now().Add(-1*time.Second))
			if nil != e {
				t.Error(e)
				return
			}
		}

		assertSQLCount(t, conn, "SELECT COUNT(*) FROM "+dbOpts.RunningTablename, 0)
		assertSQLCount(t, conn, "SELECT COUNT(*) FROM "+dbOpts.ResultTablename+" WHERE last_error IS NOT NULL", 1)
	})
}

func TestRunErrorAndFailAfterDefaultMaxRetry(t *testing.T) {
	mux := NewServeMux()
	mux.Handle("test", HandlerFunc(func(ctx *Context, job *Job) error {
		return errors.New("ok error")
	}))
	woptions := &WorkOptions{MaxRetry: 3}
	workTest(t, nil, woptions, mux, func(ctx context.Context, logger log.Logger, w *Worker, backend Backend, dbOpts *DbOptions, conn *sql.DB) {
		_, e := Enqueue(ctx, backend, "test", map[string]interface{}{"a": "b"}, MaxRetry(12))
		if nil != e {
			t.Error(e)
			return
		}

		assertSQLCount(t, conn, "select count(*) from "+dbOpts.RunningTablename, 1)

		_, e = conn.Exec("update " + dbOpts.RunningTablename + " set Retried = " + fmt.Sprint(woptions.MaxRetry+1))
		if e != nil {
			t.Error(e)
			return
		}

		success, failure, e := w.WorkOff(ctx, logger, 1, 3)
		if e != nil {
			t.Error(e)
		}
		if success != 0 {
			t.Error("want 0 got", success)
		}
		if failure != 1 {
			t.Error("want 1 got", failure)
		}

		assertSQLCount(t, conn, "SELECT COUNT(*) FROM "+dbOpts.RunningTablename, 1)
		assertSQLCount(t, conn, "SELECT COUNT(*) FROM "+dbOpts.ResultTablename, 0)
	})
}

func TestRunAgain(t *testing.T) {
	count := 2
	mux := NewServeMux()
	mux.Handle("test", HandlerFunc(func(ctx *Context, job *Job) error {
		if count <= 0 {
			return nil
		}
		count--
		return RunAgain(time.Now().Add(-1 * time.Second))
	}))
	workTest(t, nil, nil, mux, func(ctx context.Context, logger log.Logger, w *Worker, backend Backend, dbOpts *DbOptions, conn *sql.DB) {
		_, e := Enqueue(ctx, backend, "test", map[string]interface{}{"a": "b"}, MaxRetry(1))
		if nil != e {
			t.Error(e)
			return
		}

		assertSQLCount(t, conn, "select count(*) from "+dbOpts.RunningTablename, 1)

		success, failure, e := w.WorkOff(ctx, logger, 1, 3)
		if e != nil {
			t.Error(e)
		}
		if success != 3 {
			t.Error("want 3 got", success)
		}
		if failure != 0 {
			t.Error("want 0 got", failure)
		}

		assertSQLCount(t, conn, "SELECT COUNT(*) FROM "+dbOpts.RunningTablename, 0)
		assertSQLCount(t, conn, "SELECT COUNT(*) FROM "+dbOpts.ResultTablename+" WHERE last_error IS NULL", 1)
	})
}

// func TestRunFailedAndNotDestoryIt2(t *testing.T) {
// 	*default_destroy_failed_jobs = false
// 	workTest(t, nil, nil, func(ctx context.Context, logger log.Logger, w *Worker,  backend Backend, dbOpts *DbOptions, conn *sql.DB) {
// 		e := backend.enqueue(1, 0, "", 1, "aa", time.Time{}, map[string]interface{}{"type": "test", "try_interval": "0s", "error": "throw a"})
// 		if nil != e {
// 			t.Error(e)
// 			return
// 		}

// 		select {
// 		case <-test_chan:
// 		case <-time.After(4 * time.Second):
// 			t.Error("not recv")
// 		}
// 		time.Sleep(1500 * time.Millisecond)

// 		rows, e := backend.db.Query("SELECT last_error FROM " + *table_name)
// 		if nil != e {
// 			t.Error(e)
// 			return
// 		}

// 		for rows.Next() {
// 			var last_error sql.NullString

// 			e = rows.Scan(&last_error)
// 			if nil != e {
// 				t.Error(e)
// 				return
// 			}

// 			if !last_error.Valid {
// 				t.Error("excepted last_error is not empty, actual is invalid")
// 			}

// 			if last_error.Valid && "throw a" != last_error.String {
// 				t.Error("excepted run_at is 'throw a', actual is", last_error.String)
// 			}
// 		}
// 	})
// }

// func TestRunFailedAndNotDestoryIt(t *testing.T) {
// 	*default_destroy_failed_jobs = false
// 	workTest(t, nil, nil, func(ctx context.Context, logger log.Logger, w *Worker,  backend Backend, dbOpts *DbOptions, conn *sql.DB) {
// 		e := backend.enqueue(1, 0, "", 1, "aa", time.Time{}, map[string]interface{}{"type": "test", "try_interval": "0s", "error": "throw a"})
// 		if nil != e {
// 			t.Error(e)
// 			return
// 		}

// 		select {
// 		case <-test_chan:
// 		case <-time.After(4 * time.Second):
// 			t.Error("not recv")
// 		}
// 		time.Sleep(1500 * time.Millisecond)

// 		rows, e := backend.db.Query("SELECT attempts, run_at, locked_at, locked_by, handler, last_error FROM " + *table_name)
// 		if nil != e {
// 			t.Error(e)
// 			return
// 		}

// 		for rows.Next() {
// 			var attempts int64
// 			var run_at NullTime
// 			var locked_at NullTime
// 			var locked_by sql.NullString
// 			var handler sql.NullString
// 			var last_error sql.NullString

// 			e = rows.Scan(&attempts, &run_at, &locked_at, &locked_by, &handler, &last_error)
// 			if nil != e {
// 				t.Error(e)
// 				return
// 			}

// 			if !run_at.Valid {
// 				t.Error("excepted run_at is valid, actual is invalid")
// 			}
// 			// if locked_at.Valid {
// 			// 	t.Error("excepted locked_at is invalid, actual is valid - ", locked_at.Time)
// 			// }
// 			// if locked_by.Valid {
// 			// 	t.Error("excepted locked_by is invalid, actual is valid - ", locked_by.String)
// 			// }

// 			if !handler.Valid {
// 				t.Error("excepted handler is not empty, actual is invalid")
// 			}

// 			if !last_error.Valid {
// 				t.Error("excepted last_error is not empty, actual is invalid")
// 			}

// 			// if 1 != attempts {
// 			// 	t.Error("excepted attempts is '1', and actual is ", attempts)
// 			// }

// 			//if !strings.Contains(*db_drv, "mysql") {
// 			now := backend.db_time_now()
// 			interval := now.Sub(run_at.Time)
// 			if interval < 0 {
// 				interval = -interval
// 			}
// 			if interval > 1*time.Second {
// 				t.Error("excepted run_at is ", now, ", actual is", run_at.Time, "interval is ", interval)
// 			}
// 			//}

// 			if !strings.Contains(handler.String, "\"type\": \"test\"") {
// 				t.Error("excepted handler contains '\"type\": \"test\"', actual is ", handler.String)
// 			}

// 			if last_error.Valid && "throw a" != last_error.String {
// 				t.Error("excepted run_at is 'throw a', actual is", last_error.String)
// 			}
// 		}
// 	})
// }

// func TestRunFailedAndDestoryIt(t *testing.T) {
// 	*default_destroy_failed_jobs = true
// 	workTest(t, nil, nil, func(ctx context.Context, logger log.Logger, w *Worker,  backend Backend, dbOpts *DbOptions, conn *sql.DB) {
// 		e := backend.enqueue(1, 0, "", 1, "aa", time.Time{}, map[string]interface{}{"type": "test", "try_interval": "0s", "error": "throw a"})
// 		if nil != e {
// 			t.Error(e)
// 			return
// 		}

// 		select {
// 		case <-test_chan:
// 		case <-time.After(2 * time.Second):
// 			t.Error("not recv")
// 		}

// 		var count int64
// 		for i := 0; i < 10; i++ {
// 			time.Sleep(500 * time.Millisecond)

// 			e = backend.db.QueryRow("SELECT count(*) FROM " + *table_name).Scan(&count)
// 			if nil != e {
// 				t.Error(e)
// 				return
// 			}
// 			if count == 0 {
// 				break
// 			}
// 		}

// 		if 0 != count {
// 			t.Error("excepted jobs is empty, actual is", count)
// 		}
// 	})
// }
