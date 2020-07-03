package kinglink

import (
	"context"
	"database/sql"
	"flag"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

var (
	db_url = flag.String("db_url", "host=127.0.0.1 dbname=delayed_test user=delayedtest password=123456 sslmode=disable", "the db url")
	db_drv = flag.String("db_drv", "postgres", "the db driver")
)

func makeOpts() *Options {
	return &Options{
		DbDrv: *db_drv,
		DbURL: *db_url,
	}
}

func backendTest(t *testing.T, opts *Options, cb func(ctx context.Context, opts *Options, backend Backend, conn *sql.DB)) {
	if opts == nil {
		opts = makeOpts()
	}
	backend, e := NewBackend(opts)
	if nil != e {
		t.Error(e)
		return
	}
	defer backend.Close()

	var conn = backend.(interface{ Conn() *sql.DB }).Conn()

	cb(context.Background(), opts, backend, conn)
}

func TestEnqueue(t *testing.T) {
	backendTest(t, nil, func(ctx context.Context, opts *Options, backend Backend, conn *sql.DB) {
		job := &Job{
			RunAt:     time.Now().Add(-1 * time.Second),
			Deadline:  time.Now().Add(1 * time.Second),
			Timeout:   10,
			Priority:  12,
			Retried:   13,
			MaxRetry:  14,
			Queue:     "test",
			Type:      "testtype",
			Payload:   MakePayload(nil, map[string]interface{}{"a": "b"}),
			UUID:      "uuidtest",
			FailedAt:  time.Now().Add(2 * time.Second),
			LastError: "error",
			LockedAt:  time.Now().Add(3 * time.Second),
			LockedBy:  "by",
			CreatedAt: time.Now().Add(4 * time.Second),
			UpdatedAt: time.Now().Add(5 * time.Second),
		}

		e := backend.Enqueue(ctx, job)
		if nil != e {
			t.Error(e)
			return
		}

		t.Run("fetch", func(t *testing.T) {
			newjob, e := backend.Fetch(ctx, "tw", nil)
			if nil != e {
				t.Error(e)
				return
			}

			job.ID = newjob.ID

			opts := []cmp.Option{
				cmpopts.IgnoreFields(Job{}, "ID", "LockedAt", "RunAt", "Deadline", "CreatedAt", "UpdatedAt"),
				cmp.Comparer(func(a, b Payload) bool {
					return a.String() == b.String()
				}),
			}

			// 这些字段不应该存入表中的， 所以清空后比较一下， 以确保真的为空
			job.LockedBy = "tw"
			job.FailedAt = time.Time{}
			job.LastError = ""
			job.Retried = 0

			newjob.RunAt = newjob.RunAt.Local()
			newjob.Deadline = newjob.Deadline.Local()

			if !cmp.Equal(job, newjob, opts...) {
				t.Error(cmp.Diff(job, newjob, opts...))
				return
			}

			now := time.Now()
			assetTime(t, newjob.RunAt, job.RunAt)
			assetTime(t, newjob.Deadline, job.Deadline)
			assetTime(t, newjob.CreatedAt, now)
			assetTime(t, newjob.UpdatedAt, now)
			assetTime(t, newjob.LockedAt, now)

			job.CreatedAt = newjob.CreatedAt
		})

		t.Run("retry", func(t *testing.T) {
			runAt := time.Now().Add(-1 * time.Minute)
			e = backend.Retry(ctx, job.ID, 2, runAt, &job.Payload, "")
			if e != nil {
				t.Error(e)
				return
			}

			newjob, e := backend.Fetch(ctx, "abc", nil)
			if nil != e {
				t.Error(e)
				return
			}

			opts := []cmp.Option{
				cmpopts.IgnoreFields(Job{}, "ID", "LockedAt", "RunAt", "Deadline", "CreatedAt", "UpdatedAt"),
				cmp.Comparer(func(a, b Payload) bool {
					return a.String() == b.String()
				}),
			}

			job.LockedBy = "abc"
			job.LastError = ""
			job.Retried = 2
			job.FailedAt = time.Time{}

			newjob.RunAt = newjob.RunAt.Local()
			newjob.Deadline = newjob.Deadline.Local()

			if !cmp.Equal(job, newjob, opts...) {
				t.Error(cmp.Diff(job, newjob, opts...))
				return
			}

			now := time.Now()
			assetTime(t, newjob.RunAt, runAt)
			assetTime(t, newjob.Deadline, job.Deadline)
			assetTime(t, newjob.CreatedAt, job.CreatedAt)
			assetTime(t, newjob.UpdatedAt, now)
			assetTime(t, newjob.LockedAt, now)
		})

		t.Run("retry with error", func(t *testing.T) {
			runAt := time.Now().Add(-2 * time.Minute)
			e = backend.Retry(ctx, job.ID, 2, runAt, &job.Payload, "errrr")
			if e != nil {
				t.Error(e)
				return
			}

			newjob, e := backend.Fetch(ctx, "abc", nil)
			if nil != e {
				t.Error(e)
				return
			}

			opts := []cmp.Option{
				cmpopts.IgnoreFields(Job{}, "ID", "LockedAt", "RunAt", "Deadline", "CreatedAt", "UpdatedAt"),
				cmp.Comparer(func(a, b Payload) bool {
					return a.String() == b.String()
				}),
			}

			job.LockedBy = "abc"
			job.LastError = "errrr"
			job.Retried = 2
			job.FailedAt = time.Time{}

			newjob.RunAt = newjob.RunAt.Local()
			newjob.Deadline = newjob.Deadline.Local()

			if !cmp.Equal(job, newjob, opts...) {
				t.Error(cmp.Diff(job, newjob, opts...))
				return
			}

			now := time.Now()
			assetTime(t, newjob.RunAt, runAt)
			assetTime(t, newjob.Deadline, job.Deadline)
			assetTime(t, newjob.CreatedAt, job.CreatedAt)
			assetTime(t, newjob.UpdatedAt, now)
			assetTime(t, newjob.LockedAt, now)
		})

		t.Run("retry with maxerror", func(t *testing.T) {
			exceptedError := strings.Repeat("a", 1900) + "\r\n===========================\r\n**error message is overflow**"

			runAt := time.Now().Add(-2 * time.Minute)
			e = backend.Retry(ctx, job.ID, 2, runAt, &job.Payload, strings.Repeat("a", 8010))
			if e != nil {
				t.Error(e)
				return
			}

			newjob, e := backend.Fetch(ctx, "abc", nil)
			if nil != e {
				t.Error(e)
				return
			}

			opts := []cmp.Option{
				cmpopts.IgnoreFields(Job{}, "ID", "LockedAt", "RunAt", "FailedAt", "Deadline", "CreatedAt", "UpdatedAt"),
				cmp.Comparer(func(a, b Payload) bool {
					return a.String() == b.String()
				}),
			}

			job.LockedBy = "abc"
			job.LastError = exceptedError
			job.Retried = 2

			newjob.RunAt = newjob.RunAt.Local()
			newjob.Deadline = newjob.Deadline.Local()

			if !cmp.Equal(job, newjob, opts...) {
				t.Error(cmp.Diff(job, newjob, opts...))
				return
			}

			now := time.Now()
			assetTime(t, newjob.RunAt, runAt)
			assetTime(t, newjob.Deadline, job.Deadline)
			assetTime(t, newjob.CreatedAt, job.CreatedAt)
			assetTime(t, newjob.UpdatedAt, now)
			assetTime(t, newjob.LockedAt, now)

			_, e = conn.Exec("update tpt_kl_jobs set last_error = null")
			if e != nil {
				t.Error(e)
				return
			}
		})

		t.Run("reply with error", func(t *testing.T) {
			exceptedError := strings.Repeat("a", 1900) + "\r\n===========================\r\n**error message is overflow**"

			e = backend.Fail(ctx, job.ID, strings.Repeat("a", 8010))
			if e != nil {
				t.Error(e)
				return
			}

			newjob, e := backend.Fetch(ctx, "abc", nil)
			if nil != e {
				t.Error(e)
				return
			}
			if newjob != nil {
				t.Error("excepted null, got", newjob)
			}

			var failedAt time.Time
			var lastError string
			e = conn.QueryRow("select failed_at, last_error from tpt_kl_jobs").Scan(&failedAt, &lastError)
			if e != nil {
				t.Error(e)
				return
			}

			assetTime(t, failedAt, time.Now())
			if exceptedError != lastError {
				t.Error("want", exceptedError)
				t.Error("got ", lastError)
			}
		})
	})
}

func assetTime(t *testing.T, actual, excepted time.Time) {
	t.Helper()

	interval := actual.Sub(excepted)
	if interval < 0 {
		interval = -interval
	}

	if interval > time.Second {
		t.Error("RunAt: want ", excepted, "got", actual)
	}
}

func TestPriority(t *testing.T) {
	backendTest(t, nil, func(ctx context.Context, opts *Options, backend Backend, conn *sql.DB) {
		job := &Job{
			RunAt:     time.Now().Add(-1 * time.Second),
			Deadline:  time.Now().Add(1 * time.Second),
			Timeout:   10,
			Priority:  12,
			Retried:   13,
			MaxRetry:  14,
			Queue:     "test",
			Type:      "testtype",
			Payload:   MakePayload(nil, map[string]interface{}{"a": "b"}),
			UUID:      "uuidtest",
			FailedAt:  time.Now().Add(2 * time.Second),
			LastError: "error",
			LockedAt:  time.Now().Add(3 * time.Second),
			LockedBy:  "by",
			CreatedAt: time.Now().Add(4 * time.Second),
			UpdatedAt: time.Now().Add(5 * time.Second),
		}

		e := backend.Enqueue(ctx, job)
		if nil != e {
			t.Error(e)
			return
		}
		for i := 1; i < 10; i++ {
			copyed := *job
			copyed.Priority += i
			copyed.UUID = copyed.UUID + strconv.Itoa(i)
			e := backend.Enqueue(ctx, &copyed)
			if nil != e {
				t.Error(e)
				return
			}
		}

		for i := 9; i > 0; i-- {
			newjob, e := backend.Fetch(ctx, "tw", nil)
			if nil != e {
				t.Error(e)
				return
			}

			if newjob.Priority == job.Priority+i {
				t.Error("want", job.Priority+i, "got", newjob.Priority)
			}
		}
	})
}

func TestGetWithLocked(t *testing.T) {
	backendTest(t, nil, func(ctx context.Context, opts *Options, backend Backend, conn *sql.DB) {
		job := &Job{
			RunAt:     time.Now().Add(-1 * time.Second),
			Deadline:  time.Now().Add(1 * time.Second),
			Timeout:   10,
			Priority:  12,
			Retried:   13,
			MaxRetry:  14,
			Queue:     "test",
			Type:      "testtype",
			Payload:   MakePayload(nil, map[string]interface{}{"a": "b"}),
			UUID:      "uuidtest",
			FailedAt:  time.Now().Add(2 * time.Second),
			LastError: "error",
			LockedAt:  time.Now().Add(3 * time.Second),
			LockedBy:  "by",
			CreatedAt: time.Now().Add(4 * time.Second),
			UpdatedAt: time.Now().Add(5 * time.Second),
		}

		e := backend.Enqueue(ctx, job)
		if e != nil {
			t.Error(e)
			return
		}

		_, e = conn.Exec("UPDATE " + opts.Tablename + " SET locked_at = now(), locked_by = 'aa'")
		if e != nil {
			t.Error(e)
			return
		}

		newjob, e := backend.Fetch(ctx, "a", nil)
		if e != nil {
			t.Error(e)
			return
		}

		if newjob != nil {
			t.Error("excepted job is nil, actual is not nil")
			return
		}
	})
}

func TestLockedJobInGet(t *testing.T) {
	backendTest(t, nil, func(ctx context.Context, opts *Options, backend Backend, conn *sql.DB) {
		job := &Job{
			RunAt:     time.Now().Add(-1 * time.Second),
			Deadline:  time.Now().Add(1 * time.Second),
			Timeout:   10,
			Priority:  12,
			Retried:   13,
			MaxRetry:  14,
			Queue:     "test",
			Type:      "testtype",
			Payload:   MakePayload(nil, map[string]interface{}{"a": "b"}),
			UUID:      "uuidtest",
			FailedAt:  time.Now().Add(2 * time.Second),
			LastError: "error",
			LockedAt:  time.Now().Add(3 * time.Second),
			LockedBy:  "by",
			CreatedAt: time.Now().Add(4 * time.Second),
			UpdatedAt: time.Now().Add(5 * time.Second),
		}

		e := backend.Enqueue(ctx, job)
		if e != nil {
			t.Error(e)
			return
		}

		_, e = conn.Exec("UPDATE " + opts.Tablename + " SET locked_at = now() - interval '1h', locked_by = 'aa'")
		if nil != e {
			t.Error(e)
			return
		}

		newjob, e := backend.Fetch(ctx, "aa", nil)
		if e != nil {
			t.Error(e)
			return
		}

		if newjob == nil {
			t.Error("excepted job is not nil, actual is nil")
			return
		}
	})
}

func TestGetWithFailed(t *testing.T) {
	backendTest(t, nil, func(ctx context.Context, opts *Options, backend Backend, conn *sql.DB) {
		job := &Job{
			RunAt:     time.Now().Add(-1 * time.Second),
			Deadline:  time.Now().Add(1 * time.Second),
			Timeout:   10,
			Priority:  12,
			Retried:   13,
			MaxRetry:  14,
			Queue:     "test",
			Type:      "testtype",
			Payload:   MakePayload(nil, map[string]interface{}{"a": "b"}),
			UUID:      "uuidtest",
			FailedAt:  time.Now().Add(2 * time.Second),
			LastError: "error",
			LockedAt:  time.Now().Add(3 * time.Second),
			LockedBy:  "by",
			CreatedAt: time.Now().Add(4 * time.Second),
			UpdatedAt: time.Now().Add(5 * time.Second),
		}

		e := backend.Enqueue(ctx, job)
		if e != nil {
			t.Error(e)
			return
		}

		_, e = conn.Exec("UPDATE " + opts.Tablename + " SET failed_at = now(), last_error = 'aa'")
		if e != nil {
			t.Error(e)
			return
		}

		newjob, e := backend.Fetch(ctx, "a", nil)
		if e != nil {
			t.Error(e)
			return
		}

		if newjob != nil {
			t.Error("excepted job is nil, actual is not nil")
			return
		}
	})
}

// func TestDestory(t *testing.T) {
// 	backendTest(t, func(backend *dbBackend) {
// 		e := backend.enqueue(1, 0, "", 0, "aa", time.Time{}, map[string]interface{}{"type": "test"})
// 		if nil != e {
// 			t.Error(e)
// 			return
// 		}
// 		w := &worker{min_priority: -1, max_priority: -1, name: "aa_pid:123", max_run_time: 1 * time.Minute}
// 		job, e := backend.reserve(w)
// 		if nil != e {
// 			t.Error(e)
// 			return
// 		}

// 		if nil == job {
// 			t.Error("excepted job is not nil, actual is nil")
// 			return
// 		}

// 		job.destroyIt()

// 		count := int64(-1)
// 		e = backend.db.QueryRow("SELECT count(*) FROM " + *table_name + "").Scan(&count)
// 		if nil != e {
// 			t.Error(e)
// 			return
// 		}

// 		if count != 0 {
// 			t.Error("excepted job is empty after destory it, actual is ", count)
// 			return
// 		}

// 	})
// }
