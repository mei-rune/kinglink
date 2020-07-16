package kinglink

import (
	"context"
	"database/sql"
	"fmt"
	stdlog "log"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/runner-mei/kinglink/tests/common"
	"github.com/runner-mei/log"
)

var (
	DBUrl = common.DBUrl
	DBDrv = common.DBDrv
)

func MakeOpts() *DbOptions {
	return &DbOptions{
		DbDrv: *DBDrv,
		DbURL: *DBUrl,
	}
}

func backendTest(t *testing.T, opts *DbOptions, wopts *WorkOptions, cb func(ctx context.Context, opts *DbOptions, wopts *WorkOptions, backend Backend, conn *sql.DB)) {
	if opts == nil {
		opts = MakeOpts()
	}
	if wopts == nil {
		wopts = &WorkOptions{}
	}
	backend, e := NewBackend(opts, wopts)
	if nil != e {
		t.Error(e)
		return
	}
	defer backend.Close()

	var conn = backend.(interface{ Conn() *sql.DB }).Conn()

	logger := log.NewStdLogger(stdlog.New(os.Stderr, "", stdlog.LstdFlags|stdlog.Lshortfile))
	ctx := log.ContextWithLogger(context.Background(), logger)

	cb(ctx, opts, wopts, backend, conn)
}

func TestEnqueue(t *testing.T) {
	backendTest(t, nil, nil, func(ctx context.Context, opts *DbOptions, wopts *WorkOptions, backend Backend, conn *sql.DB) {
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

		t.Run("fetch", func(t *testing.T) {
			backend.ClearAll(ctx)
			_, e := backend.Enqueue(ctx, job)
			if nil != e {
				t.Error(e)
				return
			}

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
			assetTime(t, "RunAt", newjob.RunAt, job.RunAt)
			assetTime(t, "Deadline", newjob.Deadline, job.Deadline)
			assetTime(t, "CreatedAt", newjob.CreatedAt, now)
			assetTime(t, "UpdatedAt", newjob.UpdatedAt, now)
			assetTime(t, "LockedAt", newjob.LockedAt, now)
		})

		t.Run("retry_with_ok", func(t *testing.T) {
			backend.ClearAll(ctx)
			id, e := backend.Enqueue(ctx, job)
			if nil != e {
				t.Error(e)
				return
			}

			runAt := time.Now().Add(-1 * time.Minute)
			e = backend.Retry(ctx, id, 2, runAt, &job.Payload, "")
			if e != nil {
				t.Error(e)
				return
			}

			fmt.Println("============")
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

			// 这些字段不应该存入表中的， 所以清空后比较一下， 以确保真的为空
			job.LockedBy = "abc"
			job.LastError = ""
			job.Retried = 2
			job.FailedAt = time.Time{}

			if !cmp.Equal(job, newjob, opts...) {
				t.Error(cmp.Diff(job, newjob, opts...))
				return
			}

			now := time.Now()
			assetTime(t, "RunAt", newjob.RunAt.Local(), runAt.Local())
			assetTime(t, "Deadline", newjob.Deadline.Local(), job.Deadline)
			assetTime(t, "CreatedAt", newjob.CreatedAt, now)
			assetTime(t, "UpdatedAt", newjob.UpdatedAt, now)
			assetTime(t, "LockedAt", newjob.LockedAt, now)

			state, err := backend.GetState(ctx, id)
			if err != nil {
				t.Error(err)
				return
			}

			opts = []cmp.Option{
				cmpopts.IgnoreFields(JobState{}, "ID", "LockedAt", "RunAt", "Deadline", "CreatedAt", "LastAt"),
				cmp.Comparer(func(a, b Payload) bool {
					return a.String() == b.String()
				}),
			}

			excepted := &JobState{
				ID:        id,
				Type:      job.Type,
				Payload:   job.Payload,
				UniqueKey: job.UUID,
				Timeout:   strconv.Itoa(job.Timeout) + "s",
				Queue:     job.Queue,
				Priority:  job.Priority,
				MaxRetry:  job.MaxRetry,
				Retried:   job.Retried,
				// LogMessages []string
				LastError: job.LastError,
				RunBy:     "abc",
				Status:    StatusRunning,
			}
			if !cmp.Equal(state, excepted, opts...) {
				t.Error(cmp.Diff(state, excepted, opts...))
				return
			}

			assetTime(t, "RunAt", state.RunAt, runAt)
			assetTime(t, "Deadline", state.Deadline, job.Deadline)
			assetTime(t, "CreatedAt", state.CreatedAt, now)
			assetTime(t, "LastAt", state.LastAt, now)
			assetTime(t, "LockedAt", state.LockedAt, now)

		})

		t.Run("retry with error", func(t *testing.T) {
			backend.ClearAll(ctx)
			id, e := backend.Enqueue(ctx, job)
			if nil != e {
				t.Error(e)
				return
			}

			runAt := time.Now().Add(-2 * time.Minute)
			e = backend.Retry(ctx, id, 2, runAt, &job.Payload, "errrr")
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

			// 这些字段不应该存入表中的， 所以清空后比较一下， 以确保真的为空
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
			assetTime(t, "RunAt", newjob.RunAt, runAt)
			assetTime(t, "Deadline", newjob.Deadline, job.Deadline)
			assetTime(t, "CreatedAt", newjob.CreatedAt, now)
			assetTime(t, "UpdatedAt", newjob.UpdatedAt, now)
			assetTime(t, "LockedAt", newjob.LockedAt, now)

			state, err := backend.GetState(ctx, id)
			if err != nil {
				t.Error(err)
				return
			}

			opts = []cmp.Option{
				cmpopts.IgnoreFields(JobState{}, "ID", "LockedAt", "RunAt", "Deadline", "CreatedAt", "LastAt"),
				cmp.Comparer(func(a, b Payload) bool {
					return a.String() == b.String()
				}),
			}

			excepted := &JobState{
				ID:        id,
				Type:      job.Type,
				Payload:   job.Payload,
				UniqueKey: job.UUID,
				Timeout:   strconv.Itoa(job.Timeout) + "s",
				Queue:     job.Queue,
				Priority:  job.Priority,
				MaxRetry:  job.MaxRetry,
				Retried:   job.Retried,
				// LogMessages []string
				LastError: job.LastError,
				RunBy:     "abc",
				Status:    StatusFailAndRequeueing,
			}
			if !cmp.Equal(state, excepted, opts...) {
				t.Error(cmp.Diff(state, excepted, opts...))
				return
			}

			assetTime(t, "RunAt", state.RunAt, runAt)
			assetTime(t, "Deadline", state.Deadline, job.Deadline)
			assetTime(t, "CreatedAt", state.CreatedAt, now)
			assetTime(t, "LastAt", state.LastAt, now)
			assetTime(t, "LockedAt", state.LockedAt, now)

			t.Log("测试一下 GetStates")
			states, err := backend.GetStates(ctx, nil, 0, 0)
			if err != nil {
				t.Error(err)
				return
			}
			state = &states[0]

			if !cmp.Equal(state, excepted, opts...) {
				t.Error(cmp.Diff(state, excepted, opts...))
				return
			}

			assetTime(t, "RunAt", state.RunAt, runAt)
			assetTime(t, "Deadline", state.Deadline, job.Deadline)
			assetTime(t, "CreatedAt", state.CreatedAt, now)
			assetTime(t, "LastAt", state.LastAt, now)
			assetTime(t, "LockedAt", state.LockedAt, now)

		})

		t.Run("retry with maxerror", func(t *testing.T) {
			backend.ClearAll(ctx)
			id, e := backend.Enqueue(ctx, job)
			if nil != e {
				t.Error(e)
				return
			}

			exceptedError := strings.Repeat("a", 1900) + "\r\n===========================\r\n**error message is overflow**"

			runAt := time.Now().Add(-2 * time.Minute)
			e = backend.Retry(ctx, id, 2, runAt, &job.Payload, strings.Repeat("a", 8010))
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

			// 这些字段不应该存入表中的， 所以清空后比较一下， 以确保真的为空
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
			assetTime(t, "RunAt", newjob.RunAt, runAt)
			assetTime(t, "Deadline", newjob.Deadline, job.Deadline)
			assetTime(t, "CreatedAt", newjob.CreatedAt, now)
			assetTime(t, "UpdatedAt", newjob.UpdatedAt, now)
			assetTime(t, "LockedAt", newjob.LockedAt, now)

			_, e = conn.Exec("update tpt_kl_jobs set last_error = null")
			if e != nil {
				t.Error(e)
				return
			}
		})

		t.Run("check state when retry with ok", func(t *testing.T) {
			backend.ClearAll(ctx)
			id, e := backend.Enqueue(ctx, job)
			if nil != e {
				t.Error(e)
				return
			}

			runAt := time.Now().Add(-1 * time.Minute)
			e = backend.Retry(ctx, id, 2, runAt, &job.Payload, "")
			if e != nil {
				t.Error(e)
				return
			}

			// 这些字段不应该存入表中的， 所以清空后比较一下， 以确保真的为空
			job.LockedBy = "abc"
			job.LastError = ""
			job.Retried = 2
			job.FailedAt = time.Time{}

			state, err := backend.GetState(ctx, id)
			if err != nil {
				t.Error(err)
				return
			}

			opts := []cmp.Option{
				cmpopts.IgnoreFields(JobState{}, "ID", "LockedAt", "RunAt", "Deadline", "CreatedAt", "LastAt"),
				cmp.Comparer(func(a, b Payload) bool {
					return a.String() == b.String()
				}),
			}

			excepted := &JobState{
				ID:        id,
				Type:      job.Type,
				Payload:   job.Payload,
				UniqueKey: job.UUID,
				Timeout:   strconv.Itoa(job.Timeout) + "s",
				Queue:     job.Queue,
				Priority:  job.Priority,
				MaxRetry:  job.MaxRetry,
				Retried:   job.Retried,
				// LogMessages []string
				LastError: job.LastError,
				// RunBy:     "abc",
				Status: StatusQueueing,
			}
			if !cmp.Equal(state, excepted, opts...) {
				t.Error(cmp.Diff(state, excepted, opts...))
				return
			}

			now := time.Now()
			assetTime(t, "RunAt", state.RunAt, runAt)
			assetTime(t, "Deadline", state.Deadline, job.Deadline)
			assetTime(t, "CreatedAt", state.CreatedAt, now)
			assetTime(t, "LastAt", state.LastAt, now)
			assetTime(t, "LockedAt", state.LockedAt, now)

			t.Log("测试一下 GetStates")
			states, err := backend.GetStates(ctx, nil, 0, 0)
			if err != nil {
				t.Error(err)
				return
			}
			state = &states[0]

			if !cmp.Equal(state, excepted, opts...) {
				t.Error(cmp.Diff(state, excepted, opts...))
				return
			}

			assetTime(t, "RunAt", state.RunAt, runAt)
			assetTime(t, "Deadline", state.Deadline, job.Deadline)
			assetTime(t, "CreatedAt", state.CreatedAt, now)
			assetTime(t, "LastAt", state.LastAt, now)
			assetTime(t, "LockedAt", state.LockedAt, now)
		})

		t.Run("check state when retry with error", func(t *testing.T) {
			backend.ClearAll(ctx)
			id, e := backend.Enqueue(ctx, job)
			if nil != e {
				t.Error(e)
				return
			}

			runAt := time.Now().Add(-2 * time.Minute)
			e = backend.Retry(ctx, id, 2, runAt, &job.Payload, "errrr")
			if e != nil {
				t.Error(e)
				return
			}

			// 这些字段不应该存入表中的， 所以清空后比较一下， 以确保真的为空
			job.LockedBy = "abc"
			job.LastError = "errrr"
			job.Retried = 2
			job.FailedAt = time.Time{}

			state, err := backend.GetState(ctx, id)
			if err != nil {
				t.Error(err)
				return
			}

			opts := []cmp.Option{
				cmpopts.IgnoreFields(JobState{}, "ID", "LockedAt", "RunAt", "Deadline", "CreatedAt", "LastAt"),
				cmp.Comparer(func(a, b Payload) bool {
					return a.String() == b.String()
				}),
			}

			excepted := &JobState{
				ID:        id,
				Type:      job.Type,
				Payload:   job.Payload,
				UniqueKey: job.UUID,
				Timeout:   strconv.Itoa(job.Timeout) + "s",
				Queue:     job.Queue,
				Priority:  job.Priority,
				MaxRetry:  job.MaxRetry,
				Retried:   job.Retried,
				// LogMessages []string
				LastError: job.LastError,
				// RunBy:     "abc",
				Status: StatusFailAndRequeueing,
			}
			if !cmp.Equal(state, excepted, opts...) {
				t.Error(cmp.Diff(state, excepted, opts...))
				return
			}

			now := time.Now()
			assetTime(t, "RunAt", state.RunAt, runAt)
			assetTime(t, "Deadline", state.Deadline, job.Deadline)
			assetTime(t, "CreatedAt", state.CreatedAt, now)
			assetTime(t, "LastAt", state.LastAt, now)
			assetTime(t, "LockedAt", state.LockedAt, now)

			t.Log("测试一下 GetStates")
			states, err := backend.GetStates(ctx, nil, 0, 0)
			if err != nil {
				t.Error(err)
				return
			}
			state = &states[0]

			if !cmp.Equal(state, excepted, opts...) {
				t.Error(cmp.Diff(state, excepted, opts...))
				return
			}

			assetTime(t, "RunAt", state.RunAt, runAt)
			assetTime(t, "Deadline", state.Deadline, job.Deadline)
			assetTime(t, "CreatedAt", state.CreatedAt, now)
			assetTime(t, "LastAt", state.LastAt, now)
			assetTime(t, "LockedAt", state.LockedAt, now)
		})

		t.Run("check state when complete with ok", func(t *testing.T) {
			backend.ClearAll(ctx)
			id, e := backend.Enqueue(ctx, job)
			if nil != e {
				t.Error(e)
				return
			}

			e = backend.Destroy(ctx, id)
			if e != nil {
				t.Error(e)
				return
			}

			// 这些字段不应该存入表中的， 所以清空后比较一下， 以确保真的为空
			job.LockedBy = "abc"
			job.LastError = ""
			job.Retried = 0
			job.FailedAt = time.Time{}

			state, err := backend.GetState(ctx, id)
			if err != nil {
				t.Error(err)
				return
			}

			opts := []cmp.Option{
				cmpopts.IgnoreFields(JobState{}, "ID", "LockedAt", "RunAt", "Deadline", "CreatedAt", "LastAt", "CompletedAt"),
				cmp.Comparer(func(a, b Payload) bool {
					return a.String() == b.String()
				}),
			}

			excepted := &JobState{
				ID:        id,
				Type:      job.Type,
				Payload:   job.Payload,
				UniqueKey: job.UUID,
				Timeout:   "0s",
				Queue:     job.Queue,
				Priority:  job.Priority,
				MaxRetry:  0,
				Retried:   0,
				// LogMessages []string
				LastError: job.LastError,
				// RunBy:     "abc",
				Status: StatusOK,
			}
			if !cmp.Equal(state, excepted, opts...) {
				t.Error(cmp.Diff(state, excepted, opts...))
				return
			}

			now := time.Now()
			assetTime(t, "RunAt", state.RunAt, now)
			assetTime(t, "Deadline", state.Deadline, job.Deadline)
			assetTime(t, "CreatedAt", state.CreatedAt, now)
			assetTime(t, "LastAt", state.LastAt, now)
			assetTime(t, "LockedAt", state.LockedAt, now)

			t.Log("测试一下 GetStates")
			states, err := backend.GetStates(ctx, nil, 0, 0)
			if err != nil {
				t.Error(err)
				return
			}
			state = &states[0]

			if !cmp.Equal(state, excepted, opts...) {
				t.Error(cmp.Diff(state, excepted, opts...))
				return
			}

			assetTime(t, "RunAt", state.RunAt, now)
			assetTime(t, "Deadline", state.Deadline, job.Deadline)
			assetTime(t, "CreatedAt", state.CreatedAt, now)
			assetTime(t, "LastAt", state.LastAt, now)
			assetTime(t, "LockedAt", state.LockedAt, now)
			assetTime(t, "CompletedAt", state.CompletedAt, now)
		})

		t.Run("check state when complete with error", func(t *testing.T) {
			backend.ClearAll(ctx)
			id, e := backend.Enqueue(ctx, job)
			if nil != e {
				t.Error(e)
				return
			}

			e = backend.Fail(ctx, id, "errrr")
			if e != nil {
				t.Error(e)
				return
			}

			// 这些字段不应该存入表中的， 所以清空后比较一下， 以确保真的为空
			job.LockedBy = "abc"
			job.LastError = "errrr"
			job.Retried = 0
			job.FailedAt = time.Time{}

			state, err := backend.GetState(ctx, id)
			if err != nil {
				t.Error(err)
				return
			}

			opts := []cmp.Option{
				cmpopts.IgnoreFields(JobState{}, "ID", "LockedAt", "RunAt", "Deadline", "CreatedAt", "LastAt", "CompletedAt"),
				cmp.Comparer(func(a, b Payload) bool {
					return a.String() == b.String()
				}),
			}

			excepted := &JobState{
				ID:        id,
				Type:      job.Type,
				Payload:   job.Payload,
				UniqueKey: job.UUID,
				Timeout:   "0s",
				Queue:     job.Queue,
				Priority:  job.Priority,
				MaxRetry:  0,
				Retried:   job.Retried,
				// LogMessages []string
				LastError: job.LastError,
				// RunBy:     "abc",
				Status: StatusFail,
			}
			if !cmp.Equal(state, excepted, opts...) {
				t.Error(cmp.Diff(state, excepted, opts...))
				return
			}

			now := time.Now()
			assetTime(t, "RunAt", state.RunAt, now)
			assetTime(t, "Deadline", state.Deadline, job.Deadline)
			assetTime(t, "CreatedAt", state.CreatedAt, now)
			assetTime(t, "LastAt", state.LastAt, now)
			assetTime(t, "LockedAt", state.LockedAt, now)

			t.Log("测试一下 GetStates")
			states, err := backend.GetStates(ctx, nil, 0, 0)
			if err != nil {
				t.Error(err)
				return
			}
			state = &states[0]

			if !cmp.Equal(state, excepted, opts...) {
				t.Error(cmp.Diff(state, excepted, opts...))
				return
			}

			assetTime(t, "RunAt", state.RunAt, now)
			assetTime(t, "Deadline", state.Deadline, job.Deadline)
			assetTime(t, "CreatedAt", state.CreatedAt, now)
			assetTime(t, "LastAt", state.LastAt, now)
			assetTime(t, "LockedAt", state.LockedAt, now)
			assetTime(t, "CompletedAt", state.CompletedAt, now)
		})
	})
}

func assetTime(t *testing.T, field string, actual, excepted time.Time) {
	t.Helper()

	interval := actual.Sub(excepted)
	if interval < 0 {
		interval = -interval
	}

	if interval > time.Second {
		t.Error(field+": want ", excepted, "got", actual, "interval is", interval)
	}
}

func TestPriority(t *testing.T) {
	backendTest(t, nil, nil, func(ctx context.Context, opts *DbOptions, wopts *WorkOptions, backend Backend, conn *sql.DB) {
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

		_, e := backend.Enqueue(ctx, job)
		if nil != e {
			t.Error(e)
			return
		}
		for i := 1; i < 10; i++ {
			copyed := *job
			copyed.Priority += i
			copyed.UUID = copyed.UUID + strconv.Itoa(i)
			_, e := backend.Enqueue(ctx, &copyed)
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
	backendTest(t, nil, nil, func(ctx context.Context, opts *DbOptions, wopts *WorkOptions, backend Backend, conn *sql.DB) {
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

		_, e := backend.Enqueue(ctx, job)
		if e != nil {
			t.Error(e)
			return
		}

		_, e = conn.Exec("UPDATE " + opts.RunningTablename + " SET locked_at = now(), locked_by = 'aa'")
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
	backendTest(t, nil, nil, func(ctx context.Context, opts *DbOptions, wopts *WorkOptions, backend Backend, conn *sql.DB) {
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

		_, e := backend.Enqueue(ctx, job)
		if e != nil {
			t.Error(e)
			return
		}

		_, e = conn.Exec("UPDATE " + opts.RunningTablename + " SET locked_at = now() - interval '1h', locked_by = 'aa'")
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
	backendTest(t, nil, nil, func(ctx context.Context, opts *DbOptions, wopts *WorkOptions, backend Backend, conn *sql.DB) {
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

		id, e := backend.Enqueue(ctx, job)
		if e != nil {
			t.Error(e)
			return
		}

		e = backend.Fail(ctx, id, "aa")
		// _, e = conn.Exec("UPDATE " + opts.Tablename + " SET failed_at = now(), last_error = 'aa'")
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
