package tests

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/runner-mei/errors"
	"github.com/runner-mei/kinglink"
	klclient "github.com/runner-mei/kinglink/services"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestCreateAndGet(t *testing.T) {
	ServerTest(t, nil, nil, nil, func(srv *TServer) {
		id, err := srv.RemoteClient.Create(srv.Ctx, "test", map[string]interface{}{"a": "b"}, &klclient.Options{})
		if err != nil {
			t.Error(err)
			return
		}

		t.Run("client.get", func(t *testing.T) {
			state, err := srv.RemoteClient.Get(srv.Ctx, id)
			if err != nil {
				t.Error(err)
				return
			}

			excepted := &kinglink.JobState{
				Type:        "test",
				Payload:     kinglink.MakePayload(nil, map[string]interface{}{"a": "b"}),
				ID:          id,
				Queue:       "",
				Priority:    0,
				MaxRetry:    0,
				Timeout:     "0s",
				UniqueKey:   "",
				Retried:     0,
				LogMessages: nil,
				LastError:   "",
				Status:      klclient.StatusQueueing,
				RunBy:       "",
			}

			opts := []cmp.Option{
				cmpopts.IgnoreFields(kinglink.JobState{}, "ID", "CreatedAt", "LastAt"),
				cmp.Comparer(func(a, b kinglink.Payload) bool {
					return a.String() == b.String()
				}),
			}
			if !cmp.Equal(state, excepted, opts...) {
				t.Error(cmp.Diff(state, excepted, opts...))
				return
			}

			if id != fmt.Sprint(state.ID) {
				t.Error("want ", id, "got", fmt.Sprint(state.ID))
				return
			}
		})

		t.Run("backend.fetch", func(t *testing.T) {
			job, err := srv.RemoteBackend.Fetch(srv.Ctx, "", nil)
			if err != nil {
				t.Error(err)
				return
			}

			exceptedJob := &kinglink.Job{
				Type:      "test",
				Payload:   kinglink.MakePayload(nil, map[string]interface{}{"a": "b"}),
				Queue:     "",
				Priority:  0,
				MaxRetry:  0,
				Timeout:   0,
				UUID:      "",
				Retried:   0,
				LastError: "",
			}

			opts := []cmp.Option{
				cmpopts.IgnoreFields(kinglink.Job{}, "ID", "LockedAt", "CreatedAt", "UpdatedAt"),
				cmp.Comparer(func(a, b kinglink.Payload) bool {
					return a.String() == b.String()
				}),
			}
			if !cmp.Equal(job, exceptedJob, opts...) {
				t.Error(cmp.Diff(job, exceptedJob, opts...))
				return
			}

			if id != fmt.Sprint(job.ID) {
				t.Error("want ", id, "got", fmt.Sprint(job.ID))
				return
			}

			now := time.Now()
			AssetTime(t, "CreatedAt", job.CreatedAt, now)
			AssetTime(t, "UpdatedAt", job.UpdatedAt, now)
			AssetTime(t, "LockedAt", job.LockedAt, now)

			state, err := srv.RemoteClient.Get(srv.Ctx, id)
			if err != nil {
				t.Error(err)
				return
			}

			if state.Status != klclient.StatusRunning {
				t.Error("want running got", state.Status.String())
				return
			}
		})
	})
}

func TestFetch(t *testing.T) {
	ServerTest(t, nil, nil, nil, func(srv *TServer) {
		id, err := srv.RemoteClient.Create(srv.Ctx, "test", map[string]interface{}{"a": "b"}, &klclient.Options{})
		if err != nil {
			t.Error(err)
			return
		}

		job, err := srv.RemoteBackend.Fetch(srv.Ctx, "mywork", nil)
		if err != nil {
			t.Error(err)
			return
		}

		exceptedJob := &kinglink.Job{
			Type:     "test",
			Payload:  kinglink.MakePayload(nil, map[string]interface{}{"a": "b"}),
			Queue:    "",
			Priority: 0,
			MaxRetry: 0,
			Timeout:  0,
			UUID:     "",
			Retried:  0,
			LockedBy: "mywork",
		}

		opts := []cmp.Option{
			cmpopts.IgnoreFields(kinglink.Job{}, "ID", "LockedAt", "CreatedAt", "UpdatedAt"),
			cmp.Comparer(func(a, b kinglink.Payload) bool {
				return a.String() == b.String()
			}),
		}
		if !cmp.Equal(job, exceptedJob, opts...) {
			t.Error(cmp.Diff(job, exceptedJob, opts...))
			return
		}

		if id != fmt.Sprint(job.ID) {
			t.Error("want ", id, "got", fmt.Sprint(job.ID))
			return
		}

		now := time.Now()
		AssetTime(t, "CreatedAt", job.CreatedAt, now)
		AssetTime(t, "UpdatedAt", job.UpdatedAt, now)
		AssetTime(t, "LockedAt", job.LockedAt, now)

		func() {
			state, err := srv.RemoteClient.Get(srv.Ctx, id)
			if err != nil {
				t.Error(err)
				return
			}
			if state.Status != klclient.StatusRunning {
				t.Error("want fail_and_requeueing got", state.Status.String())
				return
			}
		}()

		t.Run("client.get", func(t *testing.T) {
			state, err := srv.RemoteClient.Get(srv.Ctx, id)
			if err != nil {
				t.Error(err)
				return
			}

			excepted := &kinglink.JobState{
				Type:        "test",
				Payload:     kinglink.MakePayload(nil, map[string]interface{}{"a": "b"}),
				ID:          id,
				Queue:       "",
				Priority:    0,
				MaxRetry:    0,
				Timeout:     "0s",
				UniqueKey:   "",
				Retried:     0,
				LogMessages: nil,
				LastError:   "",
				Status:      klclient.StatusRunning,
				RunBy:       "mywork",
			}

			opts := []cmp.Option{
				cmpopts.IgnoreFields(kinglink.JobState{}, "ID", "CreatedAt", "LastAt", "RunAt", "LockedAt"),
				cmp.Comparer(func(a, b kinglink.Payload) bool {
					return a.String() == b.String()
				}),
			}
			if !cmp.Equal(state, excepted, opts...) {
				t.Error(cmp.Diff(state, excepted, opts...))
				return
			}
			AssetTime(t, "LockedAt", job.LockedAt, now)

			if id != fmt.Sprint(state.ID) {
				t.Error("want ", id, "got", fmt.Sprint(state.ID))
				return
			}
		})
	})
}

func TestRetry(t *testing.T) {
	ServerTest(t, nil, nil, nil, func(srv *TServer) {
		id, err := srv.RemoteClient.Create(srv.Ctx, "test", map[string]interface{}{"a": "b"}, &klclient.Options{})
		if err != nil {
			t.Error(err)
			return
		}

		payload := kinglink.MakePayload(nil, map[string]interface{}{"a": "c"})
		nextTime := time.Now().Add(-1 * time.Minute)
		err = srv.RemoteBackend.Retry(srv.Ctx, id, 2, nextTime, &payload, "abcerr")
		if err != nil {
			t.Error(err)
			return
		}

		func() {
			state, err := srv.RemoteClient.Get(srv.Ctx, id)
			if err != nil {
				t.Error(err)
				return
			}
			if state.Status != klclient.StatusFailAndRequeueing {
				t.Error("want fail_and_requeueing got", state.Status.String())
				return
			}
		}()

		t.Run("client.get", func(t *testing.T) {
			state, err := srv.RemoteClient.Get(srv.Ctx, id)
			if err != nil {
				t.Error(err)
				return
			}

			excepted := &kinglink.JobState{
				Type:        "test",
				Payload:     kinglink.MakePayload(nil, map[string]interface{}{"a": "c"}),
				ID:          id,
				Queue:       "",
				Priority:    0,
				MaxRetry:    0,
				Timeout:     "0s",
				UniqueKey:   "",
				Retried:     2,
				LogMessages: nil,
				LastError:   "abcerr",
				Status:      klclient.StatusFailAndRequeueing,
				RunBy:       "",
			}

			opts := []cmp.Option{
				cmpopts.IgnoreFields(kinglink.JobState{}, "ID", "CreatedAt", "LastAt", "RunAt"),
				cmp.Comparer(func(a, b kinglink.Payload) bool {
					return a.String() == b.String()
				}),
			}
			if !cmp.Equal(state, excepted, opts...) {
				t.Error(cmp.Diff(state, excepted, opts...))
				return
			}

			AssetTime(t, "nextTime", excepted.RunAt, nextTime)

			if id != fmt.Sprint(state.ID) {
				t.Error("want ", id, "got", fmt.Sprint(state.ID))
				return
			}
		})

		t.Run("backend.fetch", func(t *testing.T) {
			job, err := srv.RemoteBackend.Fetch(srv.Ctx, "", nil)
			if err != nil {
				t.Error(err)
				return
			}

			exceptedJob := &kinglink.Job{
				Type:      "test",
				Payload:   kinglink.MakePayload(nil, map[string]interface{}{"a": "c"}),
				Queue:     "",
				Priority:  0,
				MaxRetry:  0,
				Timeout:   0,
				UUID:      "",
				Retried:   2,
				LastError: "abcerr",
			}

			opts := []cmp.Option{
				cmpopts.IgnoreFields(kinglink.Job{}, "ID", "LockedAt", "CreatedAt", "UpdatedAt", "RunAt"),
				cmp.Comparer(func(a, b kinglink.Payload) bool {
					return a.String() == b.String()
				}),
			}
			if !cmp.Equal(job, exceptedJob, opts...) {
				t.Error(cmp.Diff(job, exceptedJob, opts...))
				return
			}

			if id != fmt.Sprint(job.ID) {
				t.Error("want ", id, "got", fmt.Sprint(job.ID))
				return
			}

			now := time.Now()
			AssetTime(t, "CreatedAt", job.CreatedAt, now)
			AssetTime(t, "UpdatedAt", job.UpdatedAt, now)
			AssetTime(t, "LockedAt", job.LockedAt, now)
			AssetTime(t, "RunAt", job.RunAt, nextTime)
		})
	})
}

func TestRunOK(t *testing.T) {
	ServerTest(t, nil, nil, nil, func(srv *TServer) {
		id, err := srv.RemoteClient.Create(srv.Ctx, "test", map[string]interface{}{"a": "b"}, &klclient.Options{})
		if err != nil {
			t.Error(err)
			return
		}

		nextTime := time.Now().Add(-1 * time.Minute)
		err = srv.RemoteBackend.Success(srv.Ctx, id)
		if err != nil {
			t.Error(err)
			return
		}

		t.Run("client.get", func(t *testing.T) {
			state, err := srv.RemoteClient.Get(srv.Ctx, id)
			if err != nil {
				t.Error(err)
				return
			}

			excepted := &kinglink.JobState{
				Type:        "test",
				Payload:     kinglink.MakePayload(nil, map[string]interface{}{"a": "b"}),
				ID:          id,
				Queue:       "",
				Priority:    0,
				MaxRetry:    0,
				Timeout:     "0s",
				UniqueKey:   "",
				Retried:     0,
				LogMessages: nil,
				LastError:   "",
				Status:      klclient.StatusOK,
				RunBy:       "",
			}

			opts := []cmp.Option{
				cmpopts.IgnoreFields(kinglink.JobState{}, "ID", "CreatedAt", "LastAt", "RunAt", "CompletedAt"),
				cmp.Comparer(func(a, b kinglink.Payload) bool {
					return a.String() == b.String()
				}),
			}
			if !cmp.Equal(state, excepted, opts...) {
				t.Error(cmp.Diff(state, excepted, opts...))
				return
			}

			AssetTime(t, "nextTime", excepted.RunAt, nextTime)
			AssetTime(t, "CompletedAt", excepted.CompletedAt, time.Now())

			if id != fmt.Sprint(state.ID) {
				t.Error("want ", id, "got", fmt.Sprint(state.ID))
				return
			}
		})

		t.Run("backend.fetch", func(t *testing.T) {
			job, err := srv.RemoteBackend.Fetch(srv.Ctx, "", nil)

			if err != nil && !errors.IsNoContent(err) {
				t.Error(err)
				return
			}

			if err == nil && job != nil {
				t.Error("want null got", fmt.Sprintf("%#v", job))
				return
			}
		})
	})
}

func TestRunFail(t *testing.T) {
	ServerTest(t, nil, nil, nil, func(srv *TServer) {
		id, err := srv.RemoteClient.Create(srv.Ctx, "test", map[string]interface{}{"a": "b"}, &klclient.Options{})
		if err != nil {
			t.Error(err)
			return
		}

		err = srv.RemoteBackend.Fail(srv.Ctx, id, "errmsg")
		if err != nil {
			t.Error(err)
			return
		}

		t.Run("client.get", func(t *testing.T) {
			state, err := srv.RemoteClient.Get(srv.Ctx, id)
			if err != nil {
				t.Error(err)
				return
			}

			excepted := &kinglink.JobState{
				Type:        "test",
				Payload:     kinglink.MakePayload(nil, map[string]interface{}{"a": "b"}),
				ID:          id,
				Queue:       "",
				Priority:    0,
				MaxRetry:    0,
				Timeout:     "0s",
				UniqueKey:   "",
				Retried:     0,
				LogMessages: nil,
				LastError:   "errmsg",
				Status:      klclient.StatusFail,
				RunBy:       "",
			}

			opts := []cmp.Option{
				cmpopts.IgnoreFields(kinglink.JobState{}, "ID", "CreatedAt", "LastAt", "RunAt", "CompletedAt"),
				cmp.Comparer(func(a, b kinglink.Payload) bool {
					return a.String() == b.String()
				}),
			}
			if !cmp.Equal(state, excepted, opts...) {
				t.Error(cmp.Diff(state, excepted, opts...))
				return
			}

			AssetTime(t, "CompletedAt", excepted.CompletedAt, time.Now())

			if id != fmt.Sprint(state.ID) {
				t.Error("want ", id, "got", fmt.Sprint(state.ID))
				return
			}
		})

		t.Run("backend.fetch", func(t *testing.T) {
			job, err := srv.RemoteBackend.Fetch(srv.Ctx, "", nil)

			if err != nil && !errors.IsNoContent(err) {
				t.Error(err)
				return
			}

			if err == nil && job != nil {
				t.Error("want null got", fmt.Sprintf("%#v", job))
				return
			}
		})
	})
}

func TestErrNoContent(t *testing.T) {
	t.Log("这个是因为 kinglink 没有引用 errors.ErrNoContent")
	if kinglink.ErrNoContent != errors.ErrNoContent.Error() {
		t.Error("want", kinglink.ErrNoContent, "got", errors.ErrNoContent.Error())
	}
}

func TestWorkerWithRunOK(t *testing.T) {
	mux := kinglink.NewServeMux()
	mux.Handle("test", kinglink.HandlerFunc(func(ctx context.Context, job *kinglink.Job) error {
		fields := job.Payload.MustFields()
		o := fields["a"]
		s := fmt.Sprint(o)
		if s != "b" {
			return errors.New("arguement a is error - " + s)
		}
		return nil
	}))

	ServerTest(t, nil, nil, nil, func(srv *TServer) {
		id, err := srv.RemoteClient.Create(srv.Ctx, "test", map[string]interface{}{"a": "b"}, &klclient.Options{})
		if err != nil {
			t.Error(err)
			return
		}

		srv.Wopts.NamePrefix = "mywork:"
		w, err := kinglink.NewWorker(srv.Wopts, mux, srv.RemoteBackend)
		if err != nil {
			t.Error(err)
			return
		}

		w.Run(srv.Ctx, true)

		t.Run("client.get", func(t *testing.T) {
			state, err := srv.RemoteClient.Get(srv.Ctx, id)
			if err != nil {
				t.Error(err)
				return
			}

			excepted := &kinglink.JobState{
				Type:        "test",
				Payload:     kinglink.MakePayload(nil, map[string]interface{}{"a": "b"}),
				ID:          id,
				Queue:       "",
				Priority:    0,
				MaxRetry:    0,
				Timeout:     "0s",
				UniqueKey:   "",
				Retried:     0,
				LogMessages: nil,
				LastError:   "",
				Status:      klclient.StatusOK,
				RunBy:       "",
			}

			opts := []cmp.Option{
				cmpopts.IgnoreFields(kinglink.JobState{}, "ID", "CreatedAt", "LastAt", "RunBy", "RunAt", "CompletedAt"),
				cmp.Comparer(func(a, b kinglink.Payload) bool {
					return a.String() == b.String()
				}),
			}
			if !cmp.Equal(state, excepted, opts...) {
				t.Error(cmp.Diff(state, excepted, opts...))
				return
			}

			AssetTime(t, "CompletedAt", excepted.CompletedAt, time.Now())

			if id != fmt.Sprint(state.ID) {
				t.Error("want ", id, "got", fmt.Sprint(state.ID))
				return
			}
			if !strings.HasPrefix(state.RunBy, "mywork:") {
				t.Error("want startwith 'mywork:' got", state.RunBy)
			}
		})

		t.Run("backend.fetch", func(t *testing.T) {
			job, err := srv.RemoteBackend.Fetch(srv.Ctx, "", nil)

			if err != nil && !errors.IsNoContent(err) {
				t.Error(err)
				return
			}

			if err == nil && job != nil {
				t.Error("want null got", fmt.Sprintf("%#v", job))
				return
			}
		})
	})
}

func TestWorkerWithRunFail(t *testing.T) {
	mux := kinglink.NewServeMux()
	mux.Handle("test", kinglink.HandlerFunc(func(ctx context.Context, job *kinglink.Job) error {
		return errors.New("myerror")
	}))

	ServerTest(t, nil, nil, nil, func(srv *TServer) {
		id, err := srv.RemoteClient.Create(srv.Ctx, "test", map[string]interface{}{"a": "b"}, &klclient.Options{})
		if err != nil {
			t.Error(err)
			return
		}

		srv.Wopts.NamePrefix = "mywork:"
		w, err := kinglink.NewWorker(srv.Wopts, mux, srv.RemoteBackend)
		if err != nil {
			t.Error(err)
			return
		}

		w.Run(srv.Ctx, true)

		t.Run("client.get", func(t *testing.T) {
			state, err := srv.RemoteClient.Get(srv.Ctx, id)
			if err != nil {
				t.Error(err)
				return
			}

			excepted := &kinglink.JobState{
				Type:        "test",
				Payload:     kinglink.MakePayload(nil, map[string]interface{}{"a": "b"}),
				ID:          id,
				Queue:       "",
				Priority:    0,
				MaxRetry:    0,
				Timeout:     "0s",
				UniqueKey:   "",
				Retried:     0,
				LogMessages: nil,
				LastError:   "myerror",
				Status:      klclient.StatusFail,
				RunBy:       "",
			}

			opts := []cmp.Option{
				cmpopts.IgnoreFields(kinglink.JobState{}, "ID", "CreatedAt", "LastAt", "RunBy", "RunAt", "CompletedAt"),
				cmp.Comparer(func(a, b kinglink.Payload) bool {
					return a.String() == b.String()
				}),
			}
			if !cmp.Equal(state, excepted, opts...) {
				t.Error(cmp.Diff(state, excepted, opts...))
				return
			}

			AssetTime(t, "CompletedAt", excepted.CompletedAt, time.Now())

			if id != fmt.Sprint(state.ID) {
				t.Error("want ", id, "got", fmt.Sprint(state.ID))
				return
			}
			if !strings.HasPrefix(state.RunBy, "mywork:") {
				t.Error("want startwith 'mywork:' got", state.RunBy)
			}
		})

		t.Run("backend.fetch", func(t *testing.T) {
			job, err := srv.RemoteBackend.Fetch(srv.Ctx, "", nil)

			if err != nil && !errors.IsNoContent(err) {
				t.Error(err)
				return
			}

			if err == nil && job != nil {
				t.Error("want null got", fmt.Sprintf("%#v", job))
				return
			}
		})
	})
}
