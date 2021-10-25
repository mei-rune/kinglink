package kinglink

import (
	"context"
	"time"

	"github.com/runner-mei/kinglink/core"
)

type jobBackendProxy struct {
	backend core.WorkBackend
}


func (broker *jobBackendProxy) ClearLocks(ctx context.Context, queues []string) error {
	return broker.backend.ClearLocks(ctx, queues)
}

func (broker *jobBackendProxy) Fetch(ctx context.Context, name string, queues []string) (*core.Job, error) {
	return broker.backend.Fetch(ctx, name, queues)
}

func (broker *jobBackendProxy) Retry(ctx context.Context, id interface{}, attempts int, nextTime time.Time, payload *core.Payload, err string) error {
	return broker.backend.Retry(ctx, id, attempts, nextTime, payload, err)
}

func (broker *jobBackendProxy) Fail(ctx context.Context, id interface{}, error string) error {
	return broker.backend.Fail(ctx, id, error)
}

func (broker *jobBackendProxy) Success(ctx context.Context, id interface{}) error {
	return broker.backend.Success(ctx, id)
}
