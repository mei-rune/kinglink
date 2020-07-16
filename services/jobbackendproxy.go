package services

import (
	"context"
	"time"

	"github.com/runner-mei/kinglink"
)

type jobBackendProxy struct {
	backend kinglink.WorkBackend
}

func (broker *jobBackendProxy) Fetch(ctx context.Context, name string, queues []string) (*kinglink.Job, error) {
	return broker.backend.Fetch(ctx, name, queues)
}

func (broker *jobBackendProxy) Retry(ctx context.Context, id interface{}, attempts int, nextTime time.Time, payload *kinglink.Payload, err string) error {
	return broker.backend.Retry(ctx, id, attempts, nextTime, payload, err)
}

func (broker *jobBackendProxy) Fail(ctx context.Context, id interface{}, error string) error {
	return broker.backend.Fail(ctx, id, error)
}

func (broker *jobBackendProxy) Success(ctx context.Context, id interface{}) error {
	return broker.backend.Success(ctx, id)
}
