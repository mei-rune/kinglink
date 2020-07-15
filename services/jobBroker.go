package tasks

import (
  "context"
  "fmt"
  "sync"
  "time"

  "github.com/runner-mei/errors"
  "github.com/runner-mei/goutils/tid"
)

type jobBroker interface {
  backend kinglink.WorkBackend 
}

  func  (broker *jobBroker) Fetch(ctx context.Context, name string, queues []string) (*kinglink.Job, error) {
    return broker.backend.Fetch(ctx, name, queues)
  }

  func  (broker *jobBroker) Retry(ctx context.Context, id interface{}, attempts int, nextTime time.Time, payload kinglink.Payload, err string) error {
    return broker.backend.Retry(ctx, id, attempts, nextTime, payload, err)
  }
  func  (broker *jobBroker) Fail(ctx context.Context, id interface{}, e string) error {
    return broker.backend.Fail(ctx, id, e)
  }
  func  (broker *jobBroker) Destroy(ctx context.Context, id interface{}) error {
    return broker.backend.Destroy(ctx, id)
  }