package tasks

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/runner-mei/errors"
	"github.com/runner-mei/goutils/tid"
)

var testCount = 0

type jobService struct {
	backend kinglink.Backend
  worker *kinglink.Worker
}

func (jobsrv *jobService) Create(ctx context.Context, typeName string, args map[string]interface{}, opts *Options) (string, error) {
	id, err := jobSrv.backend.Enqueue(ctx, &kinglink.Job{
		// RunAt     time.Time
		Deadline: opts.Deadline,
		Timeout: opts.Timeout,
		// Priority: opts.Deadline
		MaxRetry: opts.MaxRetry,
		Queue: opts.Queue,
		Type: typeName,
		Payload:  kinglink.MakePayload(nil, args),
		UUID: opts.Uuid,
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprint(id), nil
}

func (jobsrv *jobService) BatchCreate(ctx context.Context, requests []BatchRequest) ([]BatchResult, error) {
	results := make([]BatchResult, len(requests))
	for idx := range requests {
		id, err := jobsrv.Create(ctx, requests[idx].Type, requests[idx].Args, requests[idx].Options)
		if err != nil {
			results = append(results, BatchResult{
				Err: errors.ToApplicationError(err),
			})
			continue
		}

		results = append(results, BatchResult{
			ID: id,
		})
	}
	return results, nil
}

func (jobsrv *jobService) List(ctx context.Context) ([]TaskMessage, error) {
	jobsrv.lock.Lock()
	defer jobsrv.lock.Unlock()
	tasks := make([]TaskMessage, len(jobsrv.tasks))
	copy(tasks, jobsrv.tasks)
	return tasks, nil
}

func (jobsrv *jobService) Get(ctx context.Context, id string) (*TaskMessage, error) {
	jobsrv.lock.Lock()
	defer jobsrv.lock.Unlock()

	for _, task := range jobsrv.tasks {
		if task.ID == id {
			return &task, nil
		}
	}
	return nil, errors.NotFound(id)
}

func (jobsrv *jobService) Delete(ctx context.Context, id string) error {
	jobsrv.lock.Lock()
	defer jobsrv.lock.Unlock()

	for idx, task := range jobsrv.tasks {
		if task.ID == id {
			copy(jobsrv.tasks[idx:], jobsrv.tasks[idx+1:])
			jobsrv.tasks = jobsrv.tasks[:len(jobsrv.tasks)-1]
			return nil
		}
	}
	return errors.NotFound(id)
}

func NewJobService() *jobService {
	return &jobService{}
}
