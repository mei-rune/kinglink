package services

import (
	"context"
	"fmt"

	"github.com/runner-mei/errors"
	"github.com/runner-mei/kinglink"
)

var _ Client = &jobClientService{}

type jobClientService struct {
	backend kinglink.ServerBackend
}

func (jobsrv *jobClientService) Create(ctx context.Context, typeName string, args map[string]interface{}, opts *Options) (string, error) {
	id, err := jobsrv.backend.Enqueue(ctx, &kinglink.Job{
		// RunAt     time.Time
		Deadline: opts.Deadline,
		Timeout:  int(opts.Timeout.Seconds()),
		// Priority: opts.Priority,
		MaxRetry: opts.MaxRetry,
		Queue:    opts.Queue,
		Type:     typeName,
		Payload:  kinglink.MakePayload(nil, args),
		UUID:     opts.Uuid,
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprint(id), nil
}

func (jobsrv *jobClientService) BatchCreate(ctx context.Context, requests []BatchRequest) ([]BatchResult, error) {
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

func (jobsrv *jobClientService) List(ctx context.Context, queues []string, limit, offset int) ([]TaskMessage, error) {
	return jobsrv.backend.GetStates(ctx, queues, limit, offset)
}

func (jobsrv *jobClientService) Get(ctx context.Context, id string) (*TaskMessage, error) {
	return jobsrv.backend.GetState(ctx, id)
}

func (jobsrv *jobClientService) Delete(ctx context.Context, id string) error {
	return jobsrv.backend.Cancel(ctx, id)
}

func NewjobClientService() *jobClientService {
	return &jobClientService{}
}
