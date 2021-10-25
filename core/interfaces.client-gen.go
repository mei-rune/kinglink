// Please don't edit this file!
package core

import (
	"context"
	"fmt"
	"time"

	"github.com/runner-mei/resty"
)

// Option is skipped
// JobState is skipped
// JobResult is skipped

type WorkBackendClient struct {
	Proxy *resty.Proxy
}

func (client WorkBackendClient) ClearLocks(ctx context.Context, queues []string) error {
	request := resty.NewRequest(client.Proxy, "/clear_locks").
		SetBody(map[string]interface{}{
			"queues": queues,
		})

	defer resty.ReleaseRequest(client.Proxy, request)
	return request.PUT(ctx)
}

func (client WorkBackendClient) Fetch(ctx context.Context, name string, queues []string) (*Job, error) {
	var result Job

	request := resty.NewRequest(client.Proxy, "/").
		SetParam("name", name)
	for idx := range queues {
		request = request.AddParam("queues", queues[idx])
	}
	request = request.Result(&result)

	err := request.GET(ctx)
	resty.ReleaseRequest(client.Proxy, request)
	return &result, err
}

func (client WorkBackendClient) Retry(ctx context.Context, id interface{}, attempts int, nextTime time.Time, payload *Payload, errMessage string) error {
	request := resty.NewRequest(client.Proxy, "/"+fmt.Sprint(id)+"/retry").
		SetBody(map[string]interface{}{
			"attempts":    attempts,
			"next_time":   nextTime,
			"payload":     payload,
			"err_message": errMessage,
		})

	defer resty.ReleaseRequest(client.Proxy, request)
	return request.PUT(ctx)
}

func (client WorkBackendClient) Fail(ctx context.Context, id interface{}, errMessage string) error {
	request := resty.NewRequest(client.Proxy, "/"+fmt.Sprint(id)+"/fail").
		SetBody(map[string]interface{}{
			"err_message": errMessage,
		})

	defer resty.ReleaseRequest(client.Proxy, request)
	return request.PUT(ctx)
}

func (client WorkBackendClient) Success(ctx context.Context, id interface{}) error {
	request := resty.NewRequest(client.Proxy, "/"+fmt.Sprint(id)+"/success")

	defer resty.ReleaseRequest(client.Proxy, request)
	return request.PUT(ctx)
}

// ServerBackend is skipped
// Backend is skipped
