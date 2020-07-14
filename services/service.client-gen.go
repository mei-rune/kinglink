// Please don't edit this file!
package tasks

import (
	"context"

	"github.com/runner-mei/resty"
)

// BatchResult is skipped
// BatchRequest is skipped

type ClientServiceClient struct {
	Proxy *resty.Proxy
}

func (client ClientServiceClient) Create(ctx context.Context, typeName string, args map[string]interface{}, options *Options) (string, error) {
	var result string

	request := resty.NewRequest(client.Proxy, "/").
		SetBody(map[string]interface{}{
			"type_name": typeName,
			"args":      args,
			"options":   options,
		}).
		Result(&result)

	err := request.POST(ctx)
	resty.ReleaseRequest(client.Proxy, request)
	return result, err
}

func (client ClientServiceClient) BatchCreate(ctx context.Context, requests []BatchRequest) ([]BatchResult, error) {
	var result []BatchResult

	request := resty.NewRequest(client.Proxy, "/batch").
		SetBody(requests).
		Result(&result)

	err := request.POST(ctx)
	resty.ReleaseRequest(client.Proxy, request)
	return result, err
}

func (client ClientServiceClient) List(ctx context.Context) ([]TaskMessage, error) {
	var result []TaskMessage

	request := resty.NewRequest(client.Proxy, "/").
		Result(&result)

	err := request.GET(ctx)
	resty.ReleaseRequest(client.Proxy, request)
	return result, err
}

func (client ClientServiceClient) Get(ctx context.Context, id string) (*TaskMessage, error) {
	var result TaskMessage

	request := resty.NewRequest(client.Proxy, "/"+id+"").
		Result(&result)

	err := request.GET(ctx)
	resty.ReleaseRequest(client.Proxy, request)
	return &result, err
}

func (client ClientServiceClient) Delete(ctx context.Context, id string) error {
	request := resty.NewRequest(client.Proxy, "/"+id+"")

	defer resty.ReleaseRequest(client.Proxy, request)
	return request.DELETE(ctx)
}
