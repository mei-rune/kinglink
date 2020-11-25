// Please don't edit this file!
package kinglink

import (
	"context"
	"strconv"

	"github.com/runner-mei/resty"
)

// Options is skipped
// Option is skipped
// BatchResult is skipped
// BatchRequest is skipped

type ClientClient struct {
	Proxy *resty.Proxy
}

func (client ClientClient) Create(ctx context.Context, typeName string, args map[string]interface{}, options *Options) (string, error) {
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

func (client ClientClient) BatchCreate(ctx context.Context, requests []BatchRequest) ([]BatchResult, error) {
	var result []BatchResult

	request := resty.NewRequest(client.Proxy, "/batch").
		SetBody(requests).
		Result(&result)

	err := request.POST(ctx)
	resty.ReleaseRequest(client.Proxy, request)
	return result, err
}

func (client ClientClient) List(ctx context.Context, queues []string, limit int, offset int) ([]TaskMessage, error) {
	var result []TaskMessage

	request := resty.NewRequest(client.Proxy, "/")
	for idx := range queues {
		request = request.AddParam("queues", queues[idx])
	}
	request = request.SetParam("limit", strconv.FormatInt(int64(limit), 10)).
		SetParam("offset", strconv.FormatInt(int64(offset), 10)).
		Result(&result)

	err := request.GET(ctx)
	resty.ReleaseRequest(client.Proxy, request)
	return result, err
}

func (client ClientClient) Get(ctx context.Context, id string) (*TaskMessage, error) {
	var result TaskMessage

	request := resty.NewRequest(client.Proxy, "/"+id+"").
		Result(&result)

	err := request.GET(ctx)
	resty.ReleaseRequest(client.Proxy, request)
	return &result, err
}

func (client ClientClient) Delete(ctx context.Context, id string) error {
	request := resty.NewRequest(client.Proxy, "/"+id+"")

	defer resty.ReleaseRequest(client.Proxy, request)
	return request.DELETE(ctx)
}

func (client ClientClient) DeleteList(ctx context.Context, idList []string) error {
	request := resty.NewRequest(client.Proxy, "/").
		SetBody(idList)

	defer resty.ReleaseRequest(client.Proxy, request)
	return request.DELETE(ctx)
}
