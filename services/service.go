//go:generate gogen server -pre_init_object=true -ext=.server-gen.go -config=@loong service.go
//go:generate gogen client -ext=.client-gen.go service.go

package tasks

import (
	"cn/com/hengwei/pkg/ds_client"
	"cn/com/hengwei/pkg/environment"
	"context"

	"github.com/runner-mei/errors"
)

type BatchResult struct {
	ID  string                   `json:"id,omitempty"`
	Err *errors.ApplicationError `json:"error,omitempty"`
}

type BatchRequest struct {
	Type    string                 `json:"type"`
	Args    map[string]interface{} `json:"args,omitempty"`
	Options *Options               `json:"options,omitempty"`
}

type ClientService interface {
	// @http.POST(path="?typeName=type")
	Create(ctx context.Context, typeName string, args map[string]interface{}, options *Options) (string, error)

	// @http.POST(path="/batch", data="requests")
	BatchCreate(ctx context.Context, requests []BatchRequest) ([]BatchResult, error)

	// @http.GET(path="")
	List(ctx context.Context) ([]TaskMessage, error)

	// @http.GET(path="/:id")
	Get(ctx context.Context, id string) (*TaskMessage, error)

	// @http.DELETE(path="/:id")
	Delete(ctx context.Context, id string) error
}

func NewRemoteClient(env *environment.Environment) ClientService {
	return &ClientServiceClient{
		Proxy: ds_client.NewProxy(env, "api/tasks", ds_client.ProxyDisableWrap(true)),
	}
}
