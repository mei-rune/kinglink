//go:generate gogen client -ext=.client-gen.go kinglink.go

package kinglink

import (
	"context"
	"strings"
	"time"

	"github.com/runner-mei/errors"
	"github.com/runner-mei/kinglink/core"
	_ "github.com/runner-mei/kinglink/plugins"
	"github.com/runner-mei/resty"
)

type DbOptions = core.DbOptions
type WorkOptions = core.WorkOptions
type Payload = core.Payload
type TaskMessage = core.JobState
type Backend = core.Backend
type WorkBackend = core.WorkBackend
type Worker = core.Worker
type ServeMux = core.ServeMux
type TaskStatus = core.TaskStatus
type JobState = core.JobState
type Job = core.Job
type Context = core.Context
type HandlerFunc = core.HandlerFunc
type Handler = core.Handler

const (
	// StatusQueueing indicates the task is in queue state.
	StatusQueueing = core.StatusQueueing

	// StatusRunning indicates the task is running.
	StatusRunning = core.StatusRunning

	// StatusFailAndRequeueing indicates the task has been stopped.
	StatusFailAndRequeueing = core.StatusFailAndRequeueing

	// StatusOK indicates the task has been run completed and stopped .
	StatusOK = core.StatusOK

	// StatusFail indicates the task has been fail.
	StatusFail = core.StatusFail

	// StatusScheduling indicates the task is in schedule state.
	StatusScheduling = core.StatusScheduling
)

// ErrDuplicateTask indicates that the given task could not be enqueued since it's a duplicate of another task.
//
// ErrDuplicateTask error only applies to tasks enqueued with a Unique option.
var ErrDuplicateTask = errors.New("task already exists")

var DefaultServeMux = core.DefaultServeMux

var ErrNoContent = core.ErrNoContent

type Options struct {
	MaxRetry int           `json:"max_retry,omitempty"`
	Queue    string        `json:"queue,omitempty"`
	Timeout  time.Duration `json:"timeout,omitempty"`
	Deadline time.Time     `json:"deadline,omitempty"`
	Uuid     string        `json:"uuid,omitempty"`
}

func DefaultOptions() *Options {
	return &Options{}
}

type Option interface {
	apply(*Options)
}

type optionFunc func(*Options)

func (f optionFunc) apply(opt *Options) {
	f(opt)
}

// MaxRetry returns an option to specify the max number of times
// the task will be retried.
//
// Negative retry count is treated as zero retry.
func MaxRetry(n int) Option {
	if n < 0 {
		n = 0
	}
	return optionFunc(func(opt *Options) {
		opt.MaxRetry = n
	})
}

// Queue returns an option to specify the queue to enqueue the task into.
//
// Queue name is case-insensitive and the lowercased version is used.
func Queue(name string) Option {
	return optionFunc(func(opt *Options) {
		opt.Queue = strings.ToLower(name)
	})
}

// Timeout returns an option to specify how long a task may run.
//
// Zero duration means no limit.
func Timeout(d time.Duration) Option {
	return optionFunc(func(opt *Options) {
		opt.Timeout = d
	})
}

// Deadline returns an option to specify the deadline for the given task.
func Deadline(t time.Time) Option {
	return optionFunc(func(opt *Options) {
		opt.Deadline = t
	})
}

// Uuid returns an option to specify the id for the given task.
func Uuid(id string) Option {
	return optionFunc(func(opt *Options) {
		opt.Uuid = id
	})
}

// Uuid returns an option to specify the id for the given task.
func OptionSet(opts Options) Option {
	return optionFunc(func(opt *Options) {
		*opt = opts
	})
}

type BatchResult struct {
	ID  string                   `json:"id,omitempty"`
	Err *errors.ApplicationError `json:"error,omitempty"`
}

type BatchRequest struct {
	Type    string                 `json:"type"`
	Args    map[string]interface{} `json:"args,omitempty"`
	Options *Options               `json:"options,omitempty"`
}

type Client interface {
	// @http.POST(path="?typeName=type")
	Create(ctx context.Context, typeName string, args map[string]interface{}, options *Options) (string, error)

	// @http.POST(path="/batch", data="requests")
	BatchCreate(ctx context.Context, requests []BatchRequest) ([]BatchResult, error)

	// @http.GET(path="")
	List(ctx context.Context, queues []string, limit, offset int) ([]TaskMessage, error)

	// @http.GET(path="/:id")
	Get(ctx context.Context, id string) (*TaskMessage, error)

	// @http.DELETE(path="/:id")
	Delete(ctx context.Context, id string) error

	// @http.DELETE(path="", data="idList")
	DeleteList(ctx context.Context, idList []string) error
}

func NewRemoteClient(urlstr string) (Client, error) {
	prx, err := resty.New(urlstr)
	if err != nil {
		return nil, errors.Wrap(err, "NewRemoteClient")
	}
	return &ClientClient{
		Proxy: prx,
	}, nil
}

func NewLocalBackend(dbopts *DbOptions, wopts *WorkOptions) (Backend, error) {
	return core.NewBackend(dbopts, wopts)
}

func NewRemoteWorkBackend(urlstr string) (WorkBackend, error) {
	prx, err := resty.New(urlstr)
	if err != nil {
		return nil, errors.Wrap(err, "NewRemoteBackend")
	}
	return &core.WorkBackendClient{
		Proxy: prx,
	}, nil
}

func NewRemoteClientWithProxy(proxy *resty.Proxy) Client {
	return &ClientClient{
		Proxy: proxy,
	}
}

func NewRemoteWorkBackendWithProxy(proxy *resty.Proxy) WorkBackend {
	return &core.WorkBackendClient{
		Proxy: proxy,
	}
}

func NewLocalWorkBackend(dbopts *DbOptions, wopts *WorkOptions) (WorkBackend, error) {
	return NewLocalBackend(dbopts, wopts)
}

func NewWorker(options *WorkOptions, mux *ServeMux, backend WorkBackend) (*Worker, error) {
	return core.NewWorker(options, mux, backend)
}

func MakePayload(bs []byte, values map[string]interface{}) Payload {
	return core.MakePayload(bs, values)
}

func NewServeMux() *ServeMux {
	return core.NewServeMux()
}
