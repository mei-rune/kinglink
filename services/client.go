//go:generate gogen client -ext=.client-gen.go client.go

package services

import (
	"context"
	"strings"
	"time"

	"github.com/runner-mei/errors"
	"github.com/runner-mei/kinglink"
)

type TaskStatus = kinglink.TaskStatus

const (
	// StatusQueueing indicates the task is in queue state.
	StatusQueueing = kinglink.StatusQueueing

	// StatusRunning indicates the task is running.
	StatusRunning = kinglink.StatusRunning

	// StatusFailAndRequeueing indicates the task has been stopped.
	StatusFailAndRequeueing = kinglink.StatusFailAndRequeueing

	// StatusOK indicates the task has been run completed and stopped .
	StatusOK = kinglink.StatusOK

	// StatusFail indicates the task has been fail.
	StatusFail = kinglink.StatusFail

	// StatusScheduling indicates the task is in schedule state.
	StatusScheduling = kinglink.StatusScheduling
)

type Options struct {
	MaxRetry int           `json:"max_retry,omitempty"`
	Queue    string        `json:"queue,omitempty"`
	Timeout  time.Duration `json:"timeout,omitempty"`
	Deadline time.Time     `json:"deadline,omitempty"`
	Uuid     string        `json:"uuid,omitempty"`
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

// ErrDuplicateTask indicates that the given task could not be enqueued since it's a duplicate of another task.
//
// ErrDuplicateTask error only applies to tasks enqueued with a Unique option.
var ErrDuplicateTask = errors.New("task already exists")

type DbOptions = kinglink.DbOptions
type WorkOptions = kinglink.WorkOptions
type Payload = kinglink.Payload
type TaskMessage = kinglink.JobState

var MakePayload = kinglink.MakePayload

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
}
