package kinglink

import (
	"context"
	"io"
	"strings"
	"time"
)

type Option interface {
	apply(*Job)
}

type OptionFunc func(*Job)

func (f OptionFunc) apply(opt *Job) {
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
	return OptionFunc(func(opt *Job) {
		opt.MaxRetry = n
	})
}

// Queue returns an option to specify the queue to enqueue the task into.
//
// Queue name is case-insensitive and the lowercased version is used.
func Queue(name string) Option {
	return OptionFunc(func(opt *Job) {
		opt.Queue = strings.ToLower(name)
	})
}

// Timeout returns an option to specify how long a task may run.
//
// Zero duration means no limit.
func Timeout(d int) Option {
	return OptionFunc(func(opt *Job) {
		opt.Timeout = d
	})
}

// Deadline returns an option to specify the deadline for the given task.
func Deadline(t time.Time) Option {
	return OptionFunc(func(opt *Job) {
		opt.Deadline = t
	})
}

// UUID returns an option to specify the id for the given task.
func UUID(id string) Option {
	return OptionFunc(func(opt *Job) {
		opt.UUID = id
	})
}

func NewJob(typeName string, args map[string]interface{}, options ...Option) *Job {
	job := &Job{
		Type:    typeName,
		Payload: MakePayload(nil, args),
	}

	for _, opt := range options {
		opt.apply(job)
	}
	return job
}

// A Handler processes jobs.
//
// Run should return nil if the processing of a job
// is successful.
//
// If Run return a non-nil error or panics, the job
// will be retried after delay.
type Handler interface {
	Run(context.Context, *Job) error
}

// The HandlerFunc type is an adapter to allow the use of
// ordinary functions as a Handler. If f is a function
// with the appropriate signature, HandlerFunc(f) is a
// Handler that calls f.
type HandlerFunc func(context.Context, *Job) error

// Run calls fn(ctx, job)
func (fn HandlerFunc) Run(ctx context.Context, job *Job) error {
	return fn(ctx, job)
}

type Backend interface {
	io.Closer

	Enqueue(ctx context.Context, job *Job) error
	Fetch(ctx context.Context, name string, queues []string) (*Job, error)
	Retry(ctx context.Context, id interface{}, attempts int, nextTime time.Time, payload interface{}, err string) error
	Fail(ctx context.Context, id interface{}, e string) error
	Destroy(ctx context.Context, id interface{}) error
}

func Enqueue(ctx context.Context, backend Backend, typeName string, args map[string]interface{}, opts ...Option) error {
	return backend.Enqueue(ctx, NewJob(typeName, args, opts...))
}
