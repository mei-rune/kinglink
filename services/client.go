package tasks

import (
	"errors"
	"strings"
	"time"
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

type TaskMessage = kinglink.JobState

type Client interface {
	Create(typeName string, args map[string]interface{}, options ...Option) (string, error)
	List() ([]TaskMessage, error)
	Get(id string) (*TaskMessage, error)
	Delete(id string) error
}
