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

type TaskStatus int

const (
	// StatusQueueing indicates the task is in queue state.
	StatusQueueing TaskStatus = iota

	// StatusRunning indicates the task is running.
	StatusRunning

	// StatusFailAndRequeueing indicates the task has been stopped.
	StatusFailAndRequeueing

	// StatusOK indicates the task has been run completed and stopped .
	StatusOK

	// StatusFail indicates the task has been fail.
	StatusFail

	// StatusScheduling indicates the task is in schedule state.
	StatusScheduling
)

var statuses = []string{
	"queueing",
	"running",
	"fail_and_requeueing",
	"ok",
	"fail",
	"scheduling",
}

func (s TaskStatus) String() string {
	if StatusQueueing <= s && s <= StatusScheduling {
		return statuses[s]
	}
	return "unknown status"
}

// JobState is the internal representation of a task with additional metadata fields.
// Serialized data of this type gets written to redis.
type JobState struct {
	// Type indicates the kind of the task to be performed.
	Type string

	// Payload holds data needed to process the task.
	Payload map[string]interface{}

	// UUID is a unique identifier for each task.
	UUID string

	// Queue is a name this message should be enqueued to.
	Queue string

	// Priority is priority of the task.
	Priority int

	// Retry is the max number of retry for this task.
	MaxRetry int

	// Timeout specifies how long a task may run.
	// The string value should be compatible with time.Duration.ParseDuration.
	//
	// Zero means no limit.
	Timeout string

	// Deadline specifies the deadline for the task.
	// Task won't be processed if it exceeded its deadline.
	// The string shoulbe be in RFC3339 format.
	//
	// time.Time's zero value means no deadline.
	Deadline time.Time

	// UniqueKey holds the redis key used for uniqueness lock for this task.
	//
	// Empty string indicates that no uniqueness lock was used.
	UniqueKey string

	// Retried is the number of times we've retried this task so far.
	Retried int

	// LogMessages is the log messages of the task run.
	LogMessages []string

	// LastError holds the error message from the last failure.
	LastError string

	// LastAt holds the timestamp from the last failure.
	LastAt time.Time

	// Status holds the status from the task.
	Status TaskStatus

	RunAt     time.Time
	RunBy     string
	LockedAt  time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
}

type JobResult struct {
	ID          int64
	Priority    int
	Retried     int
	Queue       string
	Type        string
	Payload     Payload
	UUID        string
	RunBy       string
	CompletedAt time.Time
	LastError   string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type Backend interface {
	io.Closer

	ClearAll(ctx context.Context) error

	Enqueue(ctx context.Context, job *Job) (interface{}, error)
	Fetch(ctx context.Context, name string, queues []string) (*Job, error)
	Retry(ctx context.Context, id interface{}, attempts int, nextTime time.Time, payload interface{}, err string) error
	Fail(ctx context.Context, id interface{}, e string) error
	Destroy(ctx context.Context, id interface{}) error

	DeleteResult(ctx context.Context, id interface{}) error
	ClearWithTimeout(ctx context.Context, minutes int) error
	GetResult(ctx context.Context, id interface{}) (*JobResult, error)

	GetState(ctx context.Context, id interface{}) (*JobState, error)
	GetStates(ctx context.Context, queues []string, offset, limit int) ([]JobState, error)
}

func Enqueue(ctx context.Context, backend Backend, typeName string, args map[string]interface{}, opts ...Option) (interface{}, error) {
	return backend.Enqueue(ctx, NewJob(typeName, args, opts...))
}
