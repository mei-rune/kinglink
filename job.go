package kinglink

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"
)

var ErrTimeout = errors.New("timeout")

var (
	sequenceID = int32(0)
)

func generateID() string {
	return strconv.FormatInt(time.Now().Unix(), 10) + "_" + strconv.FormatInt(int64(atomic.AddInt32(&sequenceID, 1)), 10)
}

// TaskMessage is the internal representation of a task with additional metadata fields.
// Serialized data of this type gets written to redis.
type TaskMessage struct {
	LogMessages []string
}

type Job struct {
	ID        int64
	RunAt     time.Time
	Deadline  time.Time
	Timeout   int
	Priority  int
	Retried   int
	MaxRetry  int
	Queue     string
	Type      string
	Payload   Payload
	UUID      string
	FailedAt  time.Time
	LastError string
	LockedAt  time.Time
	LockedBy  string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (job *Job) isFailed() bool {
	return !job.FailedAt.IsZero()
}

func (job *Job) name() string {
	fields, e := job.Payload.Fields()
	if nil == e && nil != fields {
		if v, ok := fields["display_name"]; ok {
			return fmt.Sprint(v)
		}
		if v, ok := fields["name"]; ok {
			return fmt.Sprint(v)
		}
	}
	return "**empty**"
}

func (job *Job) createCtx(ctx context.Context) context.Context {
	return ctx
}

func (job *Job) rescheduleAt() time.Time {
	duration := time.Duration(job.Retried*10) * time.Second
	return time.Now().Add(duration).Add(5 * time.Second)
}

func (job *Job) getMaxRetry(jobMaxRetry int) int {
	if job.MaxRetry > 0 {
		return job.MaxRetry
	}
	return jobMaxRetry
}

func (job *Job) execTimeout(defaultTimeout time.Duration) time.Duration {
	if job.Timeout <= 0 {
		return defaultTimeout
	}
	return time.Duration(job.Timeout) * time.Second
}
