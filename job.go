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
	return time.Now().Add(duration).Add(5 + time.Second)
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

// func (job *Job) will_update_attributes() map[string]interface{} {
// 	if nil == job.changed_attributes {
// 		job.changed_attributes = make(map[string]interface{}, 8)
// 	}

// 	if update, ok := job.handler_object.(Updater); ok {
// 		if nil != job.handler_attributes {
// 			update.UpdatePayloadObject(job.handler_attributes)
// 			job.changed_attributes["@handler"] = job.handler_attributes
// 		}
// 	}

// 	return job.changed_attributes
// }

// func (job *Job) rescheduleIt(next_time time.Time, err string) error {
// 	if len(err) > 2000 {
// 		err = err[:1900] + "\r\n===========================\r\n**error message is overflow."
// 	}

// 	job.attempts += 1
// 	job.run_at = next_time
// 	job.locked_at = time.Time{}
// 	job.locked_by = ""
// 	job.last_error = err

// 	changed := job.will_update_attributes()
// 	e := stringifiedHander(changed)
// 	if nil != e {
// 		return e
// 	}
// 	changed["@attempts"] = job.attempts
// 	changed["@run_at"] = next_time
// 	changed["@locked_at"] = nil
// 	changed["@locked_by"] = nil
// 	changed["@last_error"] = err

// 	e = job.backend.update(job.id, changed)
// 	job.changed_attributes = nil
// 	return e
// }
