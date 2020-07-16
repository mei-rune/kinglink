package kinglink

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
)

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

// ServeMux is a multiplexer for asynchronous jobs.
// It matches the type of each job against a list of registered patterns
// and calls the handler for the pattern that most closely matches the
// job's type name.
//
// Longer patterns take precedence over shorter ones, so that if there are
// handlers registered for both "images" and "images:thumbnails",
// the latter handler will be called for jobs with a type name beginning with
// "images:thumbnails" and the former will receive jobs with type name beginning
// with "images".
type ServeMux struct {
	mu  sync.RWMutex
	m   map[string]muxEntry
	es  []muxEntry // slice of entries sorted from longest to shortest.
	mws []MiddlewareFunc
}

type muxEntry struct {
	h       Handler
	pattern string
}

// MiddlewareFunc is a function which receives an asynq.Handler and returns another asynq.Handler.
// Typically, the returned handler is a closure which does something with the context and job passed
// to it, and then calls the handler passed as parameter to the MiddlewareFunc.
type MiddlewareFunc func(Handler) Handler

// NewServeMux allocates and returns a new ServeMux.
func NewServeMux() *ServeMux {
	return new(ServeMux)
}

// RunJob dispatches the job to the handler whose
// pattern most closely matches the job type.
func (mux *ServeMux) RunJob(ctx context.Context, job *Job) error {
	h, _ := mux.Handler(job)
	return h.Run(ctx, job)
}

// Handler returns the handler to use for the given job.
// It always return a non-nil handler.
//
// Handler also returns the registered pattern that matches the job.
//
// If there is no registered handler that applies to the job,
// handler returns a 'not found' handler which returns an error.
func (mux *ServeMux) Handler(t *Job) (h Handler, pattern string) {
	mux.mu.RLock()
	defer mux.mu.RUnlock()

	h, pattern = mux.match(t.Type)
	if h == nil {
		h, pattern = NotFoundHandler(), ""
	}
	for i := len(mux.mws) - 1; i >= 0; i-- {
		h = mux.mws[i](h)
	}
	return h, pattern
}

// Find a handler on a handler map given a typename string.
// Most-specific (longest) pattern wins.
func (mux *ServeMux) match(typename string) (h Handler, pattern string) {
	// Check for exact match first.
	v, ok := mux.m[typename]
	if ok {
		return v.h, v.pattern
	}

	// Check for longest valid match.
	// mux.es contains all patterns from longest to shortest.
	for _, e := range mux.es {
		if strings.HasPrefix(typename, e.pattern) {
			return e.h, e.pattern
		}
	}
	return nil, ""
}

// Handle registers the handler for the given pattern.
// If a handler already exists for pattern, Handle panics.
func (mux *ServeMux) Handle(pattern string, handler Handler) {
	mux.mu.Lock()
	defer mux.mu.Unlock()

	if pattern == "" {
		panic("asynq: invalid pattern")
	}
	if handler == nil {
		panic("asynq: nil handler")
	}
	if _, exist := mux.m[pattern]; exist {
		panic("asynq: multiple registrations for " + pattern)
	}

	if mux.m == nil {
		mux.m = make(map[string]muxEntry)
	}
	e := muxEntry{h: handler, pattern: pattern}
	mux.m[pattern] = e
	mux.es = appendSorted(mux.es, e)
}

func appendSorted(es []muxEntry, e muxEntry) []muxEntry {
	n := len(es)
	i := sort.Search(n, func(i int) bool {
		return len(es[i].pattern) < len(e.pattern)
	})
	if i == n {
		return append(es, e)
	}
	// we now know that i points at where we want to insert.
	es = append(es, muxEntry{}) // try to grow the slice in place, any entry works.
	copy(es[i+1:], es[i:])      // shift shorter entries down.
	es[i] = e
	return es
}

// HandleFunc registers the handler function for the given pattern.
func (mux *ServeMux) HandleFunc(pattern string, handler func(context.Context, *Job) error) {
	if handler == nil {
		panic("asynq: nil handler")
	}
	mux.Handle(pattern, HandlerFunc(handler))
}

// Use appends a MiddlewareFunc to the chain.
// Middlewares are executed in the order that they are applied to the ServeMux.
func (mux *ServeMux) Use(mws ...MiddlewareFunc) {
	mux.mu.Lock()
	defer mux.mu.Unlock()
	for _, fn := range mws {
		mux.mws = append(mux.mws, fn)
	}
}

// NotFound returns an error indicating that the handler was not found for the given job.
func NotFound(ctx context.Context, job *Job) error {
	return fmt.Errorf("handler not found for job %q", job.Type)
}

// NotFoundHandler returns a simple job handler that returns a ``not found`` error.
func NotFoundHandler() Handler { return HandlerFunc(NotFound) }
