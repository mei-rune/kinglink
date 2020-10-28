package core

import (
	"context"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

type Threads interface {
	Acquire(ctx context.Context) error
	Go(run func() error)
}

func NewErrGroupThreads(ctx context.Context, n int) (*ErrGroupThreads, context.Context) {
	sem := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		sem <- struct{}{}
	}
	th := &ErrGroupThreads{
		sem: sem,
	}
	th.wg, ctx = errgroup.WithContext(ctx)
	return th, ctx
}

type ErrGroupThreads struct {
	sem chan struct{}
	wg  *errgroup.Group
}

func (s *ErrGroupThreads) Wait() error {
	return s.wg.Wait()
}

func (s *ErrGroupThreads) Acquire(ctx context.Context) error {
	select {
	case <-s.sem:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *ErrGroupThreads) release() {
	s.sem <- struct{}{}
}

func (s *ErrGroupThreads) Go(run func() error) {
	s.wg.Go(func() error {
		defer s.release()
		return run()
	})
}

type WaitGroupThreads struct {
	sem         chan struct{}
	wg          sync.WaitGroup
	handleError func(err error)
}

func (s *WaitGroupThreads) Init(n int, handleError func(err error)) {
	s.sem = make(chan struct{}, n)
	for i := 0; i < n; i++ {
		s.sem <- struct{}{}
	}
	s.handleError = handleError
}

func (s *WaitGroupThreads) Wait() {
	s.wg.Wait()
}

func (s *WaitGroupThreads) Acquire(ctx context.Context) error {
	select {
	case <-s.sem:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *WaitGroupThreads) release() {
	s.sem <- struct{}{}
}

func (s *WaitGroupThreads) Go(run func() error) {
	s.wg.Add(1)

	go func() {
		defer func() {
			s.wg.Done()
			s.release()
		}()
		err := run()
		if err != nil {
			s.handleError(err)
		}
	}()
}

type stats struct {
	success int32
	failure int32
}

func (s *stats) addSuccess() {
	atomic.AddInt32(&s.success, 1)
}

func (s *stats) addFailure() {
	atomic.AddInt32(&s.failure, 1)
}

func (s *stats) Success() int {
	return int(atomic.LoadInt32(&s.success))
}

func (s *stats) Failure() int {
	return int(atomic.LoadInt32(&s.failure))
}
