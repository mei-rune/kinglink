package core

import (
	"context"
	"sync"
)

func NewThreads(n int) *Threads {
	sem := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		sem <- struct{}{}
	}
	return &Threads{
		sem: sem,
	}
}

type Threads struct {
	sem chan struct{}
	wg  sync.WaitGroup
}

func (s *Threads) Wait() {
	s.wg.Wait()
}

func (s *Threads) Acquire(ctx context.Context) error {
	select {
	case <-s.sem:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Threads) release() {
	s.sem <- struct{}{}
}

func (s *Threads) Go(run func()) {
	s.wg.Add(1)

	go func() {
		defer func() {
			s.wg.Done()
			s.release()
		}()

		run()
	}()
}
