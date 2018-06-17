package executor

import (
	"context"
	"sync"
	"github.com/pkg/errors"
)

var (
	DefaultTaskErrorFunc = func(t Task, err error){}
	ErrNotImplemented = errors.New("not implemented")
)

type Task interface {
	Run(c context.Context) error
}

type Executor interface {
	Start() error
	Stop() error
	Stats() ExecutorStats
	Wait()
	Send(Task) error
}

type SpawnExecutor struct {
	queue chan Task
	quit  chan bool // quit chan
	wg    *sync.WaitGroup
	onTaskErrFn func(t Task, err error)
}

func (e *SpawnExecutor) reactor() {
	defer func() {
		e.wg.Done()
	}()
LOOP:
	for {
		select {
		case t, more := <-e.queue:
			if !more {
				// TODO: ideally should close
				//break LOOP
				e.Stop()
			}
			if t == nil {
				continue
			}
			// since spawning, cannot handle error here
			e.execute(t)

		case <-e.quit:
			break LOOP
		}
	}
}

func (e *SpawnExecutor) execute(t Task) error {
	e.wg.Add(1)
	defer func() {
		e.wg.Done()
	}()
	ctx := context.Background()
	err := t.Run(ctx)
	if err != nil {
		e.onTaskErrFn(t, err)
	}
	// since spawned goroutine, returning error not usable
	return nil
}

func (e *SpawnExecutor) Start() error {
	go e.reactor()
	e.wg.Add(1)
	return nil
}

func (e *SpawnExecutor) Stop() error {
	close(e.quit)
	e.Wait()
	return nil
}

func (e *SpawnExecutor) Stats() ExecutorStats {
	panic(ErrNotImplemented)
	return ExecutorStats{}
}

func (e *SpawnExecutor) Wait() {
	e.wg.Wait()
}

func (e *SpawnExecutor) Send(t Task) error {
	e.queue <- t
	return nil
}

func NewSpawnExecutorForQueue(q chan Task) *SpawnExecutor {
	e := &SpawnExecutor{
		queue: q,
		quit: make(chan bool),
		wg: new(sync.WaitGroup),
		onTaskErrFn: DefaultTaskErrorFunc,
	}
	return e
}

func NewSpawnExecutor() *SpawnExecutor {
	q := make(chan Task)
	e := NewSpawnExecutorForQueue(q)
	return e
}
