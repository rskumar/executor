package executor

import (
	"testing"
	"context"
	"time"
)

type TestTask struct {
	err error
	delay time.Duration
	executed bool
}

func (t *TestTask) Run(c context.Context) error {
	<-time.After(t.delay)
	t.executed = true
	return t.err
}

func TestSpawnExecutor(t *testing.T) {
	q := make(chan Task)
	e := NewSpawnExecutorForQueue(q)
	t.Log("Starting Executor")
	e.Start()
	go func() {
		<-time.After(5*time.Second)
		t.Log("Stopping Executor")
		e.Stop()
	}()
	// send a normal task
	ts := &TestTask{}
	e.Send(ts)
	e.Wait()
	if !ts.executed {
		t.Errorf("Task not executed, got: %v, want: %v", ts.executed, true)
	}
}
