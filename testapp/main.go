package main

import (
	"github.com/rskumar/executor"
	"context"
	"fmt"
	"sync"
	"runtime"
	"bytes"
	"strconv"
	"time"
	"math/rand"
)

var (
	ID = 0
	IdLock = new(sync.Mutex)
)

func NextID() int {
	IdLock.Lock()
	defer IdLock.Unlock()
	ID++
	n := ID
	return n
}

type PrintTask struct {
	ID int
}

func (t PrintTask) Run(c context.Context) error {
	gid := getGID()
	fmt.Printf("Task: %d, GO:%d : IN\n", t.ID, gid)
	defer func() {
		fmt.Printf("Task: %d, GO:%d : OUT\n", t.ID, gid)
	}()
	<-time.After(time.Duration(rand.Intn(10)) * time.Second)
	return nil
}

func NewPrintTask() *PrintTask {
	t := &PrintTask{NextID()}
	return t
}

func GenerateTask(num int, x executor.Executor) {
	for i := 0; i < num; i++ {
		pt := NewPrintTask()
		x.Send(pt)
	}
}

// getGID returns current goroutine ID. **DO NOT USE** Only for debug
func getGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

func main() {
	x := executor.NewSpawnExecutor()
	x.Start()
	go GenerateTask(5, x)
	go func() {
		<-time.After(6*time.Hour)
		x.Stop()
	}()
	x.Wait()
}