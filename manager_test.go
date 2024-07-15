package taskmanager_test

import (
	"context"
	"errors"
	"log"
	"testing"
	"time"

	"github.com/asabya/taskmanager"
	"go.uber.org/goleak"
)

type mockLogging struct{}

func (mockLogging) Info(msg string, args ...interface{}) {
	log.Print(msg)
	log.Println(args...)
}

func (mockLogging) Error(msg string, args ...interface{}) {
	log.Print(msg)
	log.Println(args...)
}

type testTask struct {
	name      string
	stopC     chan struct{}
	panicC    chan struct{}
	err       error
	restarted bool
	progress  float64
}

func newTestTask(name string) *testTask {
	return &testTask{
		name:   name,
		stopC:  make(chan struct{}),
		panicC: make(chan struct{}),
	}
}

func (t *testTask) Execute(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.stopC:
		return t.err
	case <-t.panicC:
		panic("PANIC!")
	}
}

func (t *testTask) Name() string {
	return t.name
}

func (t *testTask) stop(err error) {
	t.err = err
	close(t.stopC)
}

func (t *testTask) panicc() {
	close(t.panicC)
}

func (t *testTask) Restart(ctx context.Context, err error) bool {
	if t.restarted {
		return false
	}
	t.restarted = true
	t.stopC = make(chan struct{})
	t.panicC = make(chan struct{})
	return true
}

func (t *testTask) setProgress(prog float64) {
	t.progress = prog
}

func (t *testTask) Progress() (float64, error) {
	return t.progress, nil
}

func (t *testTask) Description() string {
	return "dummy description"
}

func TestTaskManager(t *testing.T) {
	ml := &mockLogging{}

	t.Run("initial state", func(t *testing.T) {
		tm := taskmanager.New(1, 3, time.Second, ml)
		defer func() {
			_ = tm.Stop(context.Background())
		}()
		verifyWorkerCount(t, tm, 1)
		verifyWorkerInfo(t, tm, 1, "idle", "waiting")
	})

	goWork := func(tt *testTask, tm *taskmanager.TaskManager) {
		sched, err := tm.Go(tt)
		if err != nil {
			t.Fatalf("failed starting task %s", err.Error())
		}
		<-sched
	}

	t.Run("add tasks", func(t *testing.T) {
		tm := taskmanager.New(1, 3, time.Second, ml)
		defer func() {
			_ = tm.Stop(context.Background())
		}()

		t1 := newTestTask("1")
		goWork(t1, tm)
		time.Sleep(time.Millisecond * 100)
		verifyWorkerCount(t, tm, 1)
		verifyTaskStatus(t, tm, t1.Name(), "running")

		t2 := newTestTask("2")
		goWork(t2, tm)
		time.Sleep(time.Millisecond * 100)
		verifyWorkerCount(t, tm, 2)
		verifyTaskStatus(t, tm, t2.Name(), "running")

		t1.stop(nil)
		time.Sleep(time.Millisecond * 100)
		verifyWorkerCount(t, tm, 2)
		verifyTaskStatus(t, tm, t2.Name(), "running")

		t2.stop(nil)
		time.Sleep(time.Millisecond * 100)
		verifyAllIdle(t, tm)
	})

	t.Run("add more than capacity", func(t *testing.T) {
		tm := taskmanager.New(1, 3, time.Second, ml)
		defer func() {
			_ = tm.Stop(context.Background())
		}()

		t1 := newTestTask("1")
		t2 := newTestTask("2")
		t3 := newTestTask("3")
		t4 := newTestTask("4")

		goWork(t1, tm)
		goWork(t2, tm)
		goWork(t3, tm)

		sched, err := tm.Go(t4)
		if err != nil {
			t.Fatal(err)
		}

		time.Sleep(time.Millisecond * 100)
		verifyWorkerCount(t, tm, 3)
		verifyTaskStatus(t, tm, t4.Name(), "not assigned")

		t1.stop(nil)
		<-sched
		time.Sleep(time.Millisecond * 100)
		verifyTaskStatus(t, tm, t4.Name(), "running")

		t2.stop(nil)
		t3.stop(nil)
		t4.stop(nil)
		time.Sleep(time.Millisecond * 100)
		verifyAllIdle(t, tm)
	})

	t.Run("fail duplicate", func(t *testing.T) {
		tm := taskmanager.New(1, 3, time.Second, ml)
		defer func() {
			_ = tm.Stop(context.Background())
		}()

		t1 := newTestTask("1")
		t2 := newTestTask("1")

		goWork(t1, tm)
		_, err := tm.Go(t2)
		if err == nil {
			t.Fatal("expected failure on duplicate task")
		}
		t1.stop(nil)
		time.Sleep(time.Millisecond * 100)
		verifyAllIdle(t, tm)
	})

	t.Run("restart on error", func(t *testing.T) {
		tm := taskmanager.New(1, 3, time.Second, ml)
		defer func() {
			_ = tm.Stop(context.Background())
		}()

		t1 := newTestTask("1")
		goWork(t1, tm)
		time.Sleep(time.Millisecond * 100)
		verifyTaskStatus(t, tm, t1.Name(), "running")

		t1.stop(errors.New("dummy error"))
		time.Sleep(time.Millisecond * 500)
		verifyTaskStatus(t, tm, t1.Name(), "running")

		if t1.restarted != true {
			t.Fatal("task should have been restarted")
		}
		st := tm.TaskStatus()[t1.Name()]
		if st.Restarts != 1 {
			t.Fatalf("expected restarts to be 1. found %d", st.Restarts)
		}
		t1.stop(nil)
		time.Sleep(time.Millisecond * 100)
		verifyAllIdle(t, tm)
	})

	t.Run("recover and restart on panic", func(t *testing.T) {
		tm := taskmanager.New(1, 3, time.Second, ml)
		defer func() {
			_ = tm.Stop(context.Background())
		}()

		t1 := newTestTask("1")
		goWork(t1, tm)
		time.Sleep(time.Millisecond * 100)
		verifyTaskStatus(t, tm, t1.Name(), "running")

		t1.panicc()
		time.Sleep(time.Second)
		verifyTaskStatus(t, tm, t1.Name(), "running")

		if t1.restarted != true {
			t.Fatal("task should have been restarted")
		}
		st := tm.TaskStatus()[t1.Name()]
		if st.Restarts != 1 {
			t.Fatalf("expected restarts to be 1. found %d", st.Restarts)
		}
		t1.stop(nil)
	})

	t.Run("with progress", func(t *testing.T) {
		tm := taskmanager.New(1, 3, time.Second, ml)
		defer func() {
			_ = tm.Stop(context.Background())
		}()

		t1 := newTestTask("1")
		goWork(t1, tm)
		time.Sleep(time.Millisecond * 100)
		verifyTaskStatus(t, tm, t1.Name(), "running")

		t1.setProgress(50.0)
		status := tm.TaskStatus()[t1.Name()]

		if status.Progress != 50.0 {
			t.Fatalf("expected progress to be 50 found %f", status.Progress)
		}

		t1.setProgress(90.0)
		status = tm.TaskStatus()[t1.Name()]

		if status.Progress != 90.0 {
			t.Fatalf("expected progress to be 90 found %f", status.Progress)
		}

		t1.stop(nil)
		time.Sleep(time.Millisecond * 100)
		verifyAllIdle(t, tm)
	})

	t.Run("closure", func(t *testing.T) {
		tm := taskmanager.New(1, 3, time.Second, ml)
		defer func() {
			_ = tm.Stop(context.Background())
		}()

		closer := make(chan struct{})
		sched, err := tm.GoFunc("closure", func(ctx context.Context) error {
			select {
			case <-closer:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		<-sched
		time.Sleep(time.Millisecond * 100)
		verifyTaskStatus(t, tm, "closure", "running")
		close(closer)
		time.Sleep(time.Millisecond * 100)
		verifyAllIdle(t, tm)
	})

	t.Run("timeout", func(t *testing.T) {
		tm := taskmanager.New(1, 3, time.Second, ml)
		defer func() {
			_ = tm.Stop(context.Background())
		}()

		t1 := newTestTask("1")
		goWork(t1, tm)
		t2 := newTestTask("2")
		goWork(t2, tm)
		t3 := newTestTask("3")
		goWork(t3, tm)
		time.Sleep(time.Millisecond * 100)

		verifyTaskStatus(t, tm, t1.Name(), "running")
		verifyTaskStatus(t, tm, t2.Name(), "running")
		verifyTaskStatus(t, tm, t3.Name(), "running")

		t1.stop(nil)
		t2.stop(nil)
		t3.stop(nil)

		time.Sleep(time.Millisecond * 100)
		verifyWorkerCount(t, tm, 3)

		// 2 workers should timeout
		time.Sleep(time.Second * 2)
		verifyWorkerCount(t, tm, 1)

		t4 := newTestTask("4")
		goWork(t4, tm)
		t5 := newTestTask("5")
		goWork(t5, tm)
		time.Sleep(time.Millisecond * 100)
		verifyTaskStatus(t, tm, t4.Name(), "running")
		verifyTaskStatus(t, tm, t5.Name(), "running")
	})

	t.Run("stop", func(t *testing.T) {
		tm := taskmanager.New(1, 3, time.Second, ml)

		t1 := newTestTask("1")
		goWork(t1, tm)
		t2 := newTestTask("2")
		goWork(t2, tm)
		t3 := newTestTask("3")
		goWork(t3, tm)
		// Have extra tasks waiting while stop
		t4 := newTestTask("4")
		_, _ = tm.Go(t4)
		t5 := newTestTask("5")
		_, _ = tm.Go(t5)
		time.Sleep(time.Millisecond * 100)

		verifyTaskStatus(t, tm, t1.Name(), "running")
		verifyTaskStatus(t, tm, t2.Name(), "running")
		verifyTaskStatus(t, tm, t3.Name(), "running")

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		err := tm.Stop(ctx)
		if err != nil {
			t.Fatalf("failed to stop taskmanager %s", err.Error())
		}
		verifyWorkerCount(t, tm, 0)
	})
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func verifyAllIdle(t *testing.T, tm *taskmanager.TaskManager) {
	t.Helper()
	for _, v := range tm.Status() {
		if v.TaskName != "idle" {
			t.Fatalf("invalid taskname exp idle found %s", v.TaskName)
		}
		if v.Status != taskmanager.Waiting {
			t.Fatalf("invalid taskstatus exp waiting found %s", v.Status)
		}
	}
}

func verifyWorkerCount(t *testing.T, tm *taskmanager.TaskManager, count int) {
	t.Helper()
	if len(tm.Status()) != count {
		t.Fatalf("invalid worker count Exp: %d Found: %d", count, len(tm.Status()))
	}
}

func verifyWorkerInfo(t *testing.T, tm *taskmanager.TaskManager, wId int32, tName, state string) {
	t.Helper()
	st, ok := tm.Status()[wId]
	if !ok {
		t.Fatal("worker ID not present")
	}
	if st.TaskName != tName {
		t.Fatalf("invalid task name in worker info exp %s found %s", tName, st.TaskName)
	}
	if st.Status != taskmanager.WorkerStatus(state) {
		t.Fatalf("invalid status in worker info exp %s found %s", state, st.Status)
	}
}

func verifyTaskStatus(t *testing.T, tm *taskmanager.TaskManager, tName, state string) {
	t.Helper()
	ts, ok := tm.TaskStatus()[tName]
	if !ok {
		t.Fatal("task status not present")
	}
	if ts.Status != taskmanager.WorkerStatus(state) {
		t.Fatalf("worker state incorrect exp %s found %s", state, ts.Status)
	}
}
