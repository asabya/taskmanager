package taskmanager

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"go.uber.org/atomic"
)

// workRequest is a worker's offer to accept a task. The worker places it on the
// task queue when it becomes idle and then waits on reply. Exactly one of the
// scheduler (claim) or the worker (withdraw) wins the race to finalize the
// request's state, which guarantees a task is only ever sent to a worker that is
// still listening on reply. This replaces the previous scheme of closing the
// worker channel to signal a timeout, which could race with the scheduler's send
// and panic with "send on closed channel".
type workRequest struct {
	reply chan Task
	state atomic.Int32
}

const (
	reqPending   int32 = iota // up for grabs
	reqClaimed                // scheduler took it and will send on reply
	reqWithdrawn              // worker gave up (idle timeout / shutdown)
)

type worker struct {
	id        int32
	taskQueue chan *workRequest
	context   context.Context
	currTask  Task
	mgr       *TaskManager

	logger Logger
}

func newWorker(id int32, m *TaskManager, log Logger) *worker {
	return &worker{
		id:        id,
		taskQueue: m.taskQueue,
		context:   m.context,
		mgr:       m,
		logger:    log,
	}
}

func (w *worker) start() {
	timedOut := false
	go func() {
		defer func() {
			if r := recover(); r != nil {
				w.logger.Error(fmt.Errorf("PANIC in TaskManager worker"), "PANIC in TaskManager worker")
				debug.PrintStack()
				w.logger.Error(fmt.Errorf("recovered in worker"), "recovered in worker", "worker_id", w.id)
				if w.currTask != nil {
					w.mgr.handleError(w.id, w.currTask, errors.New("panic"))
				}
				// Route through handleWorkerPanic (not handleWorkerStop) so the
				// worker gets replaced if the pool would otherwise drop below
				// min, instead of permanently shrinking the pool on every panic.
				w.mgr.handleWorkerPanic(w)
				return
			}
			if !timedOut {
				w.mgr.handleWorkerStop(w)
			}
		}()
		for {
			req := &workRequest{reply: make(chan Task)}
			select {
			case <-w.context.Done():
				w.logger.Info("worker : stopping", w.id)
				return
			case w.taskQueue <- req:
			}

			var task Task
			select {
			case task = <-req.reply:
				// scheduler claimed our offer and handed us a task
			case <-w.context.Done():
				if req.state.CompareAndSwap(reqPending, reqWithdrawn) {
					w.logger.Info("worker : stopping", w.id)
					return
				}
				// lost the race: scheduler claimed concurrently, so a task is
				// on its way. Receive it to unblock the scheduler.
				task = <-req.reply
			case <-time.After(w.mgr.workerTimeout):
				if req.state.CompareAndSwap(reqPending, reqWithdrawn) {
					w.logger.Info("worker : idle timeout", w.id)
					timedOut = true
					w.mgr.handleWorkerTimeout(w)
					return
				}
				// lost the race: scheduler claimed just as we timed out.
				task = <-req.reply
			}

			w.logger.Info("worker: Received work request", w.id)
			w.mgr.handleStart(w.id, task.Name())
			w.currTask = task
			if err := task.Execute(w.context); err != nil {
				w.logger.Error(err, "worker : Failed with error :", "worker_id", w.id)
				w.mgr.handleError(w.id, task, err)
			} else {
				w.logger.Info("worker : Task finished successfully", w.id)
				w.mgr.handleSuccess(w.id, task)
			}
		}
	}()
}
