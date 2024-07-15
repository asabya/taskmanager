package taskmanager

import (
	"context"
	"errors"
	"runtime/debug"
	"time"
)

type worker struct {
	id        int32
	task      chan Task
	taskQueue chan chan Task
	context   context.Context
	currTask  Task
	mgr       *TaskManager

	logger Logger
}

func newWorker(id int32, m *TaskManager, log Logger) *worker {
	return &worker{
		id:        id,
		task:      make(chan Task),
		taskQueue: m.taskQueue,
		context:   m.context,
		mgr:       m,
		logger:    log,
	}
}

func (w *worker) start() {
	w.task = make(chan Task)
	timedOut := false
	go func() {
		defer func() {
			if r := recover(); r != nil {
				w.logger.Error("PANIC in TaskManager worker")
				debug.PrintStack()
				w.logger.Error("recovered in worker", w.id)
				if w.currTask != nil {
					w.mgr.handleError(w.id, w.currTask, errors.New("Panic"))
				}
			}
			if !timedOut {
				close(w.task)
				w.mgr.handleWorkerStop(w)
			}
		}()
		for {
			select {
			case <-w.context.Done():
				w.logger.Info("worker : stopping", w.id)
				return
			case w.taskQueue <- w.task:
			}
			select {
			case task := <-w.task:
				w.logger.Info("worker: Received work request", w.id)
				w.mgr.handleStart(w.id, task.Name())
				w.currTask = task
				err := task.Execute(w.context)
				if err != nil {
					w.logger.Error("worker : Failed with error :", w.id, err.Error())
					w.mgr.handleError(w.id, task, err)
				} else {
					w.logger.Info("worker : Task finished successfully", w.id)
					w.mgr.handleSuccess(w.id, task)
				}
			case <-w.context.Done():
				w.logger.Info("worker : stopping", w.id)
				return
			case <-time.After(w.mgr.workerTimeout):
				w.logger.Info("worker : idle timeout", w.id)
				timedOut = true
				close(w.task)
				w.mgr.handleWorkerTimeout(w)
				return
			}
		}
	}()
}
