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
				w.logger.Errorf("PANIC in TaskManager worker")
				debug.PrintStack()
				w.logger.Errorf("recovered in worker %d", w.id)
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
				w.logger.Infof("worker %d : stopping", w.id)
				return
			case w.taskQueue <- w.task:
			}
			select {
			case task := <-w.task:
				w.logger.Infof("worker %d : Received work request", w.id)
				w.mgr.handleStart(w.id, task.Name())
				w.currTask = task
				err := task.Execute(w.context)
				if err != nil {
					w.logger.Errorf("worker %d : Failed with error : %s", w.id, err.Error())
					w.mgr.handleError(w.id, task, err)
				} else {
					w.logger.Infof("worker %d : Task finished successfully", w.id)
					w.mgr.handleSuccess(w.id, task)
				}
			case <-w.context.Done():
				w.logger.Infof("worker %d : stopping", w.id)
				return
			case <-time.After(w.mgr.workerTimeout):
				w.logger.Infof("worker %d : idle timeout", w.id)
				timedOut = true
				close(w.task)
				w.mgr.handleWorkerTimeout(w)
				return
			}
		}
	}()
}
