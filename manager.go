// Package taskmanager implements an async task manager. Structured tasks can be executed asynchronously
// on pre-provisioned workers. Alternatively functions could also be enqueued. Taskmanager can be stopped
// and all the workers/functions will receive a `Done` signal on the context passed. This needs to be handled
// in the task logic in order to clean up properly.
package taskmanager

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"go.uber.org/atomic"
)

var log = logging.Logger("taskmanager")

// Task defines the interface to be implemented by users to enqueue tasks to the
// TaskManager
type Task interface {
	Execute(context.Context) error
	Name() string
}

// TaskWithProgress defines additional functions to be implemented in order to query
// progress using the built-in taskmanager status
type TaskWithProgress interface {
	Task

	Progress() (float64, error)
	Description() string
}

// RestarableTask interface can be implemented to restart tasks on failures. The error
// passed to Restart is the error with which the task failed previously. Appropriate
// handling can be done based on that. If restart returns true, the task is enqueued
// with the manager again.
type RestartableTask interface {
	Task

	Restart(context.Context, error) bool
}

// WorkerStatus is helper type for restricting the string values of worker status to
// known constants
type WorkerStatus string

func (w WorkerStatus) String() string {
	return string(w)
}

const (
	NotAssigned WorkerStatus = "not assigned"
	Waiting     WorkerStatus = "waiting"
	Running     WorkerStatus = "running"
	Restarted   WorkerStatus = "restarted"
)

// TaskStatus is the status saved for the task. This status can be accessed using the
// TaskStatus() function
type TaskStatus struct {
	task        Task
	Name        string
	Worker      int32
	Status      WorkerStatus
	Description string
	Progress    float64
	Restarts    int
}

// WorkerInfo shows information on the worker
type WorkerInfo struct {
	TaskName string
	Status   WorkerStatus
}

// TaskManager implements the public API for taskmanager
type TaskManager struct {
	taskQueue     chan chan Task
	wmMutex       sync.Mutex
	workerMap     map[int32]WorkerInfo
	tMpMutex      sync.Mutex
	taskMap       map[string]TaskStatus
	context       context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	running       atomic.Int32
	workerId      atomic.Int32
	max           int
	min           int
	workerTimeout time.Duration
	stopped       atomic.Bool
}

// New creates a new taskmanager instance. minCount determines the minimum no of
// workers and maxCount determines the maximum worker count. timeout is used to determine
// when workers will be timed out on being idle
func New(minCount, maxCount int, timeout time.Duration) *TaskManager {
	ctx, cancel := context.WithCancel(context.Background())
	manager := &TaskManager{
		taskQueue:     make(chan chan Task, maxCount),
		workerMap:     make(map[int32]WorkerInfo, maxCount),
		taskMap:       make(map[string]TaskStatus),
		context:       ctx,
		cancel:        cancel,
		min:           minCount,
		max:           maxCount,
		workerTimeout: timeout,
	}
	manager.addWorkers(minCount)

	return manager
}

func (m *TaskManager) addWorkers(count int) {
	for i := 0; i < count && !m.stopped.Load(); i++ {
		if int(m.running.Load()) >= m.max {
			log.Info("reached max allowed workers while adding")
			return
		}
		currentId := m.workerId.Inc()
		log.Infof("starting worker : %d", currentId)
		m.wmMutex.Lock()
		m.workerMap[currentId] = WorkerInfo{TaskName: "idle", Status: Waiting}
		m.wmMutex.Unlock()
		m.running.Inc()
		m.wg.Add(1)
		w := newWorker(currentId, m)
		w.start()
	}
}

var ErrAlreadyExists = errors.New("task with same name already exists")

// Go enqueues the task to taskmanager. The function returns an error if we try
// to enqueue a task with the same name. The channel returned is closed when the
// task is actually assigned a worker.
func (m *TaskManager) Go(newTask Task) (<-chan struct{}, error) {
	m.tMpMutex.Lock()
	curr, exists := m.taskMap[newTask.Name()]
	if exists && curr.Status != Restarted {
		m.tMpMutex.Unlock()
		log.Errorf("trying to enqueue same task. Not allowed.")
		return nil, ErrAlreadyExists
	}
	restarts := 0
	if exists && curr.Status == Restarted {
		restarts = curr.Restarts
	}
	m.taskMap[newTask.Name()] = TaskStatus{
		Name:     newTask.Name(),
		Status:   NotAssigned,
		Restarts: restarts,
		task:     newTask,
	}
	m.tMpMutex.Unlock()

	sched := make(chan struct{})
	go func() {
		defer close(sched)
		for {
			select {
			case <-time.After(time.Millisecond * 500):
				current := int(m.running.Load())
				if m.max > current {
					newWorkers := math.Ceil(float64(m.max-current) / 2)
					log.Infof("adding %d new workers", int(newWorkers))
					m.addWorkers(int(newWorkers))
				}
			case <-m.context.Done():
				log.Warnf("manager stopped before task %s could be scheduled",
					newTask.Name())
				return
			case worker := <-m.taskQueue:
				open := true
				select {
				case _, open = <-worker:
				default:
				}
				if !open {
					log.Warnf("worker was timed out, ignoring...")
					break
				}
				log.Infof("dispatching task %s to worker", newTask.Name())
				worker <- newTask
				return
			}
		}
	}()
	return sched, nil
}

type closureTask struct {
	key     string
	closure func(context.Context) error
}

func (c *closureTask) Name() string {
	return c.key
}

func (c *closureTask) Execute(ctx context.Context) error {
	return c.closure(ctx)
}

// GoFunc is used to enqueue a closure to the taskmanager. key should be unique for each closure.
// If another closure with the same key is enqueued, we get an error.
func (m *TaskManager) GoFunc(key string, closure func(ctx context.Context) error) (<-chan struct{}, error) {
	return m.Go(&closureTask{key, closure})
}

// Status is used to obtain the status of workers in taskmanager
func (m *TaskManager) Status() (res map[int32]WorkerInfo) {
	m.wmMutex.Lock()
	defer m.wmMutex.Unlock()
	res = make(map[int32]WorkerInfo)
	for k, v := range m.workerMap {
		res[k] = v
	}
	return res
}

// TaskStatus is used to obtain status of tasks enqueued
func (m *TaskManager) TaskStatus() (res map[string]TaskStatus) {
	m.tMpMutex.Lock()
	defer m.tMpMutex.Unlock()
	res = make(map[string]TaskStatus)
	for k, v := range m.taskMap {
		status := TaskStatus{
			Name:     v.Name,
			Worker:   v.Worker,
			Status:   v.Status,
			Restarts: v.Restarts,
		}
		if progTask, ok := v.task.(TaskWithProgress); ok {
			status.Progress, _ = progTask.Progress()
			status.Description = progTask.Description()
		}
		res[k] = status
	}
	return res
}

// Stop is used to stop all running routines
func (m *TaskManager) Stop(ctx context.Context) error {
	log.Info("stopping taskmanager")
	m.cancel()
	m.stopped.Store(true)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		m.wg.Wait()
	}()
	select {
	case <-ctx.Done():
		return errors.New("failed to stop taskmanager")
	case <-stopped:
		return nil
	}
}

func (m *TaskManager) updateState(wId int32, name string, status WorkerStatus) {
	m.wmMutex.Lock()
	m.workerMap[wId] = WorkerInfo{TaskName: name, Status: status}
	m.wmMutex.Unlock()

	m.tMpMutex.Lock()
	oldStatus, exists := m.taskMap[name]
	if exists {
		m.taskMap[name] = TaskStatus{
			Name:     name,
			Worker:   wId,
			Status:   status,
			Restarts: oldStatus.Restarts,
			task:     oldStatus.task,
		}
	}
	m.tMpMutex.Unlock()
}

func (m *TaskManager) handleStart(id int32, tName string) {
	log.Infof("worker %d started running task %s", id, tName)
	m.updateState(id, tName, Running)
}

func (m *TaskManager) handleError(id int32, t Task, err error) {
	log.Infof("worker %d had error %s running task %s", id, err.Error(), t.Name())
	m.updateState(id, "idle", Waiting)
	if r, ok := t.(RestartableTask); ok && !errors.Is(err, context.Canceled) {
		if r.Restart(m.context, err) {
			m.tMpMutex.Lock()

			oldTask := m.taskMap[t.Name()]
			oldTask.Restarts++
			oldTask.Worker = 0
			oldTask.Status = Restarted

			m.taskMap[t.Name()] = oldTask
			m.tMpMutex.Unlock()
			_, e := m.Go(t)
			if e == nil {
				log.Infof("restarted task %s on err %s", t.Name(), err.Error())
				return
			} else {
				log.Errorf("failed restarting task err: %s", e.Error())
			}
		}
	}
	m.tMpMutex.Lock()
	delete(m.taskMap, t.Name())
	m.tMpMutex.Unlock()
}

func (m *TaskManager) handleSuccess(id int32, t Task) {
	log.Infof("worker %d successfully completed task %s", id, t.Name())
	m.updateState(id, "idle", Waiting)
	m.tMpMutex.Lock()
	delete(m.taskMap, t.Name())
	m.tMpMutex.Unlock()
}

func (m *TaskManager) handleWorkerTimeout(w *worker) {
	log.Infof("timed out Worker %d", w.id)
	if int(m.running.Load()) <= m.min {
		log.Info("worker count going below threshold. Restarting worker")
		w.start()
		return
	}
	m.running.Dec()
	m.wg.Done()
	m.wmMutex.Lock()
	delete(m.workerMap, w.id)
	m.wmMutex.Unlock()
}

func (m *TaskManager) handleWorkerStop(w *worker) {
	log.Infof("removing worker %d", w.id)
	m.running.Dec()
	m.wg.Done()
	m.wmMutex.Lock()
	delete(m.workerMap, w.id)
	m.wmMutex.Unlock()
}
