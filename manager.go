package taskmanager

import (
	"context"
	"errors"
	"go.uber.org/atomic"
	"math"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
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

// RestarableTask interface can be implemented to restart tasks on failures
type RestartableTask interface {
	Task

	Restart(context.Context, error) bool
}

type TaskStatus struct {
	task         Task
	Name         string
	Worker       int32
	WorkerStatus string
	Description  string
	Progress     float64
	Restarts     int
}

type WorkerInfo struct {
	TaskName string
	Status   string
}

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
	max           int
	min           int
	workerTimeout time.Duration
	stopped       atomic.Bool
}

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

var workerId atomic.Int32

func (m *TaskManager) addWorkers(count int) {
	m.wmMutex.Lock()
	defer m.wmMutex.Unlock()
	for i := 0; i < count && !m.stopped.Load(); i++ {
		if int(m.running.Load()) >= m.max {
			log.Info("reached max allowed workers while adding")
			return
		}
		currentId := workerId.Inc()
		log.Infof("starting worker : %d", currentId)
		m.workerMap[currentId] = WorkerInfo{TaskName: "idle", Status: "waiting"}
		m.running.Inc()
		m.wg.Add(1)
		w := newWorker(currentId, m)
		w.start()
	}
}

var ErrAlreadyExists = errors.New("task with same name already exists")

func (m *TaskManager) Go(newTask Task) (<-chan struct{}, error) {
	m.tMpMutex.Lock()
	curr, exists := m.taskMap[newTask.Name()]
	if exists && curr.WorkerStatus != "restarted" {
		m.tMpMutex.Unlock()
		log.Errorf("trying to enqueue same task. Not allowed.")
		return nil, ErrAlreadyExists
	}
	restarts := 0
	if exists && curr.WorkerStatus == "restarted" {
		restarts = curr.Restarts
	}
	m.taskMap[newTask.Name()] = TaskStatus{
		Name:         newTask.Name(),
		WorkerStatus: "not running",
		Restarts:     restarts,
		task:         newTask,
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

func (m *TaskManager) GoFunc(key string, closure func(ctx context.Context) error) (<-chan struct{}, error) {
	return m.Go(&closureTask{key, closure})
}

func (m *TaskManager) updateState(wId int32, name, status string) {
	m.wmMutex.Lock()
	m.workerMap[wId] = WorkerInfo{TaskName: name, Status: status}
	m.wmMutex.Unlock()

	m.tMpMutex.Lock()
	oldStatus, exists := m.taskMap[name]
	if exists {
		m.taskMap[name] = TaskStatus{
			Name:         name,
			Worker:       wId,
			WorkerStatus: status,
			Restarts:     oldStatus.Restarts,
			task:         oldStatus.task,
		}
	}
	m.tMpMutex.Unlock()
}

func (m *TaskManager) Status() (res map[int32]WorkerInfo) {
	m.wmMutex.Lock()
	defer m.wmMutex.Unlock()
	res = make(map[int32]WorkerInfo)
	for k, v := range m.workerMap {
		res[k] = v
	}
	return res
}

func (m *TaskManager) TaskStatus() (res map[string]TaskStatus) {
	m.tMpMutex.Lock()
	defer m.tMpMutex.Unlock()
	res = make(map[string]TaskStatus)
	for k, v := range m.taskMap {
		status := TaskStatus{
			Name:         v.Name,
			Worker:       v.Worker,
			WorkerStatus: v.WorkerStatus,
			Restarts:     v.Restarts,
		}
		if progTask, ok := v.task.(TaskWithProgress); ok {
			status.Progress, _ = progTask.Progress()
			status.Description = progTask.Description()
		}
		res[k] = status
	}
	return res
}

func (m *TaskManager) Stop() {
	log.Info("stopping taskmanager")
	m.cancel()
	m.stopped.Store(true)
	m.wg.Wait()
}

func (m *TaskManager) handleStart(id int32, tName string) {
	log.Infof("worker %d started running task %s", id, tName)
	m.updateState(id, tName, "running")
}

func (m *TaskManager) handleError(id int32, t Task, err error) {
	log.Infof("worker %d had error %s running task %s", id, err.Error(), t.Name())
	m.updateState(id, "idle", "waiting")
	if r, ok := t.(RestartableTask); ok && !errors.Is(err, context.Canceled) {
		if r.Restart(m.context, err) {
			m.tMpMutex.Lock()

			oldTask := m.taskMap[t.Name()]
			oldTask.Restarts++
			oldTask.Worker = 0
			oldTask.WorkerStatus = "restarted"

			m.taskMap[t.Name()] = oldTask
			m.tMpMutex.Unlock()
			m.Go(t)
			log.Infof("restarted task %s on err %s", t.Name(), err.Error())
			return
		}
	}
	m.tMpMutex.Lock()
	delete(m.taskMap, t.Name())
	m.tMpMutex.Unlock()
	return
}

func (m *TaskManager) handleSuccess(id int32, t Task) {
	log.Infof("worker %d successfully completed task %s", id, t.Name())
	m.updateState(id, "idle", "waiting")
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
