package mr

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	taskName       string
	nMap           int // number of map tasks to process
	nReduce        int // number of reduce tasks to process
	doneMapTask    map[int]bool
	doneReduceTask map[int]bool
	mapFiles       map[int]string

	mapTaskStream       chan TaskNode
	reduceTaskStream    chan TaskNode
	inProcessTaskStream chan TaskNode	// todo: 更直接的方式是为每一个分配的task启动一个计时goroutine
	doneMapTaskLock     sync.RWMutex
	doneReduceTaskLock  sync.RWMutex
	done                chan interface{}

	reduceTaskFilesMap map[int][]string
}

func (m *Master) readDoneMapTask(taskID int) bool {
	defer m.doneMapTaskLock.RUnlock()
	m.doneMapTaskLock.RLock()
	_, ok := m.doneMapTask[taskID]
	if !ok {
		return false
	}
	return true
}

func (m *Master) setDoneMapTask(taskID int) {
	defer m.doneMapTaskLock.Unlock()
	m.doneMapTaskLock.Lock()
	m.doneMapTask[taskID] = true
	return
}

func (m *Master) lenDoneMapTask() int {
	defer m.doneMapTaskLock.Unlock()
	m.doneMapTaskLock.Lock()
	return len(m.doneMapTask)
}

func (m *Master) readDoneReduceTask(taskID int) bool {
	defer m.doneReduceTaskLock.RUnlock()
	m.doneReduceTaskLock.RLock()
	_, ok := m.doneReduceTask[taskID]
	if !ok {
		return false
	}
	return true
}

func (m *Master) setDoneReduceTask(taskID int) {
	defer m.doneReduceTaskLock.Unlock()
	m.doneReduceTaskLock.Lock()
	m.doneReduceTask[taskID] = true
	return
}

func (m *Master) lenDoneReduceTask() int {
	defer m.doneReduceTaskLock.Unlock()
	m.doneReduceTaskLock.Lock()
	return len(m.doneReduceTask)
}

func (m *Master) taskTick(done <-chan interface{}) { // 处理执行中的任务
	for {
		select {
		case <-done:
			return
		case ingTask := <-m.inProcessTaskStream:
			if ingTask.TaskType == MAP_TASK || ingTask.TaskType == REDUCE_TASK {
				if ingTask.TaskType == MAP_TASK && m.readDoneMapTask(ingTask.ID) {
					continue
				}
				if ingTask.TaskType == REDUCE_TASK && m.readDoneReduceTask(ingTask.ID) {
					continue
				}
				if ingTask.BeginAT.Add(10 * time.Second).Before(time.Now()) {
					if ingTask.TaskType == MAP_TASK {
						m.mapTaskStream <- ingTask
					}
					if ingTask.TaskType == REDUCE_TASK {
						m.reduceTaskStream <- ingTask
					}
				} else {
					m.inProcessTaskStream <- ingTask
				}
			}
		case <-time.After(time.Second):
			println("task Tick: ingTask idle")
		}
	}
}

// Your code here -- RPC handlers for the worker to call.

// dispatch map or reduce task or exit
func (m *Master) DispatchTask(args *DispatchTaskReq, reply *DispatchTaskResp) error {

	defer func() {
		reply.Task.TaskName = m.taskName
	}()
	println(args.PID)
	// 分发map任务
	for {
		if m.nReduce == m.lenDoneReduceTask() {
			reply.Task.TaskType = EXIST_TASK
			return nil
		}
		select {
		case task := <-m.mapTaskStream:
			reply.Task.TaskType = MAP_TASK
			reply.Task.ID = task.ID
			reply.Task.FileName = task.FileName
			reply.Task.NReduce = m.nReduce
			println("get task from stream: ", task.ID, " ", task.FileName)
			task.BeginAT = time.Now()
			task.WorkerID = args.IP + "-" + string(args.PID)
			m.inProcessTaskStream <- task
			return nil
		case task := <-m.reduceTaskStream:
			reply.Task.TaskType = REDUCE_TASK
			reply.Task.ID = task.ID
			reply.Task.NReduce = m.nReduce
			reply.Task.ReduceTaskFiles = m.reduceTaskFilesMap[task.ID]
			task.BeginAT = time.Now()
			task.WorkerID = args.IP + "-" + string(args.PID)
			m.inProcessTaskStream <- task
			return nil
		default:
			reply.Task.TaskType = WATIT_TASK
			return nil
		}
	}

	return nil
}

// process completed tasks
func (m *Master) CompleteTask(args *CompleteTaskReq, reply *CompleteTaskResp) error {

	if args.TaskType == MAP_TASK {
		m.setDoneMapTask(args.TaskID)
	} else if args.TaskType == REDUCE_TASK {
		m.setDoneReduceTask(args.TaskID)
	}

	return nil
}

func (m *Master) CollectSubMapReduceTask(args *CollectInterFilesReq, reply *CollectInterFilesResp) error {
	if args != nil {
		for reduceTaskNO, files := range args.SubMapReduceTaskMap {
			m.reduceTaskFilesMap[reduceTaskNO] = append(m.reduceTaskFilesMap[reduceTaskNO], files...)
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)

	go func() { // 初始化顺序队列
		for mapID, fileName := range m.mapFiles {
			if fileName != "" {
				m.mapTaskStream <- TaskNode{
					ID:       mapID,
					WorkerID: "",
					TaskType: MAP_TASK,
					FileName: fileName,
				}
			}
		}
		println("init map task done")
		for m.nMap > m.lenDoneMapTask() { // todo: cond 方式优化
			time.Sleep(time.Second)
		}
		println("Map task done")
		for i := 0; i < m.nReduce; i++ {
			task := TaskNode{
				ID:       i,
				TaskType: REDUCE_TASK,
				TaskName: m.taskName,
			}
			m.reduceTaskStream <- task
		}

		for m.nReduce > m.lenDoneReduceTask() {
			time.Sleep(time.Second)
		}

		println("Reduce task done")

		close(m.done)
	}()
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if m.nReduce == m.lenDoneReduceTask() {
		ret = true
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.nReduce = nReduce
	m.nMap = len(files)
	m.mapTaskStream = make(chan TaskNode, 10)
	m.inProcessTaskStream = make(chan TaskNode, 100)
	m.reduceTaskStream = make(chan TaskNode, 10)
	m.doneMapTask = map[int]bool{}
	m.doneReduceTask = map[int]bool{}
	m.reduceTaskFilesMap = map[int][]string{}

	done := make(chan interface{})
	m.done = done

	rand.Seed(time.Now().UnixNano())
	taskName := fmt.Sprintf("mr-%d", rand.Int())
	m.taskName = taskName
	go func() { // 初始化map任务
		for ind, fileName := range files {
			taskNode := TaskNode{
				ID:       ind,
				BeginAT:  time.Now(),
				WorkerID: "",
				TaskType: MAP_TASK,
				FileName: fileName,
				TaskName: m.taskName,
			}
			m.mapTaskStream <- taskNode
		}
	}()

	go m.taskTick(done)

	m.server()
	return &m
}
