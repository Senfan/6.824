package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"log"
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskType int

const (
	MAP_TASK    TaskType = 1
	REDUCE_TASK TaskType = 2
	EXIST_TASK  TaskType = 3
	WATIT_TASK  TaskType = 4
)

type TaskStatus int

const (
	TASK_STATUS_IDLE        TaskStatus = 1
	TASK_STATUS_IN_PROGRESS TaskStatus = 2
	TASK_STATUS_COMPLETE    TaskStatus = 3
)

type TaskReqType int

const (
	TASK_REQ_TYPE_CALL_FOR_NEW_TASK TaskReqType = 1 // 请求新任务
	TASK_REQ_TYPE_REPORT_TASK       TaskReqType = 2 // 报告当前任务完成
)

// ask for task request args
type DispatchTaskReq struct {
	IP  string
	PID int
}

// correspond reply
type DispatchTaskResp struct {
	Task TaskNode
}

type CompleteTaskReq struct {
	TaskType TaskType
	TaskID   int
}
type CompleteTaskResp struct {
}

type CollectInterFilesReq struct {
	SubMapReduceTaskMap map[int][]string
}
type CollectInterFilesResp struct {
}

type TaskNode struct {
	ID              int
	BeginAT         time.Time
	WorkerID        string
	TaskType        TaskType
	FileName        string
	TaskName        string
	ReduceTaskFiles []string
	NReduce         int
}

// Add your RPC definitions here.
// ask for task from master
func CallTask(args *DispatchTaskReq) *DispatchTaskResp {
	reply := DispatchTaskResp{}
	isSuc := call("Master.DispatchTask", args, &reply)

	if !isSuc {
		println("can not get the task from master")
		return nil
	}
	return &reply
}

func CompleteTaskReport(args *CompleteTaskReq) *CompleteTaskResp {
	reply := CompleteTaskResp{}
	isSuc := call("Master.CompleteTask", &args, &reply)

	if !isSuc {
		log.Fatal("can not get the task from master")
		return nil
	}

	return &reply
}

func SendInterFiles(args *CollectInterFilesReq) *CollectInterFilesResp {
	reply := CollectInterFilesResp{}

	isSuc := call("Master.CollectSubMapReduceTask", &args, &reply)

	if !isSuc {
		log.Fatal("can not get the task from master")
		return nil
	}

	return &reply
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
