package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// example to show how to declare the arguments
// and reply for an RPC.

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// worker_rpc_definition
type WorkerArgs struct {
	WorkerID int
	CallType CallType // 0: init 1: assign task 2: report map task 3: report reduce task
	TaskID   int      // used for report task
}

type InitWorkerReply struct {
	WorkerID int
}

type AssignTaskReply struct {
	WorkerID int
	TaskType TaskType //  0: Map 1: Reduce 2: Wait 3: Exit
	TaskFile string   // Map task: input file; Reduce task: intermediate files
	TaskID   int      // TaskID
	TaskNum  int      // Total number of tasks
}

type ReportTaskReply struct {
	Success bool
}

type CallType int

const (
	CallInit CallType = iota
	CallAssign
	CallReportMap
	CallReportReduce
)

type TaskType int

const (
	TaskMap TaskType = iota
	TaskReduce
	TaskWait
	TaskExit
)

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
