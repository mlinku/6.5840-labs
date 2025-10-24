package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type TaskState int

const (
	TaskStatePending TaskState = iota
	TaskStateInProgress
	TaskStateCompleted
)

type Coordinator struct {
	// lock to protect shared state
	mu sync.Mutex
	// mantain a counter for worker IDs, initialize to 0
	WorkerIDCounter int
	// input files for map tasks
	MapFiles []string
	// record map tasks state
	MapTaskState map[int]TaskState
	// record number of successful map tasks
	NumDoneMapTasks int
	// number of reduce workers
	NReduce int
	// intermediate files for reduce tasks
	ReduceFiles []string
	// record reduce tasks state
	ReduceTaskState map[int]TaskState
	// record number of successful reduce tasks
	NumDoneReduceTasks int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	fmt.Println("Example RPC called with args:", args.X)
	return nil
}

// init worker RPC handler
func (c *Coordinator) InitWorker(args *WorkerArgs, reply *InitWorkerReply) error {
	reply.WorkerID = c.WorkerIDCounter
	c.WorkerIDCounter++
	fmt.Println("Init Worker RPC called with WorkerID:", reply.WorkerID)
	return nil
}

// CoordinateTask is a helper function to assign tasks to workers
// first complete all map tasks, then reduce tasks
func CoordinateTask(c *Coordinator, reply *AssignTaskReply) {
	// if there are remaining map tasks, assign a map task
	if MapTaskID := freeMapTaskID(c); MapTaskID != -1 {
		reply.TaskType = TaskMap
		reply.TaskFile = c.MapFiles[MapTaskID]
		// generate intermediate file prefix
		reply.TaskID = MapTaskID
		reply.TaskNum = c.NReduce
		c.MapTaskState[MapTaskID] = TaskStateInProgress
		fmt.Printf("Assigned Map task for file %v to WorkerID %v\n", reply.TaskFile, reply.WorkerID)
		return
	} else if c.NumDoneMapTasks < len(c.MapFiles) {
		// there are still map tasks in progress
		reply.TaskType = TaskWait
		fmt.Printf("Map tasks in progress. Instructing WorkerID %v to wait.\n", reply.WorkerID)
		return
	} else if ReduceTaskID := freeReduceTaskID(c); ReduceTaskID != -1 {
		reply.TaskType = TaskReduce
		reply.TaskFile = fmt.Sprintf("mr-%d-%d", c.MapTaskID, ReduceTaskID)
		reply.TaskID = ReduceTaskID
		reply.TaskNum = len(c.MapFiles)
		fmt.Printf("Assigned Reduce task for file %v to WorkerID %v\n", reply.TaskFile, reply.WorkerID)
		return
	} else if c.ReduceTaskID == c.NReduce && c.NumDoneReduceTasks < c.NReduce {
		// there are still reduce tasks in progress
		reply.TaskType = TaskWait
		fmt.Printf("Reduce tasks in progress. Instructing WorkerID %v to wait.\n", reply.WorkerID)
		return
	} else if c.NumDoneMapTasks == len(c.MapFiles) && c.NumDoneReduceTasks == c.NReduce {
		reply.TaskType = TaskExit
		fmt.Printf("All tasks offered. Instructing WorkerID %v to exit.\n", reply.WorkerID)
	}
}
func (c *Coordinator) AssignTask(arg *WorkerArgs, reply *AssignTaskReply) error {
	reply.WorkerID = arg.WorkerID

	CoordinateTask(c, reply)
	return nil
}

func (c *Coordinator) ReportTask(arg *WorkerArgs, reply *ReportTaskReply) error {
	// update the coordinator's state based on the worker's report
	switch arg.CallType {
	case CallReportMap:
		c.NumDoneMapTasks++
		c.MapTaskDone[arg.TaskID] = true
		reply.Success = true
	case CallReportReduce:
		c.NumDoneReduceTasks++
		c.ReduceTaskDone[arg.TaskID] = true
		reply.Success = true
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.WorkerIDCounter = 0
	c.MapFiles = files
	c.NReduce = nReduce
	c.MapTaskID = 0
	c.ReduceTaskID = 0
	c.MapTaskDone = make(map[int]bool)
	c.ReduceTaskDone = make(map[int]bool)
	c.NumDoneMapTasks = 0
	c.NumDoneReduceTasks = 0
	// Your code here.

	c.server()
	return &c
}
