package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
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
	// worker id counter
	workerIDCounter int
	// number of map tasks
	mMap int
	// map tasks and their state
	mapTasks        []MapTask
	numDoneMapTasks int
	// number of reduce workers
	nReduce int
	// intermediate files for reduce tasks
	reduceTasks        []ReduceTask
	numDoneReduceTasks int
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
	reply.WorkerID = c.workerIDCounter
	c.workerIDCounter++
	fmt.Println("Init Worker RPC called with WorkerID:", reply.WorkerID)
	return nil
}

// CoordinateTask is a helper function to assign tasks to workers
// first complete all map tasks, then reduce tasks
func CoordinateTask(c *Coordinator, reply *AssignTaskReply) {
	// if there are remaining map tasks, assign a map task
	if MapTaskID := FindFreeMapTaskID(c); MapTaskID != -1 {
		reply.TaskType = TaskMap
		reply.TaskFile = c.mapTasks[MapTaskID].FileName
		// generate intermediate file prefix
		reply.TaskID = MapTaskID
		reply.TaskNum = c.nReduce
		c.mu.Lock()
		defer c.mu.Unlock()
		c.mapTasks[MapTaskID].State = TaskStateInProgress
		c.mapTasks[MapTaskID].AssignTime = time.Now()
		fmt.Printf("Assigned Map task for file %v to WorkerID %v\n", reply.TaskFile, reply.WorkerID)
		return
	} else if c.numDoneMapTasks < c.mMap {
		// there are still map tasks in progress
		reply.TaskType = TaskWait
		fmt.Printf("Map tasks in progress. Instructing WorkerID %v to wait.\n", reply.WorkerID)
		return
	} else if ReduceTaskID := FindFreeReduceTaskID(c); ReduceTaskID != -1 {
		reply.TaskType = TaskReduce
		reply.TaskFile = "" // reduce tasks do not have specific input files
		reply.TaskID = ReduceTaskID
		reply.TaskNum = c.mMap
		c.mu.Lock()
		defer c.mu.Unlock()
		c.reduceTasks[ReduceTaskID].State = TaskStateInProgress
		c.reduceTasks[ReduceTaskID].AssignTime = time.Now()
		fmt.Printf("Assigned Reduce task for file %v to WorkerID %v\n", reply.TaskFile, reply.WorkerID)
		return
	} else if c.numDoneReduceTasks < c.nReduce {
		// there are still reduce tasks in progress
		reply.TaskType = TaskWait
		fmt.Printf("Reduce tasks in progress. Instructing WorkerID %v to wait.\n", reply.WorkerID)
		return
	} else if c.numDoneMapTasks == c.mMap && c.numDoneReduceTasks == c.nReduce {
		reply.TaskType = TaskExit
		fmt.Printf("All tasks offered. Instructing WorkerID %v to exit.\n", reply.WorkerID)
	}
}

func FindFreeMapTaskID(c *Coordinator) int {
	if c.numDoneMapTasks == c.mMap {
		return -1
	}
	for i, mapTask := range c.mapTasks {
		if mapTask.State == TaskStatePending {
			return i
		}
	}
	return -1
}

func FindFreeReduceTaskID(c *Coordinator) int {
	if c.numDoneReduceTasks == c.nReduce {
		return -1
	}
	for i, reduceTask := range c.reduceTasks {
		if reduceTask.State == TaskStatePending {
			return i
		}
	}
	return -1
}

func (c *Coordinator) AssignTask(arg *WorkerArgs, reply *AssignTaskReply) error {
	reply.WorkerID = arg.WorkerID

	CoordinateTask(c, reply)
	return nil
}

func (c *Coordinator) ReportTask(arg *WorkerArgs, reply *ReportTaskReply) error {
	// update the coordinator's state based on the worker's report
	c.mu.Lock()
	defer c.mu.Unlock()
	switch arg.CallType {
	case CallReportMap:
		c.numDoneMapTasks++
		c.mapTasks[arg.TaskID].State = TaskStateCompleted
		reply.Success = true
	case CallReportReduce:
		c.numDoneReduceTasks++
		c.reduceTasks[arg.TaskID].State = TaskStateCompleted
		reply.Success = true
	}
	return nil
}

func (c *Coordinator) CheckTaskTimeouts() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()

		//
		if c.numDoneMapTasks != c.mMap {
			// check for map task timeouts
			for i := range c.mapTasks {
				if c.mapTasks[i].State == TaskStateInProgress {
					if time.Since(c.mapTasks[i].AssignTime) > 10*time.Second {
						c.mapTasks[i].State = TaskStatePending
						c.mapTasks[i].AssignTime = time.Time{}
						fmt.Printf("Map task for file %v timed out. Resetting to pending.\n", c.mapTasks[i].FileName)
					}
				}
			}
		}
		if c.numDoneReduceTasks != c.nReduce {
			// check for reduce task timeouts
			for i := range c.reduceTasks {
				if c.reduceTasks[i].State == TaskStateInProgress {
					if time.Since(c.reduceTasks[i].AssignTime) > 3*time.Second {
						c.reduceTasks[i].State = TaskStatePending
						c.reduceTasks[i].AssignTime = time.Time{}
						fmt.Printf("Reduce task %v timed out. Resetting to pending.\n", i)
					}
				}
			}
		}

		c.mu.Unlock()
	}
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
	go c.CheckTaskTimeouts()
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
	c.workerIDCounter = 0
	c.mMap = len(files)
	c.mapTasks = make([]MapTask, len(files))
	for i := 0; i < len(files); i++ {
		c.mapTasks[i] = MapTask{FileName: files[i], State: TaskStatePending}
	}
	c.numDoneMapTasks = 0
	c.nReduce = nReduce
	c.reduceTasks = make([]ReduceTask, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = ReduceTask{State: TaskStatePending}
	}
	c.numDoneReduceTasks = 0

	c.server()
	return &c
}
