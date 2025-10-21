package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// mantain a counter for worker IDs, initialize to 0
	WorkerIDCounter int
	// input files for map tasks
	MapFiles []string
	// map task ID, used to assign unique IDs to map tasks and track progress
	MapTaskID int
	// number of reduce workers
	NReduce int
	// intermediate files for reduce tasks
	ReduceFiles []string
	// reduce task ID, used to assign unique IDs to reduce tasks and track progress
	ReduceTaskID int
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
	if c.MapTaskID < len(c.MapFiles) {
		reply.TaskType = TaskMap
		reply.TaskFile = c.MapFiles[c.MapTaskID]
		// generate intermediate file prefix
		reply.GenerateFile = fmt.Sprintf("mr-%d-", c.MapTaskID)
		c.MapTaskID++
		fmt.Printf("Assigned Map task for file %v to WorkerID %v\n", reply.TaskFile, reply.WorkerID)
		return
	}
}
func (c *Coordinator) AssignTask(arg *WorkerArgs, reply *AssignTaskReply) error {
	reply.WorkerID = arg.WorkerID

	CoordinateTask(c, reply)

	reply.TaskType = TaskMap
	reply.TaskFile = "pg-being_ernest.txt"
	fmt.Printf("Assign Task RPC called for WorkerID %v: assigned Map task for file %v\n", reply.WorkerID, reply.TaskFile)
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
	// Your code here.

	c.server()
	return &c
}
