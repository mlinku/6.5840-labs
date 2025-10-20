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
	// Your definitions here.

	// mantain a counter for worker IDs, initialize to 0
	workerIDCounter int
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

func (c *Coordinator) AssignTask(arg *WorkerArgs, reply *AssignTaskReply) error {
	// temp implementation for testing
	reply.WorkerID = arg.WorkerID
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
	c.workerIDCounter = 0
	// Your code here.

	c.server()
	return &c
}
