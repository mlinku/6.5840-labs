package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// init worker RPC call for unique workerID
	CallInitWorker()

	state := 0 // 0: idle 1: map 2: reduce 3: exit

	for {
		if state == 0 {
			// idle state, call task from coordinator
			reply := CallTask()
			switch reply.TaskType {
			case TaskMap:
				// map task
				state = 1
				fmt.Printf("Worker %v: received Map task for file %v\n", reply.WorkerID, reply.TaskFile)
				MapTask(reply.TaskFile, mapf)
			}

		}

		// Your worker implementation here.

		// uncomment to send the Example RPC to the coordinator.
		// CallExample()
	}
}

func MapTask(filename string, mapf func(string, string) []KeyValue) {
	// read file content
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// call mapf
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	fmt.Printf("MapTask: read %v and produced %v key-value pairs\n", filename, len(intermediate))

	// partition intermediate key-value pairs
	// write to intermediate files
}

// call init worker for workID
func CallInitWorker() {
	args := WorkerArgs{}
	args.WorkerID = 0
	args.CallType = CallInit

	reply := InitWorkerReply{}

	ok := call("Coordinator.InitWorker", &args, &reply)
	if ok {
		fmt.Printf("Init Worker reply WorkerID %v\n", reply.WorkerID)
	} else {
		fmt.Printf("call failed!\n")
	}

}

// call task from coordinator
func CallTask() (reply AssignTaskReply) {
	args := WorkerArgs{}
	args.CallType = CallAssignTask

	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		fmt.Printf("Assign Task reply WorkerID %v TaskType %v TaskFile %v\n", reply.WorkerID, reply.TaskType, reply.TaskFile)

	} else {
		fmt.Printf("call failed!\n")
	}
	return

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
