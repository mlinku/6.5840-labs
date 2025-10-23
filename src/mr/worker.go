package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"time"
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
				MapTask(&reply, mapf)
			}

		}

		// Your worker implementation here.

		// uncomment to send the Example RPC to the coordinator.
		// CallExample()
	}
}

func MapTask(reply *AssignTaskReply, mapf func(string, string) []KeyValue) {
	// read file content
	intermediate := []KeyValue{}
	file, err := os.Open(reply.TaskFile)
	if err != nil {
		log.Fatalf("cannot open %v", reply.TaskFile)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.TaskFile)
	}
	file.Close()

	// call mapf
	kva := mapf(reply.TaskFile, string(content))

	intermediate = append(intermediate, kva...)
	fmt.Printf("MapTask: read %v and produced %v key-value pairs\n", reply.TaskFile, len(intermediate))

	fmt.Printf("MapTask: generated intermediate files %v\n", reply.TaskFile)
	fmt.Printf("MapTask: generated intermediate files %v\n", reply.GenerateFile)
	time.Sleep(100 * time.Second)
	NReduce := int(reply.GenerateFile[len(reply.GenerateFile)-1] - '0') // extract NReduce from filename

	// write intermediate key-value pairs to intermediate files
	for _, kv := range intermediate {
		reduceTaskNum := ihash(kv.Key) % NReduce
		intermediateFileName := fmt.Sprintf("mr-%d-%d", reply.WorkerID, reduceTaskNum)
		intermediateFile, err := os.OpenFile(intermediateFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("cannot open intermediate file %v", intermediateFileName)
		}
		fmt.Fprintf(intermediateFile, "%v %v\n", kv.Key, kv.Value)
		intermediateFile.Close()
	}
	fmt.Printf("MapTask: Worker %v completed Map task for file %v\n", reply.WorkerID, reply.TaskFile)
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
