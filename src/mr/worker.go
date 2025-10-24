package mr

import (
	"encoding/json"
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

	state := 0 // 0: idle 1: map 2: reduce 3: wait 4: exit

	for {
		if state == 0 {
			// idle state, call task from coordinator
			reply := CallTask()
			switch reply.TaskType {
			case TaskMap:
				// map task
				state = 1
				fmt.Printf("Worker %v: received Map task for file %v\n", reply.WorkerID, reply.TaskFile)
				go MapTask(&reply, mapf, &state)
			case TaskReduce:
				// reduce task
				state = 2
				fmt.Printf("Worker %v: received Reduce task for file %v\n", reply.WorkerID, reply.TaskFile)
				go ReduceTask(&reply, reducef, &state)
			case TaskWait:
				// wait task
				state = 3
				fmt.Printf("Worker %v: received Wait task\n", reply.WorkerID)
				time.Sleep(time.Second)
				state = 0
			case TaskExit:
				// exit task
				state = 4
				fmt.Printf("Worker %v: received Exit task, exiting\n", reply.WorkerID)
				return
			default:
				fmt.Printf("Worker %v: received unknown task type %v\n", reply.WorkerID, reply.TaskType)
			}

		}

		// Your worker implementation here.

		// uncomment to send the Example RPC to the coordinator.
		// CallExample()
	}
}

func MapTask(reply *AssignTaskReply, mapf func(string, string) []KeyValue, state *int) {
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

	NReduce := reply.TaskNum
	fmt.Printf("MapTask: NReduce is %v\n", NReduce)

	// write intermediate key-value pairs to intermediate files
	for _, kv := range intermediate {
		reduceTaskNum := ihash(kv.Key) % NReduce
		intermediateFileName := fmt.Sprintf("mr-%d-%d", reply.TaskID, reduceTaskNum)
		intermediateFile, err := os.OpenFile(intermediateFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("cannot open intermediate file %v", intermediateFileName)
		}
		enc := json.NewEncoder(intermediateFile)
		err = enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode key-value pair %v", kv)
		}
		intermediateFile.Close()
	}
	fmt.Printf("MapTask: Worker %v completed Map task for file %v\n", reply.WorkerID, reply.TaskFile)
	*state = 0
	CallReportMapTask(reply.WorkerID, reply.TaskID)
}

func ReduceTask(reply *AssignTaskReply, reducef func(string, []string) string, state *int) {
	intermediate := []KeyValue{}

	ReduceID := reply.TaskID
	NMap := reply.TaskNum

	// read intermediate files
	for m := 0; m < NMap; m++ {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", m, ReduceID)
		intermediateFile, err := os.Open(intermediateFileName)
		if err != nil {
			log.Fatalf("cannot open intermediate file %v", intermediateFileName)
		}
		dec := json.NewDecoder(intermediateFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	fmt.Printf("ReduceTask: Worker %v read %v key-value pairs for Reduce task %v\n", reply.WorkerID, len(intermediate), ReduceID)

	// sort intermediate key-value pairs by key
	kvMap := make(map[string][]string)
	for _, kv := range intermediate {
		kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
	}

	// write output file
	outputFileName := fmt.Sprintf("mr-out-%d", ReduceID)
	outputFile, err := os.Create(outputFileName)
	if err != nil {
		log.Fatalf("cannot create output file %v", outputFileName)
	}
	for key, values := range kvMap {
		output := reducef(key, values)
		fmt.Fprintf(outputFile, "%v %v\n", key, output)
	}
	outputFile.Close()
	fmt.Printf("ReduceTask: Worker %v completed Reduce task %v\n", reply.WorkerID, ReduceID)
	*state = 0
	CallReportReduceTask(reply.WorkerID, reply.TaskID)
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
	args.CallType = CallAssign

	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		fmt.Printf("Assign Task reply WorkerID %v TaskType %v TaskFile %v\n", reply.WorkerID, reply.TaskType, reply.TaskFile)

	} else {
		fmt.Printf("call failed!\n")
	}
	return

}

func CallReportMapTask(workerID int, taskID int) {
	args := WorkerArgs{}
	args.WorkerID = workerID
	args.CallType = CallReportMap
	args.TaskID = taskID

	reply := ReportTaskReply{}

	ok := call("Coordinator.ReportTask", &args, &reply)
	if ok {
		fmt.Printf("Report Task reply Success %v\n", reply.Success)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallReportReduceTask(workerID int, taskID int) {
	args := WorkerArgs{}
	args.WorkerID = workerID
	args.CallType = CallReportReduce
	args.TaskID = taskID

	reply := ReportTaskReply{}

	ok := call("Coordinator.ReportTask", &args, &reply)
	if ok {
		fmt.Printf("Report Task reply Success %v\n", reply.Success)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.

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
