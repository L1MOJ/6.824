package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "sort"
import "strconv"
import "encoding/json"
import "time"

// DO I have to modify the capitalization of the functions

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

const OutputNameTemplate string = "mr-out-%d"
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// 定义全局变量保存mapf和reducef
var storedMapf func(string, string) []KeyValue
var storedReducef func(string, []string) string
var exitState bool = false
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Save mapf and reducef 到全局变量以便其他函数使用
	storedMapf = mapf
	storedReducef = reducef

	// Your worker implementation here.
	// if exitState == false, exit
	if exitState == true {
		return
	}

	for {
		// 1. CallRequestTask to get a task from master
		task := callRequestTask()
		// 2. Handle different tasks
		taskHandler(&task)
	}

}

// make RPC call to master to report task status
func callReportTask(task *Task) {
	args := *task
	reply := ReportReplyArgs{}

	call("Master.ReportTask", &args, &reply)
}


// make RPC call to master to request for task
func callRequestTask()  Task{
	args := RequestTaskArgs{1}
	reply := Task{}
	call("Master.RequestTask", &args, &reply)
	// fmt.Printf("Get Task type: %v, id: %v NReduce: %v\n", reply.Type, reply.TaskId, reply.NReduce)
	return reply
}

// taskHandler to handle different tasks
func taskHandler(task *Task) {
	switch task.Type {
	case MapTask:
		handleMapTask(task)
	case ReduceTask:
		handleReduceTask(task)
	case WaitTask:
		handleWaitTask(task)
	case ExitTask:
		handleExitTask(task)
	}
}

// read intermediate file
func readIntermediateFile(filename string) []KeyValue {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	dec := json.NewDecoder(file)
	kva := []KeyValue{}
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	file.Close()
	return kva
}

// handle exit task
func handleExitTask(task *Task) {
	// fmt.Printf("Worker exiting...\n")
	exitState = true
}

// handle wait task
func handleWaitTask(task *Task) {
	fmt.Printf("Waiting...\n")
	time.Sleep(2 * time.Second)
}

// handle reduce task
func handleReduceTask(task *Task) {
	fmt.Printf("Handling reduce task %v\n", task.TaskId)
	// 1. Read all intermediate files
	intermediate := []KeyValue{}

	for _, fileName := range task.Files {
		intermediate = append(intermediate, readIntermediateFile(fileName)...)
	}
	// sort intermediates
	sort.Sort(ByKey(intermediate))
	ofile, err := ioutil.TempFile(".", "mrtmp-reduce")
	if err != nil {
		log.Panic("[Worker] Failed to create reduce tmp file.", err)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := storedReducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	reduceFileName := fmt.Sprintf(OutputNameTemplate, task.TaskId)
	os.Rename(ofile.Name(), reduceFileName)

	callReportTask(task)
}

// handle map task
func handleMapTask(task *Task) {
	fmt.Printf("Handling map task %v\n", task.TaskId)
	filename := task.Files[0]
	nReduce := task.NReduce
	mapTaskId := task.TaskId

	// Map task only contain one file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := storedMapf(filename, string(content))
	// create nReduce slices to hold kv pairs with different hash value
	nReduceSlices := make([][]KeyValue, nReduce)
	

	// traverse all the kva slices, put them into corresponding nReduce slices
	for _, kv := range kva {
		nReduceIndex := ihash(kv.Key) % nReduce
		nReduceSlices[nReduceIndex] = append(nReduceSlices[nReduceIndex], kv)
	}

	// sort all nReduce slices and  write all nReduce slices to corresponding intermediate files in json format
	for i, slice := range nReduceSlices {
		sort.Sort(ByKey(slice))
		intermediateFile := "mr-" + strconv.Itoa(mapTaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(intermediateFile)
		enc:=json.NewEncoder(ofile)
		for _, kv := range slice {
			enc.Encode(&kv)
		}
	}

	callReportTask(task)
}


//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
