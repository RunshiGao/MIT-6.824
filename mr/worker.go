package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		fmt.Println("Asking for a job")
		args := JobArgs{}
		reply := JobReply{}
		ok := call("Coordinator.JobHandler", &args, &reply)
		if !ok {
			return
		}

		switch reply.JobType {
		case 0: // map task
			fmt.Println("got a map task")
			doMapTask(mapf, reply)
		case 1: // reduce task
			fmt.Println("got a reduce task")
			doReduceTask(reducef, reply)
		case 3:
			fmt.Println("didn't get a job, wait...")
		case 2:
			fmt.Println("got a exit task")
			return
		default:
			fmt.Println("unexpected job")
		}
		time.Sleep(time.Second)
	}
}
func doMapTask(mapf func(string, string) []KeyValue, reply JobReply) {
	filename := reply.File
	nReduce := reply.NReduce
	fmt.Printf("filename:%v\n", filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	// intermediate := []KeyValue{}
	// intermediate = append(intermediate, kva...)
	buckets := make([][]KeyValue, nReduce)
	for i := range buckets {
		buckets[i] = []KeyValue{}
	}
	for _, kv := range kva {
		buckets[ihash(kv.Key)%nReduce] = append(buckets[ihash(kv.Key)%nReduce], kv)
	}
	for i := range buckets {
		bucket := buckets[i]
		oname := "mr-inter-" + strconv.Itoa(reply.JobID) + "-" + strconv.Itoa(i)
		f, _ := ioutil.TempFile("", oname+"*")
		enc := json.NewEncoder(f)
		for _, kv := range bucket {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write to %v", oname)
			}
		}
		// fmt.Println(f.Name())
		os.Rename(f.Name(), oname)
		f.Close()
	}
	fmt.Println("file ok")
	args := ReportArgs{reply.JobID, reply.JobType, -1}
	r := ReportReply{}
	ok := call("Coordinator.ReportHandler", &args, &r)
	if !ok {
		fmt.Println("Report failed")
	} else {
		fmt.Println("Report ok")
	}
}
func doReduceTask(reducef func(string, []string) string, reply JobReply) {
	// first read from intermediate files
	intermediate := []KeyValue{}
	for i := 0; i < reply.Nmap; i++ {
		iname := "mr-inter-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.JobID)
		file, err := os.Open(iname)
		if err != nil {
			fmt.Printf("Cannot open intermediate %v\n", iname)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(reply.JobID)
	ofile, _ := ioutil.TempFile("", oname+"*")
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
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	os.Rename(ofile.Name(), oname)
	ofile.Close()
	for i := 0; i < reply.Nmap; i++ {
		iname := "mr-inter-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.JobID)
		err := os.Remove(iname)
		if err != nil {
			fmt.Printf("cannot open delete %v \n", iname)
		}
	}
	args := ReportArgs{reply.JobID, reply.JobType, -1}
	r := ReportReply{}
	ok := call("Coordinator.ReportHandler", &args, &r)
	if !ok {
		fmt.Println("Report failed")
	} else {
		fmt.Println("Report ok")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
