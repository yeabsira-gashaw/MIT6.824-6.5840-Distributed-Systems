package mr

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"plugin"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type Workers struct {
	ID      string
	RPCAddr string
	Port    string
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}

func (w *Workers) Ping(args *PingArgs, reply *PingReply) error {

	reply.PingStatus = args.WorkerId == w.ID
	return nil
}

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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("tcp", "localhost:3000")
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

func WorkerRegistrationRequest(worker *Workers) {

	var result WorkerRegistrationReply
	args := WorkerRegistrationArgs{
		WorkerID: worker.ID,
		RpcAddr:  worker.RPCAddr,
		Port:     worker.Port,
	}

	ok := call("Coordinator.RegisterWorker", &args, &result)
	if ok {
		fmt.Printf("{ WorkerRegistrationRequest } : reply  %v\n", result)
		go WorkerFetchTasks(worker)
	} else {
		fmt.Printf("{ WorkerRegistrationRequest } : call failed!\n")
	}
}

func WorkerFetchTasks(worker *Workers) {

	mapf, _ := loadPlugin(os.Args[1])

	var result WorkerTaskRequestReply
	args := WorkerTaskRequestArgs{
		WorkerID: worker.ID,
		RpcAddr:  worker.RPCAddr,
		Port:     worker.Port,
	}

	intermediate := ByKey{}

	ok := call("Coordinator.TaskRequestByWorker", &args, &result)
	if ok {
		fmt.Printf("{ WorkerFetchTasks } : reply  %v\n", result)

		file, err := os.Open(result.FilePath)
		if err != nil {
			log.Fatalf("cannot open %v", result.FilePath)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", result.FilePath)
		}
		file.Close()
		kva := mapf(result.FilePath, string(content))

		intermediate = append(intermediate, kva...)

		partitions := Partition{}

		for _, kv := range intermediate {
			partitionValue := ihash(kv.Key) % result.NReduce
			partitions[partitionValue] = append(partitions[partitionValue], kv)
		}

		fmt.Printf("{ WorkerFetchTasks ---  } : intermediate %v\n", intermediate)

		var resultTaskUpdate WorkerTaskUpdateReply
		argsTaskUpdate := WorkerTaskUpdateArgs{
			WorkerID:              worker.ID,
			TaskId:                result.TaskId,
			Type:                  result.Type,
			Status:                DONE,
			IntermediatePartition: partitions,
		}

		passed := call("Coordinator.TaskUpdateByWorker", &argsTaskUpdate, &resultTaskUpdate)
		if passed {
			fmt.Printf("{ WorkerTaskUpdate } : reply  %v\n", resultTaskUpdate)
		}
		//next update the task status to coordinator alongside the partition
	} else {
		fmt.Printf("{ WorkerFetchTasks } : call failed!\n")
	}
}

func MakeWorker() {

	worker := new(Workers)
	err := rpc.Register(worker)
	if err != nil {
		log.Fatal("RPC register error:", err)
	}

	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal("listen error:", err)
	}

	defer listener.Close()

	addr := listener.Addr().(*net.TCPAddr)
	fmt.Printf("Worker listening on %s:%d\n", addr.IP, addr.Port)

	worker.ID = fmt.Sprintf("worker-%d", time.Now().UnixMicro())
	worker.RPCAddr = addr.IP.String()
	worker.Port = fmt.Sprintf("%d", addr.Port)

	go WorkerRegistrationRequest(worker)

	rpc.Accept(listener) //allow incoming requests
}
