package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"plugin"
	"sort"
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

// send an RPC request to the coordinator, wait for the response.
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

	var result WorkerTaskRequestReply
	args := WorkerTaskRequestArgs{
		WorkerID: worker.ID,
		RpcAddr:  worker.RPCAddr,
		Port:     worker.Port,
	}

	ok := call("Coordinator.TaskRequestByWorker", &args, &result)
	if ok {

		fmt.Println("PHASE ", result.Phase)
		if result.Phase == MapPhase {

			//instruct map task
			MapperTask(result, worker)

		} else if result.Phase == ReducePhase {
			fmt.Println("WORKER FETCHED REDUCE TASKS")
			fmt.Println(" Reduce task : ", result.Partition)
			ReducerTask(result, worker)
		} else {
			fmt.Println("WORKER FINALIZED ALL")
		}

	} else {
		fmt.Printf("{ WorkerFetchTasks } : call failed!\n")
	}
}

func MapperTask(result WorkerTaskRequestReply, worker *Workers) {

	mapf, _ := loadPlugin(os.Args[1])

	intermediate := ByKey{}

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

	for partitionID := 0; partitionID < result.NReduce; partitionID++ {

		filename := fmt.Sprintf("mr-data/intermediate/mr-%s-%d", result.TaskId, partitionID)
		file, err := os.Create(filename)
		if err != nil {
			fmt.Println("cannot create file ", filename)
			log.Fatal(err)
		}

		encoder := json.NewEncoder(file)
		for _, kv := range partitions[partitionID] {
			if err := encoder.Encode(&kv); err != nil {
				fmt.Println("cannot encode ", kv)
				log.Fatal(err)
			}
		}

		file.Close()
	}

	var resultTaskUpdate WorkerTaskUpdateReply
	argsTaskUpdate := WorkerTaskUpdateArgs{
		WorkerID: worker.ID,
		TaskId:   result.TaskId,
		Type:     result.Type,
		Status:   DONE,
	}

	passed := call("Coordinator.TaskUpdateByWorker", &argsTaskUpdate, &resultTaskUpdate)
	if passed {
		fmt.Printf("{ WorkerTaskUpdate } : reply  %v\n", resultTaskUpdate)
		if resultTaskUpdate.Phase != FinishedPhase {
			go WorkerFetchTasks(worker)
		}
	}
}

func ReducerTask(result WorkerTaskRequestReply, worker *Workers) {

	_, reducef := loadPlugin(os.Args[1])

	//files read (mr-partition_num-*) and reduce to be implemented
	files, err := filepath.Glob(
		fmt.Sprintf("mr-data/intermediate/mr-task-*-%d", result.Partition),
	)

	if err != nil {
		log.Fatal(err)
	}

	var intermediate []KeyValue
	for _, filename := range files {
		fmt.Println("reading:", filename)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open intermediate file %v", filename)
		}

		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := decoder.Decode(&kv)
			if err != nil {
				break
			}

			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	filename := fmt.Sprintf(
		"mr-data/output/mr-out-%d",
		result.Partition,
	)

	file, err := os.Create(filename)

	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	i := 0
	for i < len(intermediate) {

		j := i + 1
		values := []string{
			intermediate[i].Value,
		}

		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			values = append(values, intermediate[j].Value)
			j++
		}

		// reduce here

		reduceResult := reducef(
			intermediate[i].Key,
			values,
		)

		fmt.Fprintf(
			file,
			"%v %v\n",
			intermediate[i].Key,
			reduceResult,
		)

		i = j
	}

}
func Worker() {

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
