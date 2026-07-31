package mr

import (
	"fmt"
	"log"
	"time"
)
import "net"
import "net/rpc"

type TaskType string

const (
	Map    TaskType = "MAP"
	Reduce TaskType = "REDUCE"
)

type WorkerStatus string

const (
	StateIdle      WorkerStatus = "IDLE"
	StateConnected WorkerStatus = "CONNECTED"
	StatusError    WorkerStatus = "ERROR"
	StateRetrying  WorkerStatus = "RETRYING"
)

type TaskStatus string

const (
	Waiting TaskStatus = "WAITING"
	Pending TaskStatus = "PENDING"
	DONE    TaskStatus = "DONE"
)

type Job struct {
	taskId   string
	filePath string
	taskType TaskType
	status   TaskStatus
	worker   WorkerType
}

type WorkerType struct {
	workerId string
	status   WorkerStatus
	Address  string
	Port     int
	LastSeen time.Time
}

type Coordinator struct {
	isTaskDone bool         //indicates if all tasks are done
	workers    []WorkerType //to keep track of workers which are idle , active or crashed
	jobs       []Job        //will contain all running / scheduled tasks
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RegisterWorker(args *WorkerRegistrationArgs, reply *WorkerRegistrationReply) error {
	worker := WorkerType{
		workerId: args.WorkerID,
		status:   StateConnected,
		Address:  args.RpcAddr,
		Port:     args.Port,
		LastSeen: time.Now(),
	}

	c.workers = append(c.workers, worker)

	reply.RegistrationStatus = true
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {

	err := rpc.Register(c)
	if err != nil {
		log.Fatal("RPC register error:", err)
	}

	l, e := net.Listen("tcp", ":3000")
	if e != nil {
		log.Fatal("listen error:", e)
	}

	fmt.Println("Coordinator listening on port 3000")

	//for c.Done() != true {
	//	time.Sleep(time.Second)
	//	fmt.Println(c.workers, "workers ", " ---  ", len(c.workers))
	//}
	rpc.Accept(l)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	return c.isTaskDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.

/*

	ToDo

	- Prepare a worker registration
	- Prepare healthcheck / ping merchanism from coordinator to workers

*/

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	//tasks := []Job{}

	//for idx, file := range files {
	//	task := Job{taskId: fmt.Sprintf("task-%s", idx), taskType: Map, status: Waiting}
	//
	//	append(tasks, task)
	//}
	c.server()
	return &c
}
