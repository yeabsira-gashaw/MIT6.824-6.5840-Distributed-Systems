package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "net/rpc"

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
	taskId     string
	filePath   string
	status     TaskStatus
	worker     WorkerType
	assignedAt time.Time
	nReduce    int
}

type WorkerType struct {
	workerId        string
	status          WorkerStatus
	Address         string
	Port            string
	LastSeen        time.Time
	HeartBeatChecks int
}

type Coordinator struct {
	noOfReduceTasks int

	mapTasks    []Job
	reduceTasks []Job

	completedMap    int
	completedReduce int

	mu         sync.Mutex
	isTaskDone bool         //indicates if all tasks are done
	workers    []WorkerType //to keep track of workers which are idle , active or crashed
}

func (c *Coordinator) RegisterWorker(args *WorkerRegistrationArgs, reply *WorkerRegistrationReply) error {
	worker := WorkerType{
		workerId:        args.WorkerID,
		status:          StateConnected,
		Address:         args.RpcAddr,
		Port:            args.Port,
		LastSeen:        time.Now(),
		HeartBeatChecks: 0,
	}

	c.workers = append(c.workers, worker)

	reply.RegistrationStatus = true
	return nil
}

func (c *Coordinator) TaskRequestByWorker(args *WorkerTaskRequestArgs, reply *WorkerTaskRequestReply) error {

	/*
		This lock guarantees that while one worker is:

		1. Looking for a waiting task,
		2. Changing it to Pending,
		3. Recording ownership,

		- no other worker can enter this same section of code.

	*/
	c.mu.Lock()
	defer c.mu.Unlock()

	var assignedWorker WorkerType
	found := false

	//check the worker if registered before
	for _, worker := range c.workers {
		if worker.workerId == args.WorkerID {
			assignedWorker = worker
			found = true
			break
		}
	}

	reply.TaskAvailable = false

	if !found {
		return nil
	}

	for i := range c.mapTasks {

		task := &c.mapTasks[i]

		if task.status == Waiting {

			task.status = Pending
			task.worker = assignedWorker

			reply.TaskAvailable = true
			reply.TaskId = task.taskId
			reply.FilePath = task.filePath
			reply.Status = task.status
			reply.nReduce = task.nReduce
			return nil
		}
	}

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
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go rpc.ServeConn(conn)
		}
	}()

	go c.Healthcheck()

}

func (c *Coordinator) Healthcheck() {

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {

		if c.Done() {
			fmt.Println("All tasks are done")
			return
		}

		c.mu.Lock()

		workers := make([]WorkerType, len(c.workers))
		copy(workers, c.workers)
		c.mu.Unlock()

		for idx, worker := range workers {
			go c.WorkerSync("Workers.Ping", worker.workerId, worker.Port, idx)
		}
	}
}

func (c *Coordinator) Done() bool {

	return c.isTaskDone
}

func (c *Coordinator) ActiveWorkers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for _, worker := range c.workers {
		if worker.status == StateConnected {
			count++
		}
	}

	return count
}

func (c *Coordinator) WorkerSync(rpcname string, workerId string, port string, workerIdx int) {

	workerAddress := fmt.Sprintf("localhost:%d", port)
	client, err := rpc.Dial("tcp", workerAddress)
	if err != nil {
		c.workers[workerIdx].status = StateRetrying
		return
	}
	defer client.Close()

	var result PingReply
	args := PingArgs{
		WorkerId: workerId,
	}

	err = client.Call(rpcname, args, &result)
	c.mu.Lock()
	if err != nil {
		fmt.Println("Ping error ", err)
		if c.workers[workerIdx].HeartBeatChecks == 3 {

			c.workers[workerIdx].status = StatusError
		} else {

			c.workers[workerIdx].HeartBeatChecks += 1
			c.workers[workerIdx].LastSeen = time.Now()
			c.workers[workerIdx].status = StateRetrying
		}
	} else {

		fmt.Println("Worker Sync Result :: ", result.PingStatus)

		//intentional failure here - for simulating worker failures
		//if c.workers[workerIdx].HeartBeatChecks == 5 && c.workers[workerIdx].status == StateConnected {
		//
		//	c.workers[workerIdx].status = StatusError
		//	c.workers[workerIdx].HeartBeatChecks = 0
		//	fmt.Println("Worker is not responding for 5 consecutive checks")
		//	return
		//}

		c.workers[workerIdx].HeartBeatChecks += 1
		c.workers[workerIdx].LastSeen = time.Now()
		c.workers[workerIdx].status = StateConnected
	}

	defer c.mu.Unlock()

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.

/*

	ToDo

	- Prepare a worker registration ✅
	- Prepare healthcheck / ping mechanism from coordinator to workers ✅
	- Worker fetches task from Coordinator ✅

*/

func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{}
	c.noOfReduceTasks = nReduce

	for idx, file := range files {
		task := Job{
			taskId:   fmt.Sprintf("task-%d", idx),
			status:   Waiting,
			filePath: file,
			nReduce:  nReduce,
		}

		c.mapTasks = append(c.mapTasks, task)
	}

	go c.server()
	return &c
}
