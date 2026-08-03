package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "net/rpc"

type Coordinator struct {
	noOfReduceTasks int

	mapTasks    []Job
	reduceTasks []Job

	phase CoordinatorPhase

	mu      sync.Mutex
	workers []WorkerType //to keep track of workers which are idle , active or crashed
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

func (c *Coordinator) TaskTransitionManager(t TaskType) {

	hasPendingTask := false
	switch t {
	case MapTask:
		for _, task := range c.mapTasks {
			if task.status == Pending || task.status == Waiting {
				hasPendingTask = true
				break
			}
		}

		if !hasPendingTask {

			for i := 0; i < c.noOfReduceTasks; i++ {
				reduceTask := Job{
					taskId:    fmt.Sprintf("task-%d", i),
					status:    Waiting,
					partition: i,
				}

				c.reduceTasks = append(c.reduceTasks, reduceTask)
			}

			//Transition to Reduce phase
			c.phase = ReducePhase
			return
		}
	case ReduceTask:
		count := 0
		for _, task := range c.reduceTasks {
			if task.status == DONE {
				count++
			}
		}

		if count == len(c.reduceTasks) {
			c.phase = FinishedPhase
			return
		}
	}
}

func (c *Coordinator) TaskUpdateByWorker(args *WorkerTaskUpdateArgs, reply *WorkerTaskUpdateReply) error {

	reply.Status = Invalid
	if args.TaskId == "" {

		reply.Message = "Task ID is required"
		return nil
	}

	if args.WorkerID == "" {

		reply.Message = "Worker ID is required"
		return nil
	}

	if args.Type == "" {

		reply.Message = "Provide task type ( MAP | REDUCE )"
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	var jobs []Job

	switch args.Type {
	case MapTask:
		jobs = c.mapTasks
	case ReduceTask:
		jobs = c.reduceTasks
	default:
		reply.Message = "Task type must be either MAP or REDUCE"
		return nil
	}

	for i := range jobs {
		job := &jobs[i]
		if job.taskId == args.TaskId {

			if job.worker.workerId != args.WorkerID {
				reply.Message = fmt.Sprintf("Task not assigned to worker %s", args.WorkerID)
				reply.Status = Invalid
				return nil
			}

			if job.status == DONE {
				reply.Message = "Task already completed"
				reply.Status = DONE
				return nil
			}

			job.status = DONE

			reply.Status = DONE
			reply.Message = "Task update received successfully"
			reply.Phase = c.phase

			c.TaskTransitionManager(args.Type)
			return nil
		}
	}

	reply.Message = "Task not found"
	return nil
}

func (c *Coordinator) assignMapTask(assignedWorker *WorkerType, reply *WorkerTaskRequestReply) error {
	for i := range c.mapTasks {

		task := &c.mapTasks[i]

		if task.status == Waiting {

			task.status = Pending
			task.worker = *assignedWorker

			reply.TaskAvailable = true
			reply.TaskId = task.taskId
			reply.FilePath = task.filePath
			reply.Status = task.status
			reply.NReduce = task.nReduce
			reply.Type = MapTask
			reply.Phase = c.phase
			break
		}
	}

	return nil
}

func (c *Coordinator) assignReduceTask(assignedWorker *WorkerType, reply *WorkerTaskRequestReply) error {

	for i := range c.reduceTasks {

		task := &c.reduceTasks[i]

		if task.status == Waiting {
			task.status = Pending
			task.worker = *assignedWorker

			reply.TaskAvailable = true
			reply.TaskId = task.taskId
			reply.Status = task.status
			reply.Type = ReduceTask
			reply.Phase = c.phase
			reply.Partition = task.partition

			return nil
		}

	}

	reply.Phase = c.phase
	reply.TaskAvailable = false

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

	var assignedWorker *WorkerType

	//check the worker if registered before
	for i := range c.workers {
		if c.workers[i].workerId == args.WorkerID {
			assignedWorker = &c.workers[i]
			break
		}
	}

	reply.TaskAvailable = false

	if assignedWorker == nil {
		fmt.Println("Couldn't find assigned worker ")
		return nil
	}

	switch c.phase {
	case MapPhase:
		return c.assignMapTask(assignedWorker, reply)
	case ReducePhase:
		return c.assignReduceTask(assignedWorker, reply)
	case FinishedPhase:

		reply.TaskAvailable = false
		reply.Phase = c.phase
		return nil
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

		currentPhase := c.Done()
		if currentPhase == FinishedPhase {
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

func (c *Coordinator) Done() CoordinatorPhase {

	return c.phase
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
	- Worker map and partition mapped values as per nReduce ✅
	- Worker updates task status to coordinator

*/

func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{}
	c.noOfReduceTasks = nReduce
	c.phase = MapPhase

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
