package mr

import "time"

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
	Invalid TaskStatus = "INVALID"
)

type TaskType string

const (
	MapTask    TaskType = "MAP"
	ReduceTask TaskType = "REDUCE"
)

type CoordinatorPhase string

const (
	MapPhase      CoordinatorPhase = "MAP-PHASE"
	ReducePhase   CoordinatorPhase = "REDUCE-PHASE"
	FinishedPhase CoordinatorPhase = "FINISHED-PHASE"
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

type Partition map[int][]KeyValue
