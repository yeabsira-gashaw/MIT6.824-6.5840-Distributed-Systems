package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// PingArgs - Coordinator to Worker ping check
type PingArgs struct {
	WorkerId string
}
type PingReply struct {
	PingStatus bool
}

type WorkerRegistrationArgs struct {
	WorkerID string
	RpcAddr  string
	Port     int
}
type WorkerRegistrationReply struct {
	RegistrationStatus bool
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
