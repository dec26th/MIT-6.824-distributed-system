package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type FinishedReq struct {
	ID	int
}

type FinishedResp struct {}

type AcquireTaskReq struct {}

type AcquireTaskResp struct {
	Task		T
	N			int
}

type T struct {
	ID       int
	FileName []string
	Status   int8
	Finished chan struct{}
	Tries	int8
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
