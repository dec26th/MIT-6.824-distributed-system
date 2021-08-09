package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"6.824/consts"
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
	ID		 int
	TaskType consts.TaskType
	KeyValue []KeyValue
}

type FinishedResp struct {}

type AcquireTaskReq struct {
	taskType consts.TaskType
}

type AcquireTaskResp struct {
	Task		T
	N			int
}

type T struct {
	ID       int
	TaskType int8
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
