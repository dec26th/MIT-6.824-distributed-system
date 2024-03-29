package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"

	"6.824/consts"
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
	ID       int
	TaskType consts.TaskType
	Filename []string
}

type FinishedResp struct{}

type AcquireTaskReq struct {
	TaskType consts.TaskType
}

type AcquireTaskResp struct {
	Task   Task
	N      int
	Status consts.CoordinatorType
}

type Task struct {
	ID       int
	TaskType consts.TaskType
	FileName []string
	Status   int8
	Finished chan struct{}
}

type ExitReq struct{}

type ExitResp struct{}

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
