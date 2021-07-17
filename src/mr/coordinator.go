package mr

import (
	"6.824/consts"
	"6.824/models"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)


type Coordinator struct {
	mu				sync.Mutex
	tasks			[]models.T
	taskFinished	int
	// Your definitions here.
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) AcquireTask(req *models.AcquireTaskReq, resp *models.AcquireTaskResp) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = req
	if c.taskFinished == len(c.tasks) {
		resp.Task = models.T{}
		return nil
	}
	for i := 0; i < len(c.tasks); i++ {
		if c.tasks[i].Status == consts.TaskStatusIdle {
			c.tasks[i].Status = consts.TaskStatusRunning
			resp.Task = c.tasks[i]
			return nil
		}
	}

	resp.Task = models.T{}
	return nil
}

func (c *Coordinator) Finished(req *models.FinishedReq, resp *models.FinishedResp) error {
	c.mu.Lock()
	c.mu.Unlock()
	_ = resp
	for i := 0; i < len(c.tasks); i++ {
		if c.tasks[i].ID == req.ID {
			c.tasks[i].Status = consts.TaskStatusFinished
			break
		}
	}
	c.taskFinished++
	return nil
}


//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.taskFinished == len(c.tasks) {
		return true
	}
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	var lenOfParts int
	c := Coordinator{}

	if nReduce > len(files) {
		lenOfParts = 1
		nReduce = len(files)
	} else {
		lenOfParts = len(files) / nReduce
	}
	c.tasks = make([]models.T, nReduce)
	for i := 1; i <= nReduce; i++ {
		c.tasks[i - 1].ID = i - 1
		c.tasks[i - 1].FileName = files[(i - 1)*lenOfParts: i * lenOfParts]
		c.tasks[i - 1].Status = consts.TaskStatusIdle
	}
	// Your code here.


	c.server()
	return &c
}
