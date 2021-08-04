package mr

import (
	"6.824/consts"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


type Coordinator struct {
	Mu           sync.Mutex
	Tasks        []T
	TaskFinished int
	N			 int
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

func (c *Coordinator) AcquireTask(req *AcquireTaskReq, resp *AcquireTaskResp) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	_ = req
	resp.N = c.N
	if c.TaskFinished == len(c.Tasks) {
		return nil
	}
	for i := 0; i < len(c.Tasks); i++ {
		if c.Tasks[i].Status == consts.TaskStatusIdle {
			c.Tasks[i].Status = consts.TaskStatusRunning
			resp.Task = c.Tasks[i]
			go CheckIfTimeout(c.Tasks[i].ID, c)
			return nil
		}
	}


	return nil
}

func CheckIfTimeout(id int, c *Coordinator) {
	t := findTasks(id, c)
	if t == nil {
		log.Fatalf("invalid task id = %d", id)
	}

	select {
	case <-time.After(time.Second * 10):
		c.Mu.Lock()
		defer c.Mu.Unlock()
		t.Status = consts.TaskStatusIdle
	case <-t.Finished:
		c.Mu.Lock()
		defer c.Mu.Unlock()
		t.Status = consts.TaskStatusFinished
	}
}

func findTasks(id int, c *Coordinator) *T {
	for i := 0; i < len(c.Tasks); i++ {
		if c.Tasks[i].ID == id {
			return &c.Tasks[i]
		}
	}
	return nil
}

func (c *Coordinator) Finished(req *FinishedReq, resp *FinishedResp) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	_ = resp

	t := findTasks(req.ID, c)
	t.Finished <- struct{}{}
	c.TaskFinished++
	return nil
}


//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	if c.TaskFinished == len(c.Tasks) {
		return true
	}
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce Tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	var lenOfParts int
	c := Coordinator{}
	c.N = nReduce

	if nReduce > len(files) {
		lenOfParts = 1
		nReduce = len(files)
	} else {
		lenOfParts = len(files) / nReduce
	}
	c.Tasks = make([]T, nReduce)
	for i := 1; i <= nReduce; i++ {
		c.Tasks[i - 1].ID = i - 1
		c.Tasks[i - 1].FileName = files[(i - 1)*lenOfParts: i * lenOfParts]
		c.Tasks[i - 1].Status = consts.TaskStatusIdle
	}
	// Your code here.


	c.server()
	return &c
}
