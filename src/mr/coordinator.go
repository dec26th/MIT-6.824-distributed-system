package mr

import (
	"6.824/consts"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)


type Coordinator struct {
	Mu           sync.Mutex
	Tasks        map[consts.TaskType][]*Task
	TaskFinished map[consts.TaskType]int
	N			 int
	// Your definitions here.
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

type Content []string

func (k Content)Len() int {return len(k)}
func (k Content)Swap(i, j int) {k[i], k[j] = k[j], k[i]}
func (k Content) Less(i, j int) bool {return strings.Split(k[i], " ")[0] < strings.Split(k[j], " ")[0]}

type ContentIndexer []string
func (c ContentIndexer)Len() int {return len(c)}
func (c ContentIndexer)Swap(i, j int) {c[i], c[j] = c[j], c[i]}
func (c ContentIndexer) Less(i, j int) bool {return c[i] < c[j]}


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

	if !c.isAvailableReq(req) {
		if c.AllTaskFinished() {
			resp.Status = consts.CoordinatorTypeNoTask
		}
		resp.Task = Task{}
		resp.N = c.N
		return nil
	}

	task := c.GetIdleTask(req.TaskType)
	if task == nil {
		fmt.Println("No valid task")
		resp.Task = Task{}
		resp.N = c.N
		return nil
	}

	resp.N = c.N
	resp.Task = GetTask(task)
	go c.CheckIfTimeout(task.ID, req.TaskType)
	return nil
}

func (c *Coordinator) isAvailableReq(req *AcquireTaskReq) bool {
	return !(c.AllTaskFinished() || (req.TaskType == consts.TaskTypeReduce && !c.TaskTypeFinished(consts.TaskTypeMap)))
}

func GetTask(task *Task) Task {
	if task != nil {
		return *task
	}
	return Task{}
}

func (c *Coordinator) GetIdleTask(taskType consts.TaskType) *Task {
	fmt.Println("[c.GetIdleTask] Begin to acquire task: ", taskType)
	tasks := c.Tasks[taskType]
	for i := 0; i < len(tasks); i++ {
		if isValidTask(tasks[i]) {
			fmt.Println("[c.GetIdleTask] Start to process task:", tasks[i].ID)
			fmt.Println("[c.GetIdleTask] Filenames:", tasks[i].FileName)
			tasks[i].Status = consts.TaskStatusRunning
			return tasks[i]
		}
	}
	return nil
}

func isValidTask(task *Task) bool {
	return len(task.FileName) > 0 && task.Status == consts.TaskStatusIdle
}

func (c *Coordinator) CheckIfTimeout(id int, taskType consts.TaskType) {
	fmt.Println("[c.CheckIfTimeout] start to check if task ", id, "time out")
	t := findTasks(id, taskType, c)
	if t == nil {
		log.Fatalf("[c.CheckIfTimeout] invalid task id = %d", id)
	}

	select {
	case <-time.After(time.Second * 10):
		fmt.Println("[c.CheckIfTimeout] task: ", id, "TaskType: ", taskType, ",time out")
		c.Mu.Lock()
		t.Status = consts.TaskStatusIdle
		c.Mu.Unlock()
	case <-t.Finished:
		fmt.Println("[c.CheckIfTimeout] task: ", id, "TaskType: ", taskType, ",finished")
		c.Mu.Lock()
		t.Status = consts.TaskStatusFinished
		c.Mu.Unlock()
	}
}

func findTasks(id int, taskType consts.TaskType,c *Coordinator) *Task {
	return c.Tasks[taskType][id]
}

func combineFile() {
	fmt.Println("Begin to combineWordCount files")
	pwd, _ := os.Getwd()
	fmt.Println("pwd: ", pwd)
	files, _ := ioutil.ReadDir("./")
	fmt.Println("files: ", files)
	fileList := make([]string, 0, len(files))
	for _, file := range files {
		if strings.Contains(file.Name(), "mr-out") {
			fileList = append(fileList, path.Join(pwd, file.Name()))
		}
	}
	fmt.Println("FileList: ", fileList)

	for _, fileName := range fileList{
		fmt.Println("Ready to combine file, ", fileName)
		f, _ := os.Open(fileName)
		stat, _ := f.Stat()

		result := make([]byte, stat.Size())
		_, err := f.Read(result)
		f.Close()
		os.Remove(fileName)
		if err != nil {
			fmt.Println("Read failed, err =", err)
			return
		}
		content := string(result)
		split := strings.Split(content, "\n")
		sort.Sort(Content(split))

		f, _ = os.OpenFile(fileName, os.O_CREATE | os.O_WRONLY, 0666)
		switch len(strings.Split(split[1], " ")) {
		case 2:
			split = combineWordCount(split[1:])
		case 3:
			split = combineIndexer(split[1:])
		default:
			fmt.Println("unknown format", split[1])
			return
		}
		fmt.Fprintf(f, strings.Join(split, "\n"))
		f.Close()
	}
}

func combineIndexer(raw []string) []string {
	fmt.Println("combine indexer")
	result := make([]string, 0, len(raw))

	for i := 0; i < len(raw); {
		key := strings.Split(raw[i], " ")[0]
		values := []string{strings.Split(raw[i], " ")[2]}
		j := i + 1
		for j < len(raw) && key == strings.Split(raw[j], " ")[0] {
			values = append(values, strings.Split(raw[j], " ")[2])
			j++
		}

		sort.Sort(ContentIndexer(values))
		result = append(result, fmt.Sprintf("%v %v %s", key, len(values), strings.Join(values, ",")))
		i = j
	}
	return result
}

func combineWordCount(raw []string) []string {
	fmt.Println("combine word count")
	result := make([]string, 0, len(raw))

	for i := 0; i < len(raw); {
		key := strings.Split(raw[i], " ")[0]
		value := strings.Split(raw[i], " ")[1]
		num, _ := strconv.Atoi(value)
		j := i + 1
		for j < len(raw) && key == strings.Split(raw[j], " ")[0] {
			temp, _ := strconv.Atoi(strings.Split(raw[j], " ")[1])
			num += temp
			j++
		}

		result = append(result, fmt.Sprintf("%v %d", key, num))
		i = j
	}
	return result
}


func (c *Coordinator) Finished(req *FinishedReq, resp *FinishedResp) error {
	fmt.Println("[c.Finished] finished called by ", req.ID)
	_ = resp

	t := findTasks(req.ID, req.TaskType, c)
	fmt.Println("[c.Finished] start to send signal to task", req.ID)
	t.Finished <- struct{}{}

	c.Mu.Lock()
	c.TaskFinished[req.TaskType] += 1
	c.Mu.Unlock()

	c.TryCrateMapTask(req)
	return nil
}

func (c *Coordinator) TryCrateMapTask(req *FinishedReq) {
	if req.TaskType == consts.TaskTypeMap && len(req.Filename) > 0 {
		c.Mu.Lock()
		fmt.Println("[c.TryCrateMapTask] task id: ", req.ID, "filename list: ", req.Filename)
		for _, name := range req.Filename {
			reduceIDStr := strings.Split(name, "-")[2]
			id, _ := strconv.Atoi(reduceIDStr)
			c.Tasks[consts.TaskTypeReduce][id].FileName = append(c.Tasks[consts.TaskTypeReduce][id].FileName, name)
		}
		c.Mu.Unlock()
	}
}


//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	return c.AllTaskFinished()
}

func (c *Coordinator) AllTaskFinished() bool {
	return c.TaskTypeFinished(consts.TaskTypeReduce) && c.TaskTypeFinished(consts.TaskTypeMap)
}

func (c *Coordinator) TaskTypeFinished(taskType consts.TaskType) bool {
	return c.TaskFinished[taskType] == len(c.Tasks[taskType])
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

	c.Tasks = make(map[consts.TaskType][]*Task, 2)
	c.TaskFinished = make(map[consts.TaskType]int, 2)
	c.TaskFinished[consts.TaskTypeMap] = 0
	c.TaskFinished[consts.TaskTypeReduce] = 0
	c.Tasks[consts.TaskTypeReduce] = make([]*Task, nReduce)
	for i := 0; i < nReduce; i++ {
		c.Tasks[consts.TaskTypeReduce][i] = &Task{
			ID: i,
			Status: consts.TaskStatusIdle,
			Finished: make(chan struct{}),
			TaskType: consts.TaskTypeReduce,
		}
	}

	if nReduce > len(files) {
		lenOfParts = 1
		nReduce = len(files)
	} else {
		lenOfParts = len(files) / nReduce
	}
	c.Tasks[consts.TaskTypeMap] = make([]*Task, nReduce)
	for i := 0; i < nReduce; i++ {
		c.Tasks[consts.TaskTypeMap][i] = &Task{
			ID: i,
			FileName: files[(i)*lenOfParts: (i + 1) * lenOfParts],
			Status: consts.TaskStatusIdle,
			Finished: make(chan struct{}),
			TaskType: consts.TaskTypeMap,
		}
	}
	// Your code here.


	c.server()
	return &c
}
