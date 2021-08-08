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
	Tasks        []T
	TaskFinished int
	N			 int
	Combine		 bool
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
	_ = req
	resp.N = c.N
	if c.TaskFinished == len(c.Tasks) + 1 || c.TaskFinished == len(c.Tasks) {
		fmt.Println("hi there")
		if c.Combine == false {
			combineFile()
			c.TaskFinished ++
			c.Combine = true
		}
		return nil
	}
	for i := 0; i < len(c.Tasks); i++ {
		if c.Tasks[i].Status == consts.TaskStatusIdle {
			fmt.Println("Start to process task:", c.Tasks[i].ID)
			fmt.Println("Filenames:", c.Tasks[i].FileName)
			c.Tasks[i].Status = consts.TaskStatusRunning
			resp.Task = c.Tasks[i]
			go CheckIfTimeout(c.Tasks[i].ID, c)
			return nil
		}
	}


	return nil
}

func CheckIfTimeout(id int, c *Coordinator) {
	fmt.Println("start to check if task ", id, "time out")
	t := findTasks(id, c)
	if t == nil {
		log.Fatalf("invalid task id = %d", id)
	}

	select {
	case <-time.After(time.Second * 10):
		fmt.Println("task: ", id, ",time out")
		c.Mu.Lock()
		t.Status = consts.TaskStatusIdle
		c.Mu.Unlock()
	case <-t.Finished:
		fmt.Println("task: ", id, ",finished")
		c.Mu.Lock()
		t.Status = consts.TaskStatusFinished
		c.Mu.Unlock()
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
	fmt.Println("finished called by ", req.ID)
	_ = resp

	t := findTasks(req.ID, c)
	fmt.Println("start to send signal to task", req.ID)
	t.Finished <- struct{}{}

	c.Mu.Lock()
	c.TaskFinished++
	c.Mu.Unlock()
	return nil
}


//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	if c.TaskFinished == len(c.Tasks) + 1 {
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
	c.Combine = false

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
		c.Tasks[i - 1].Finished = make(chan struct{})
	}
	// Your code here.


	c.server()
	return &c
}
