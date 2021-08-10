package mr

import (
	"6.824/consts"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type KeyValues struct {
	Key	string
	Values []string
}

type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


var wg sync.WaitGroup

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}



//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	wg = sync.WaitGroup{}
	go MapWorker(mapf)
	go ReduceWorker(reducef)
	wg.Wait()
}

func MapWorker(mapf func(string, string) []KeyValue) {
	wg.Add(1)
	fmt.Println("New map worker")
	for  {
		resp := CallForAcquireTask(consts.TaskTypeReduce)
		if resp.Status == consts.CoordinatorTypeNoTask {
			break
		}
		if len(resp.Task.FileName) == 0 {
			time.Sleep(time.Millisecond * 500)
			continue
		}

		temp := Map(mapf, resp.Task)
		fileName, err := getTempFileNameList(temp, resp.Task.ID, resp.N)
		if err != nil {
			fmt.Println("Get temp file name list failed, err = ", err)
			return
		}
		CallForFinished(resp.Task.ID, resp.Task.TaskType, fileName)
		time.Sleep(time.Millisecond * 100)
	}
	wg.Done()
}

func Map(mapf func(string, string) []KeyValue, task Task) []KeyValue {
	intermediate := []KeyValue{}
	filenames := task.FileName
	for _, filename := range filenames {
		file, err := os.Open(filename)
		defer file.Close()
		if err != nil {
			log.Fatalf("can not open file:%s", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read content:%s", filename)
		}
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	sort.Sort(ByKey(intermediate))
	return intermediate
}

func getTempFileNameList(keyValue []KeyValue, id int, N int) ([]string, error) {
	keyValue2Temp := make(map[int][]KeyValue)
	for _, value := range keyValue {
		reduceID := ihash(value.Key) % N
		if _, ok := keyValue2Temp[reduceID]; !ok {
			keyValue2Temp[reduceID] = []KeyValue{value}
		} else {
			keyValue2Temp[reduceID] = append(keyValue2Temp[reduceID], value)
		}
	}

	fileList := make([]string, 0,len(keyValue2Temp))
	for key, value := range keyValue2Temp {
		filename := fmt.Sprintf("./mr-%d-%d", id, key)
		fileList = append(fileList, filename)

		err := writeToTempFile(value, filename)
		if err != nil {
			fmt.Println("write to temp file failed:", err)
			return nil, err
		}
	}

	return fileList, nil
}

func writeToTempFile(value []KeyValue, filename string) error {
	temp, _ := ioutil.TempFile("", "temp")
	defer temp.Close()
	err := os.Rename(temp.Name(), filename)
	if err != nil {
		fmt.Println("Rename file error, err = ", err)
		return err
	}

	enc := json.NewEncoder(temp)
	for _, v := range value {
		err := enc.Encode(v)
		if err != nil {
			fmt.Println("encode keyvalue into file error! filename:", filename)
			return err
		}
	}
	return nil
}

func DivideTask(intermediate []KeyValue, N	int) map[int][]KeyValues {
	result := make(map[int][]KeyValues)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j ++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		reduceID := ihash(intermediate[i].Key) % N
		if _, ok := result[reduceID]; !ok {
			result[reduceID] = []KeyValues{
				{
					Key: intermediate[i].Key,
					Values:  values,
				},
			}
		} else {
			result[reduceID] = append(result[reduceID], KeyValues{Key: intermediate[i].Key, Values: values})
		}

		i = j
	}
	return result
}
func ReduceWorker(reducef func(string, []string) string) {
	wg.Add(1)
	fmt.Println("New reduce worker")
	for  {
		resp := CallForAcquireTask(consts.TaskTypeReduce)
		if resp.Status == consts.CoordinatorTypeNoTask {
			break
		}
		if len(resp.Task.FileName) == 0 {
			time.Sleep(time.Millisecond * 500)
			continue
		}
		Reduce(reducef, resp)
		CallForFinished(resp.Task.ID, resp.Task.TaskType, nil)
		time.Sleep(time.Millisecond * 100)
	}
	wg.Done()
}
func Reduce(reducef func(string, []string) string, resp AcquireTaskResp) {

}

func CallForAcquireTask(taskType consts.TaskType) AcquireTaskResp {
	req := AcquireTaskReq{taskType}
	resp := AcquireTaskResp{}

	call(consts.MethodAcquireTask, &req, &resp)
	return resp
}

func CallForFinished(id int, taskType consts.TaskType, filename []string) {
	req := FinishedReq{ID: id, TaskType: taskType, filename: filename}
	resp := FinishedResp{}

	call(consts.MethodFinished, &req, &resp)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}