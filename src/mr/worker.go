package mr

import (
	"6.824/consts"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
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
		resp := CallForAcquireTask(consts.TaskTypeMap)
		if len(resp.Task.FileName) == 0 {
			break
		}
	}
	wg.Done()
}

func ReduceWorker(reducef func(string, []string) string) {
	wg.Add(1)
	fmt.Println("New reduce worker")
	for  {
		resp := CallForAcquireTask(consts.TaskTypeReduce)
		if len(resp.Task.FileName) == 0 {
			break
		}

	}
	wg.Done()
}

func Map(mapf func(string, string) []KeyValue, task T) []KeyValue {
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

func Reduce(reducef func(string, []string) string, intermediate map[int][]KeyValues) {
	for id, keyvalues := range intermediate {
		oname := fmt.Sprintf("mr-out-%d", id)
		ofile, _ := os.OpenFile(oname, os.O_CREATE | os.O_WRONLY | os.O_APPEND, 0666)

		for _, keyvalue := range keyvalues {
			output := reducef(keyvalue.Key, keyvalue.Values)
			fmt.Fprintf(ofile, "%v %v\n", keyvalue.Key, output)
		}
		ofile.Close()
	}
}

func CallForAcquireTask(taskType consts.TaskType) AcquireTaskResp {
	req := AcquireTaskReq{taskType}
	resp := AcquireTaskResp{}

	call(consts.MethodAcquireTask, &req, &resp)
	return resp
}

func CallForFinished(id int, taskType int8, result []KeyValue) {
	req := FinishedReq{ID: id, TaskType: taskType, KeyValue: result}
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