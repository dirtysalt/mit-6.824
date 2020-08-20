package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "math/rand"
import "time"
import "strings"
import "io/ioutil"
import "encoding/json"
import "os"
import "sort"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

func runMapper(task GetTaskResp, mapf func(string, string) []KeyValue) {
	inputFile := task.InputFile
	outputFile := task.OutputFile
	seq := task.Seq
	nReduce := task.ReducerNumber

	data, _ := ioutil.ReadFile(inputFile)
	contents := string(data)
	kva := mapf(inputFile, contents)

	// shuffle
	shuffle := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		hash := ihash(kv.Key)
		bucket := hash % nReduce
		shuffle[bucket] = append(shuffle[bucket], kv)
	}

	// write files.
	pat := fmt.Sprintf("map-%d-*", seq)
	for i, kva := range shuffle {
		out := fmt.Sprintf("%s/R%d-M%d", outputFile, i, seq)
		f, _ := ioutil.TempFile(TempFileDir, pat)
		log.Printf("writing temp file: %v\n", f.Name())
		enc := json.NewEncoder(f)
		for _, kv := range kva {
			enc.Encode(&kv)
		}
		f.Close()
		log.Printf("rename %v -> %v\n", f.Name(), out)
		os.Rename(f.Name(), out)
		// defer os.Remove(f.Name())
	}
}

func runReducer(task GetTaskResp, reducef func(string, []string) string) {
	inputFile := task.InputFile
	outputFile := task.OutputFile
	seq := task.Seq

	kva := []KeyValue{}
	for i := 0; i < task.MapperNumber; i++ {
		input := fmt.Sprintf("%s/R%d-M%d", inputFile, seq, i)
		f, _ := os.Open(input)
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))

	i := 0
	pat := fmt.Sprintf("reduce-%d-*", seq)
	f, _ := ioutil.TempFile(TempFileDir, pat)
	log.Printf("writing temp file: %v\n", f.Name())

	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(f, "%v %v\n", kva[i].Key, output)

		i = j
	}

	f.Close()
	log.Printf("rename %v -> %v\n", f.Name(), outputFile)
	os.Rename(f.Name(), outputFile)
	// defer os.Remove(f.Name())
}

func runTask(task GetTaskResp,
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
	ch chan bool) {
	log.Printf("run task: %v\n", task)
	taskId := task.TaskId
	if strings.HasPrefix(taskId, "mapper") {
		runMapper(task, mapf)
	} else {
		runReducer(task, reducef)
	}
	ch <- true
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	workerId := fmt.Sprintf("%v", rand.Uint32())

	log.Printf("worker %v: start ...\n", workerId)

	for {
		req := GetTaskReq{workerId}
		resp := GetTaskResp{}
		call("Master.GetTask", &req, &resp)
		if resp.Quit {
			break
		}
		if !resp.Got {
			log.Printf("worker %v: no task. sleep 5 secs\n", workerId)
			time.Sleep(time.Duration(5) * time.Second)
			continue
		}
		task := resp
		log.Printf("worker %v: get task: %v\n", workerId, task)
		ch := make(chan bool)
		go runTask(task, mapf, reducef, ch)
		for {
			req := ReportTaskReq{
				WorkerId: workerId,
				TaskId:   resp.TaskId,
				State:    TASK_RUNNING,
			}
			resp := ReportTaskResp{}

			select {
			case <-ch:
				{
					req.State = TASK_DONE
				}
			default:
				{
					time.Sleep(time.Duration(5) * time.Second)
				}
			}
			call("Master.ReportTask", &req, &resp)
			if req.State == TASK_DONE {
				break
			}
		}
	}
	log.Printf("worker %v: quit\n", workerId)
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
