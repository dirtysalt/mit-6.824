package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "math/rand"
import "time"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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

func runWorker(task GetTaskResp, ch chan bool) {
	log.Printf("run worker with task: %v\n", task)
	ch <- true
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	workerId := fmt.Sprintf("%v", rand.Uint32())

	log.Printf("worker %v start ...\n", workerId)

	for ;; {
		req := GetTaskReq {workerId}
		resp := GetTaskResp {}
		call("Master.GetTask", &req, &resp)
		if resp.Quit {
			break
		}
		if !resp.Got {
			log.Printf("worker %v: no task. sleep 5 secs\n", workerId);
			time.Sleep(time.Duration(5) * time.Second)
			continue;
		}
		task := resp
		log.Printf("worker %v: get task: %v\n", workerId, task);
		ch := make(chan bool)
		go runWorker(task, ch)
		for ;; {
			req := ReportTaskReq {
				WorkerId: workerId,
				TaskId: resp.TaskId,
				State: TASK_RUNNING,
			}
			resp := ReportTaskResp {}

			select {
			case <- ch: {
				req.State = TASK_DONE
			}
			default: {
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
