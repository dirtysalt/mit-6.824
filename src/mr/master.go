package mr

import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "encoding/json"

const (
	TASK_UNASSIGNED = "task_unassigned"
	TASK_RUNNING = "task_running"
	TASK_DONE = "task_done"

	WORKER_UNREACHABLE = 0
	WORKER_UNASSIGNED = 1
	WORKER_RUNNING = 2
)

type TaskObject struct {
	Id string
	State string
	InputFile string
	OutputFile string
	LastTime time.Time
}

type WorkerObject struct {
	Id string
	TaskId string
	State string
	LastTime time.Time
}

type Master struct {
	// Your definitions here.
	Tasks []TaskObject
	MapperNumber int
	ReducerNumber int
	// Workers []WorkerObject
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) getTask() *TaskObject{
	now := time.Now()
	for i := range(m.Tasks) {
		task := &m.Tasks[i]
		switch(task.State) {
		case TASK_DONE: continue
		case TASK_RUNNING: {
			// TODO: if it's has running for a long time.
			d := now.Sub(task.LastTime)
			if d.Seconds() > 60 {
				log.Printf("task %v has been running for a long time: %v seconds\n", task.Id, d.Seconds())
			}
			continue;
		}
		case TASK_UNASSIGNED: {
			return task
		}
		default: panic(fmt.Sprintf("unknown state: %v", task.State));
		}
	}
	return nil;
}

func (m *Master) GetTask(req *GetTaskReq, resp *GetTaskResp) error {
	resp.Quit = false
	resp.Got = false

	if m.Done() {
		resp.Quit = true
		return nil
	}

	task := m.getTask()
	log.Printf("GetTaskReq from worker:%v, return task:%v\n", req.WorkerId, task);
	if task != nil {
		task.State = TASK_RUNNING
		resp.Got = true
		resp.TaskId = task.Id
		resp.InputFile = task.InputFile
		resp.OutputFile = task.OutputFile
		resp.MapperNumber = m.MapperNumber
		resp.ReducerNumber = m.ReducerNumber
	}
	return nil
}

func (m *Master) reportTask(workerId string, taskId string, state string) {
	found := false
	now := time.Now()
	for i := range(m.Tasks) {
		task := &m.Tasks[i]
		if task.Id == taskId {
			found = true
			task.State = state
			task.LastTime = now
			break
		}
	}
	if !found {
		log.Printf("unknown task id: %v\n", taskId)
	}
}

func (m *Master) ReportTask(req *ReportTaskReq, resp *ReportTaskResp) error {
	m.reportTask(req.WorkerId, req.TaskId, req.State)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// ret := false
	ret := true
	// Your code here.

	for i := range(m.Tasks) {
		task := &m.Tasks[i]
		if task.State != TASK_DONE {
			ret = false
			break
		}
	}

	return ret
}

func (m *Master) Dump() {
	for ;; {
		bs, _ := json.Marshal(m);
		log.Printf("========== master state ==========\n%v\n", string(bs))
		time.Sleep(time.Duration(5) * time.Second)
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	swapFilePrefix := "mr-swap-"

	// Your code here.
	now := time.Now()

	for i, f := range(files) {
		tid := fmt.Sprintf("mapper-%04d", i)
		// mr-swap-<reduceno>-<mapno>
		m.Tasks = append(m.Tasks, TaskObject {
			Id: tid,
			State: TASK_UNASSIGNED,
			InputFile: f,
			OutputFile: swapFilePrefix,
			LastTime: now,
		})
	}

	for i:=0;i<nReduce;i++ {
		tid := fmt.Sprintf("reducer-%04d", i)
		output := fmt.Sprintf("mr-out-%04d", i)
		m.Tasks = append(m.Tasks, TaskObject {
			Id: tid,
			State: TASK_UNASSIGNED,
			InputFile:swapFilePrefix,
			OutputFile:output,
			LastTime: now,
		})
	}

	m.MapperNumber = len(files)
	m.ReducerNumber = nReduce
	m.server()

	go m.Dump()
	return &m
}
