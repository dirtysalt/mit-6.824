package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
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

// Add your RPC definitions here.

// get a task from master
type GetTaskReq struct {
	WorkerId string
}
type GetTaskResp struct {
	Got bool
	Quit bool
	TaskId string
	InputFile string
	OutputFile string
	MapperNumber int
	ReducerNumber int
}

// report task to master
type ReportTaskReq struct {
	WorkerId string
	TaskId string
	State string
}

type ReportTaskResp struct {
}

// heartbeat to master
type WorkerHBReq struct {
	WorkerId string
	State string
}

type WorkerHBResp struct {
}



// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
