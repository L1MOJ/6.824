package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type RequestTaskArgs struct {
	WorkerId int
}

type ReportTaskArgs struct {
	Task *Task
}

type ReportReplyArgs struct {
	// Ack == 1 when normally finished, -1 for others
	Ack int
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
