package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "fmt"

const IntermediateNameTemplate string = "mr-%d-%d"

type Master struct {
	// Define master struct, including numMapTasks, numReduceTasks, channels to store different status tasks
	numMapTasks int
	idleMapTasks chan *Task
	inProgressMapTasks chan *Task
	completedMapTasks chan *Task

	numReduceTasks int
	idleReduceTasks chan *Task
	inProgressReduceTasks chan *Task
	completedReduceTasks chan *Task
}

// function to handle task report
func (m *Master) processTask(taskType TaskType, taskID int, beginTime time.Time, inProgress chan *Task, completed chan *Task, reply *ReportReplyArgs) {
	// fmt.Printf("Report received: Type: %v, Id: %v, BeginTime: %v\n", taskType, taskID, beginTime)
	for i := 0; i < len(inProgress); i++ {
        task := <-inProgress
		// fmt.Printf("Comparing task: Type: %v, Id: %v, BeginTime: %v\n", task.Type, task.TaskId, task.BeginTime)
		// fmt.Printf("Is time equal? %v\n", beginTime.Equal(task.BeginTime))
        if (taskID == task.TaskId) && beginTime.Equal(task.BeginTime) {
            // match! put it in to completed channel
            completed <- task
            reply.Ack = 1
            fmt.Printf("Type %v Task %v completed\n", task.Type, task.TaskId)
            return
        } else {
            inProgress <- task
        }
    }
    reply.Ack = -1
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) ReportTask(args *Task, reply *ReportReplyArgs) error {
	// fmt.Printf("Report received: Type: %v, Id: %v, BeginTime: %v\n", args.Type, args.TaskId, args.BeginTime)
	tType := args.Type
	switch tType {
	case MapTask:
		m.processTask(MapTask, args.TaskId, args.BeginTime, m.inProgressMapTasks, m.completedMapTasks, reply)
	case ReduceTask:
		m.processTask(ReduceTask, args.TaskId, args.BeginTime, m.inProgressReduceTasks, m.completedReduceTasks, reply)
	default:
		reply.Ack = -1
	}

	return nil
}

// RPC handler for worker request task
func (m *Master) RequestTask(args *RequestTaskArgs, reply *Task) error {
	// if all map tasks are completed, check reduce tasks
	if len(m.completedMapTasks) == m.numMapTasks {
		// if all reduce tasks are completed, return exit task
		if len(m.completedReduceTasks) == m.numReduceTasks {
			*reply = *NewTask(ExitTask, -1, 0, []string{}, time.Time{})
			return nil
		} else {
			// if there is idle reduce task, return reduce task
			if len(m.idleReduceTasks) > 0 {
				task := <- m.idleReduceTasks
				task.BeginTime = time.Now()
				m.inProgressReduceTasks <- task
				*reply = *task
				return nil
			}
			// no idle tasks, check in progress tasks, if timeout, redispatch it, else send wait
			for i := 0; i < len(m.inProgressReduceTasks); i++ {
				task := <- m.inProgressReduceTasks
				if time.Now().Sub(task.BeginTime) > 10 * time.Second {
					task.BeginTime = time.Now()
					*reply = *task
					m.inProgressReduceTasks <- task
					return nil
				} else {
					m.inProgressReduceTasks <- task
				}
			}
			// no timeout tasks, return wait task
			*reply = *NewTask(WaitTask, -1, 0, []string{}, time.Time{})
			return nil
		}
	} else {
		// if there is idle map task, return map task
		if len(m.idleMapTasks) > 0 {
			task := <- m.idleMapTasks
			task.BeginTime = time.Now()
			m.inProgressMapTasks <- task
			*reply = *task
			return nil
		}
		// no idle tasks, check in progress tasks, if timeout, redispatch it, else send wait
		for i := 0; i < len(m.inProgressMapTasks); i++ {
			task := <- m.inProgressMapTasks
			if time.Now().Sub(task.BeginTime) > 10 * time.Second {
				task.BeginTime = time.Now()
				*reply = *task
				m.inProgressMapTasks <- task
				return nil
			} else {
				m.inProgressMapTasks <- task
			}
		}
		// no timeout tasks, return wait task
		*reply = *NewTask(WaitTask, -1, 0, []string{}, time.Time{})
		return nil
	}
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

	// if reduce tasks and map tasks are completed, return true
	if len(m.completedMapTasks) == m.numMapTasks && len(m.completedReduceTasks) == m.numReduceTasks {
		fmt.Printf("All tasks completed, exiting...\n")
		return true
	} else {
		return false
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	
	// Your code here.
	// Initialize master struct
	m := Master{
		numMapTasks: len(files),
		idleMapTasks: make(chan *Task, len(files)),
		inProgressMapTasks: make(chan *Task, len(files)),
		completedMapTasks: make(chan *Task, len(files)),

		numReduceTasks: nReduce,
		idleReduceTasks: make(chan *Task, nReduce),
		inProgressReduceTasks: make(chan *Task, nReduce),
		completedReduceTasks: make(chan *Task, nReduce),
	}
	// Initialize map tasks
	for i := 0; i < len(files); i++ {
		m.idleMapTasks <- &Task{
			Type: MapTask,
			TaskId: i,
			NReduce: nReduce,
			// apeend file name to files
			Files: []string{files[i]},
			BeginTime: time.Time{},
		}
	}
	// Initialize reduce tasks
	for i := 0; i < nReduce; i++ {
		task := &Task{
			Type: ReduceTask,
			TaskId: i,
			Files: []string{},
			BeginTime: time.Time{},
		}
		for j := 0; j < len(files); j++ {
			file := fmt.Sprintf(IntermediateNameTemplate, j, i)
			task.Files = append(task.Files, file)
		}
		m.idleReduceTasks <- task
	}
	// Start server
	m.server()
	return &m
}
