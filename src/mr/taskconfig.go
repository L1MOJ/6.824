package mr
//import time
import "time"


type TaskType int

// Define task types including MapTask, ReduceTask, WaitTask, ExitTask
const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

// Define task struct
type Task struct {
	Type   TaskType
	TaskId int
	NReduce int
	Files   []string
	BeginTime time.Time
}

// Initialize a task
func NewTask(taskType TaskType, taskId int, nReduce int, files []string, beginTime time.Time) *Task {
	return &Task{
		Type: taskType,
		TaskId: taskId,
		NReduce: nReduce,
		Files: files,
		BeginTime: beginTime,
	}
}