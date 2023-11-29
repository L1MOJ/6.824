# 6.824

## Lec2 Go

[Effective GO](https://go.dev/doc/effective_go)

## Lab1 MapReduce

### Rules:

- map功能应该把所有intermidiate keys放入nReduce个buckets中，用于下一步的reduce操作
- 第X个reduce task的output应该输出到文件mr-out-X中
- mr-out-X文件的每一行包含一个Reduce函数的输出，以%v %v的格式输出
- worker应该把Map函数输出的文件存放在当前目录下以供Reduce函数的读取
- main/mrmaster.go期待mr/master.go实现一个Done()函数，当MapReduce完成后返回true，这时mrmaster会退出
- 当一个任务完成时，这个工作进程就应该结束。一个简单的实现就是使用`call()`函数的返回值：如果一个工作进程未能与Master进程连接上，那就可以假定调度进程因为任务完成而退出了，所以工作进程也就可以结束了。这取决于你的设计，你可能发现让调度进程给工作进程一个”请退出”的任务也是可以的。

### Hints:

- 当你不知道从何开始时，修改mr/worker.go文件中的Worker()，给master发送一个RPC来请求任务。然后修改master，回复一个尚未开始的map任务的文件名。然后修改worker让woker读入数据并执行Map函数，就像mrsequential.go那样

- Map和Reduce函数通过Go语言的`plugin`包从`.so`结尾的文件中实时加载

- 如果你改变了`mr/`目录下的内容，就需要重新构建MapReduce的插件，就像这样：`go build -race -buildmode=plugin ../mrapps/wc.go`

- 这个实验里，所有工作进程共享一个文件系统。当所有工作进程都运行在同一台机器上时，这是简单的，但如果运行在不同机器上，就需要一个像GFS一样的全局化的文件系统。

- intermidiate文件的命名规则mr-X-Y，X是Map任务的ID，Y是Reduce任务的ID。

- 工作进程的Map任务需要将中间产物以键值对的形式存入文件，在Reduce任务中还要能正确地读取。Go语言中的encoding/json包是个不错的选择。将键值对以JSON的格式写入文件中：

  ```
    enc:=json.NewEncoder(file)
    for _, kv := ... {
        err := enc.Encode(&kv)
  ```

  然后从文件中读取：

  ```go
    dec := json.NewDecoder(file)
    for {
        var kv KeyValue
        if err:=dec.Decode(&kv);err!=nil{
          break
        }
        kva = append(kva, kv)
    }
  ```

- worker可以使用ihash(key)函数(worker.go)来选择相应的reduce任务, ihash(key) % nReduce

- 可以从mrsequential.go中借鉴代码，读入Map 输入文件、在Map和Reduce之前排序中间件的kv对、将Reduce的结果排序后写入文件

- Master作为RPC服务器，是并发的，要给共享数据上锁

- 要使用race detector

- workers有时需要进入等待，比如在最后一个map任务完成之前。

  - 可以让workers每隔一段时间就向master请求work，每个请求之间time.Sleep()
  - 另一种方案是在master进程的RPC处理器使用等待循环（`time.Sleep()`或者`sync.Cond`都可以）。Go程序将RPC的处理程序运行在自己的线程里，所以事实上一个处在等待中的RPC处理程序不会影响其他RPC的处理进程。

- master进程无法准确地分辨出哪些工作进程是死机了，哪些是运行着但由于某些原因瘫痪了，哪些是执行着但是太慢了而起不了作用。你能做的就是让master进程等待一段时间，如果还没有结果，就放弃它，将任务重新分配给另一个工作进程。在这个实验中，让调度进程等待**10s**，超出这个时间，调度进程就可以认为这个工作进程已经死了

- 用mrapps/crash.gp测试crash recovery

- 为了确保没人能发现因程序崩溃而产生的不完整的文件，在MapReduce的论文中提到了一个小技巧，即先写入临时文件，等它完成后再重命名。你可以用`ioutil.TempFile`创建临时文件，用 `os.Rename`重命名。

- `test-mr.sh`在它的子目录`mr-tmp`中执行它所有的进程，所以如果出错了，可以在那里找到相关输出文件。你可以临时修改`test-mr.sh`文件，让它失败时就退出，这样就不会继续往下测试（覆盖之前的输入内容）。

**sync.Cond**

```go
package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	var mutex sync.Mutex
	cond := sync.NewCond(&mutex)

	for i := 0; i < 3; i++ {
		go func(i int) {
			mutex.Lock()
			defer mutex.Unlock()
			fmt.Printf("Goroutine %d 正在等待\n", i)
			// Wait会自动释放锁，接收到信号后重新获取锁
             cond.Wait()
			fmt.Printf("Goroutine %d 被释放\n", i)
		}(i)
	}

	// 让 goroutines 达到 Wait 点
	time.Sleep(time.Second)
	mutex.Lock()
	fmt.Println("正在广播")
    //Broadcast唤醒所有goroutines, Signal唤醒一个
	cond.Broadcast()
	mutex.Unlock()

	// 等待一会儿看所有 goroutines 完成
	time.Sleep(time.Second)
}

```

### 碰到的问题

```
dialing:dial unix /var/tmp/824-mr-1000: connect: connection refused
```

是因为master.go中Done函数立即return了true，导致master很快exit了，添加延时后return解决

- time.Time 比较是否相等时要用到Equal方法，不能直接使用==

### 思路
- 拿到题目的时候确实感觉无从下手，去b站找了个视频，看了一点大佬的笔记，用RPC把worker, master调通以后就自己开始写了
- 根据论文思路画了个示意图，整个过程会涉及到Map, Reduce两类Task，定义Task时比较直观的思路就是分为Map, Reduce, Wait, Exit几个TaskType，根据Rules和Hints可以得知master可以通过发送wait和exit给worker进行通讯
- Task结构体主要就是分为TaskType, Id, NReduce（进行Map Task时创建intermidiate文件用），Files数组（Reduce Task时遍历），BeginTime进行超时处理以及任务完成时master的校验
- 直观地，master端可以为Map，Reduce各创建idle, inProgree, completed三个channel来管理Tasks
- 接下来从worker给master发送requestTask请求Map Task开始一步一步做就可以了

### Task结构体

```go
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
```

### Master

```go
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
```

初始化函数

```go
func MakeMaster(files []string, nReduce int) *Master 
```

Done

```go
func (m *Master) Done() bool {

	// if reduce tasks and map tasks are completed, return true
	if len(m.completedMapTasks) == m.numMapTasks && len(m.completedReduceTasks) == m.numReduceTasks {
		fmt.Printf("All tasks completed, exiting...\n")
		return true
	} else {
		return false
	}
}
```

RequestTask处理来自worker的请求，根据channel情况决定分发哪个task

```go
func (m *Master) RequestTask(args *RequestTaskArgs, reply *Task) error
```

ReportTask如果worker完成了Task后会给master发送report

```go
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
```

### Worker

初始化，如果没有exit就一直问master索要任务

```go
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Save mapf and reducef 到全局变量以便其他函数使用
	storedMapf = mapf
	storedReducef = reducef

	// Your worker implementation here.
	// if exitState == false, exit
	if exitState == true {
		return
	}

	for {
		// 1. CallRequestTask to get a task from master
		task := callRequestTask()
		// 2. Handle different tasks
		taskHandler(&task)
	}

}
```

taskHandler

```go
// taskHandler to handle different tasks
func taskHandler(task *Task) {
	switch task.Type {
	case MapTask:
		handleMapTask(task)
	case ReduceTask:
		handleReduceTask(task)
	case WaitTask:
		handleWaitTask(task)
	case ExitTask:
		handleExitTask(task)
	}
}
```

handleMap和Reduce的函数可以借鉴sequentialMR

wait我的实现是等待两秒钟，exit是直接把worker的exitstate设为true，不再向master请求任务

- Go channel
- RPC

### References

https://blog.rayzhang.top/2022/10/29/mit-6.824-lab1-mapreduce
