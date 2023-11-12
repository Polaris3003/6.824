package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var coordinator *Coordinator

type Coordinator struct {
	sync.RWMutex
	files       []string // 传入的文件数组
	nReduce     int
	nMap        int
	phase       SchedulePhase
	taskMetaMap map[int64]*TaskMetaInfo // 存放task
	TaskBeginId int64                   // 用于生成task的特殊id

	ReduceTaskChan chan *Task // 使用chan保证并发安全
	MapTaskChan    chan *Task // 使用chan保证并发安全
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.Lock()
	defer c.Unlock()
	if c.phase == AllDone {
		fmt.Println("All tasks are finished,the coordinator will exit!!!")
		return true
	} else {
		return false
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	coordinator = &Coordinator{
		files:          files,
		nReduce:        nReduce,
		nMap:           len(files),
		phase:          MapPhase,
		taskMetaMap:    make(map[int64]*TaskMetaInfo, len(files)+nReduce),
		MapTaskChan:    make(chan *Task, len(files)),
		ReduceTaskChan: make(chan *Task, nReduce),
	}
	coordinator.makeMapTasks(files)
	coordinator.server()
	go coordinator.CrashDetector()
	return coordinator
}

func (c *Coordinator) makeMapTasks(files []string) {
	for _, v := range files {
		id := c.generateTaskId()
		task := &Task{
			Id:       id,
			TaskType: MapTask,
			NReduce:  c.nReduce,
			Files:    []string{v},
		}
		taskMetaInfo := &TaskMetaInfo{
			state:   Waiting,
			TaskAdr: task,
		}
		c.acceptTaskMeta(taskMetaInfo)
		c.MapTaskChan <- task
	}
}

func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.nReduce; i++ {
		id := c.generateTaskId()
		task := &Task{
			Id:       id,
			TaskType: ReduceTask,
			Files:    selectReduceName(i),
		}

		// 保存任务的初始状态
		taskMetaInfo := &TaskMetaInfo{
			state:   Waiting, // 任务等待被执行
			TaskAdr: task,    // 保存任务的地址
		}
		c.acceptTaskMeta(taskMetaInfo)

		//fmt.Println("make a reduce task :", &task)
		c.ReduceTaskChan <- task
	}
}

func selectReduceName(nReduce int) []string {
	var s []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, fi := range files {
		// 匹配对应的reduce文件
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(nReduce)) {
			s = append(s, fi.Name())
		}
	}
	return s
}

// CrashDetector 定时检测执行任务是否超时
func (c *Coordinator) CrashDetector() {
	for {
		time.Sleep(HeartDuration)
		c.Lock()
		if c.phase == AllDone {
			c.Unlock()
			break
		}
		for _, v := range c.taskMetaMap {
			if v.state == Working && time.Since(v.StartTime) > WorkerTimeOut {
				fmt.Printf("the task[ %d ] is crash,take [%d] s\n", v.TaskAdr.Id, time.Since(v.StartTime))
				switch v.TaskAdr.TaskType {
				case MapTask:
					c.MapTaskChan <- v.TaskAdr
					v.state = Waiting
				case ReduceTask:
					c.ReduceTaskChan <- v.TaskAdr
					v.state = Waiting
				}
			}
		}
		c.Unlock()
	}
}

// PollTask worker调用，获取任务
func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	c.Lock()
	defer c.Unlock()
	switch c.phase {
	case MapPhase:
		if len(c.MapTaskChan) > 0 {
			*reply = *<-c.MapTaskChan
			//fmt.Printf("poll-Map-taskid[ %d ]\n", reply.TaskId)
			if !c.judgeState(reply.Id) {
				fmt.Printf("Map-taskid[ %d ] is running\n", reply.Id)
			}
		} else {
			reply.TaskType = WaitingTask // 如果map任务被分发完了但是又没完成，此时就将任务设为Waitting
			if c.checkTaskDone() {
				c.toNextPhase()
			}
			return nil
		}
	case ReducePhase:
		if len(c.ReduceTaskChan) > 0 {
			*reply = *<-c.ReduceTaskChan
			//fmt.Printf("poll-Reduce-taskid[ %d ]\n", reply.TaskId)
			if !c.judgeState(reply.Id) {
				fmt.Printf("Reduce-taskid[ %d ] is running\n", reply.Id)
			}
		} else {
			reply.TaskType = WaitingTask // 如果map任务被分发完了但是又没完成，此时就将任务设为Waiting
			if c.checkTaskDone() {
				c.toNextPhase()
			}
			return nil
		}
	case AllDone:
		reply.TaskType = ExitTask
	default:
		panic("The phase undefined ! ! !")
	}
	return nil
}

// MarkFinished worker调用，标记某个任务完成
func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	c.Lock()
	defer c.Unlock()
	switch args.TaskType {
	case MapTask:
		meta, ok := c.taskMetaMap[args.Id]
		//prevent a duplicated work which returned from another worker
		if ok && meta.state == Working {
			meta.state = Done
			//fmt.Printf("Map task Id[%d] is finished.\n", args.TaskId)
		} else if meta.state == Done {
			fmt.Printf("Map task Id[%d] is finished,already ! ! !\n", args.Id)
		} else {
			fmt.Printf("Map task Id[%d] is in trouble, the task message is %v.\n", args.Id, args)
		}
		break
	case ReduceTask:
		meta, ok := c.taskMetaMap[args.Id]

		//prevent a duplicated work which returned from another worker
		if ok && meta.state == Working {
			meta.state = Done
			//fmt.Printf("Reduce task Id[%d] is finished.\n", args.TaskId)
		} else if meta.state == Done {
			fmt.Printf("Reduce task Id[%d] is finished,already ! ! !\n", args.Id)
		} else {
			fmt.Printf("Reduce task Id[%d] is in trouble, the task message is %v.\n", args.Id, args)
		}
		break

	default:
		panic("The task type undefined ! ! !")
	}
	return nil

}

func (c *Coordinator) acceptTaskMeta(taskInfo *TaskMetaInfo) bool {
	taskId := taskInfo.TaskAdr.Id
	if _, ok := c.taskMetaMap[taskId]; ok {
		fmt.Printf("taskMetaMap contains Id=%v task\n", taskId)
		return false
	} else {
		c.taskMetaMap[taskId] = taskInfo
	}
	return true
}

// 检查多少个任务做了包括（map、reduce）
func (c *Coordinator) checkTaskDone() bool {
	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)

	for _, v := range c.taskMetaMap {
		if v.TaskAdr.TaskType == MapTask {
			if v.state == Done {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if v.TaskAdr.TaskType == ReduceTask {
			if v.state == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}
	}
	// Map
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		return true
	} else if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
		return true
	}
	return false
}

// 判断给定任务是否在工作，并修正其目前任务信息状态,如果任务不在工作的话返回true
func (c *Coordinator) judgeState(taskId int64) bool {
	taskInfo, ok := c.taskMetaMap[taskId]
	if !ok || taskInfo.state != Waiting {
		return false
	}
	taskInfo.state = Working
	taskInfo.StartTime = time.Now()
	return true
}

func (c *Coordinator) toNextPhase() {
	if c.phase == MapPhase {
		c.makeReduceTasks()
		c.phase = ReducePhase
	} else if c.phase == ReducePhase {
		c.phase = AllDone
	}
}

// 通过结构体的TaskId自增来获取唯一的任务id
func (c *Coordinator) generateTaskId() int64 {
	res := c.TaskBeginId
	c.TaskBeginId++
	return res
}