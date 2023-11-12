package mr

import "time"

const(
	HeartDuration = 2*time.Second
	WorkerTimeOut = 10 * time.Second 
	WaitingTime = 5*time.Second
)

type TaskMetaInfo struct {
	state TaskState
	StartTime time.Time
	TaskAdr *Task
}

type Task struct{
	Files []string
	Id  int64
	TaskType TaskType
	NReduce int
}

type TaskArgs struct {}

type SchedulePhase int
type TaskType int
type TaskState int

const (
	MapPhase    SchedulePhase = iota // 此阶段在分发MapTask
	ReducePhase                      // 此阶段在分发ReduceTask
	AllDone                          // 此阶段已完成
)

// 枚举任务阶段的类型
const (
	MapTask TaskType = iota
	ReduceTask
	WaitingTask // Waiting任务代表此时任务都分发完了，但是任务还没完成，阶段未改变
	ExitTask
)

// 枚举任务状态类型
const (
	Waiting TaskState = iota // 此阶段在等待执行
	Working                  // 此阶段在工作
	Done                     // 此阶段已经做完
)