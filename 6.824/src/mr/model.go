package mr

import "encoding/json"

// KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

func (kv *KeyValue) Marshal() []byte {
	bytes, _ := json.Marshal(kv)
	return bytes
}

func (kv *KeyValue) Unmarshal(bytes []byte) {
	_ = json.Unmarshal(bytes, &kv)
}

type ByKey []*KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

const (
	MapOpName    = "Map"
	ReduceOpName = "Reduce"
)

type MapOp func(document string, value string) []KeyValue

type ReduceOp func(key string, values []string) string

const (
	Ready      TaskState = 1 // 待执行
	InProgress TaskState = 2 // 正在执行
	Completed  TaskState = 3 // 完成
)

type TaskState uint8

type Task struct {
	TaskId   string
	WorkerId int64     // 执行任务的worker
	FnName   string    // 函数名
	State    TaskState // 任务状态
	NReduce  int       // Reduce数量
	Inputs   []string  // 输入集合, 采用资源定位符描述方式, file://{ip}:{port}/inputs.txt
	Output   []string  // 输出集合, 同上
}

const (
	Unknown State = 0
	Running State = 1
	Exit    State = 2
)

type State uint8

type AssignMapTasks struct {
	Tasks []*Task
}

type AssignReduceTasks struct {
	Tasks []*Task
}
