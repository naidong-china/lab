package mr

import (
	"encoding/json"
	"log"
	"strconv"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// Coordinator 协调者
type Coordinator struct {
	Workers map[int64]*WorkerInfo
	Tasks   map[string]*Task
	Events  chan interface{}
	State   State
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	_ = rpc.Register(c)
	rpc.HandleHTTP()

	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	_ = os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	c.State = Running
	log.Printf("coordinator is running on %v \n", sockname)
	go func() { _ = http.Serve(l, nil) }()
}

// Done main/mrcoordinator.go calls Done() periodically to find out. if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.State == Exit
}

// MakeCoordinator create a Coordinator. main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Workers: make(map[int64]*WorkerInfo),
		Events:  make(chan interface{}, 1024),
		Tasks:   make(map[string]*Task),
	}

	// 构建M个MapTask, 推送给协调者
	mapTasks := make([]*Task, 0)
	for i, file := range files {
		mapTasks = append(mapTasks, &Task{
			TaskId:  "map" + strconv.Itoa(i),
			FnName:  MapOpName,
			State:   Ready,
			NReduce: nReduce,
			Inputs:  []string{file},
		})
	}
	c.Events <- &AssignMapTasks{Tasks: mapTasks}

	c.server()
	go func() { c.eventLoop() }()
	return &c
}

func (c *Coordinator) eventLoop() {

	for {
		event := <-c.Events
		switch ev := event.(type) {
		case *AssignMapTasks:
			go func() { c.assignMapTask(ev) }()
		default:
			log.Printf("unsupported event: %v \n", event)
		}
	}
}

func (c *Coordinator) updateWorkers(data string) (err error) {

	var info WorkerInfo
	if err = json.Unmarshal([]byte(data), &info); err != nil {
		log.Printf("update worker infos. err:%v \n", err)
		return
	}
	info.Client = NewRpcClient(info.Network, info.Addr)
	c.Workers[info.WorkerId] = &info
	return
}

func (c *Coordinator) assignMapTask(submitTasks *AssignMapTasks) {

	// todo 协调者寻找合适的Worker分配任务
	assignMapTask := make([]*Task, 0)
	for _, task := range submitTasks.Tasks {
		if task.FnName != MapOpName {
			continue
		}
		switch task.State {
		case Ready:
			for {
				if len(c.Workers) == 0 {
					log.Println("not found running worker")
					time.Sleep(5 * time.Second)
					continue
				}
				for wid := range c.Workers {
					task.WorkerId = wid
					break
				}
				break
			}
			c.Tasks[task.TaskId] = task
			assignMapTask = append(assignMapTask, task)
		case Completed:
			if t, ok := c.Tasks[task.TaskId]; ok {
				t.State = Completed
				t.Output = task.Output
			}
			// todo 判断是否所有MapTask已完成
		}
	}

	for _, task := range assignMapTask {
		if task.FnName != MapOpName {
			continue
		}
		worker, ok := c.Workers[task.WorkerId]
		if !ok {
			continue
		}
		log.Printf("assign map task to worker:%d \n", task.WorkerId)
		worker.Client.Call(WorkerInvoke, &InvokeReq{Tasks: []*Task{task}}, &InvokeResp{})
		task.State = InProgress
	}
}
