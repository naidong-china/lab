package mr

import (
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

// Coordinator 协调者
type Coordinator struct {
	Workers   map[int64]*WorkerInfo
	Tasks     map[string]*Task
	Events    chan interface{}
	State     State
	mapTaskWG sync.WaitGroup
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
			TaskId:  MapOpName + strconv.Itoa(i),
			FnName:  MapOpName,
			State:   Ready,
			NReduce: nReduce,
			Inputs:  []string{file},
		})
		c.mapTaskWG.Add(1)
	}
	go func() {
		c.assignMapTask(&AssignMapTasks{Tasks: mapTasks})
	}()
	go func() {
		c.mapTaskWG.Wait()
		c.assignReduceTask(&AssignReduceTasks{})
	}()

	c.server()
	return &c
}

func (c *Coordinator) updateWorkers(list []*WorkerInfo) (err error) {

	for _, info := range list {
		c.Workers[info.WorkerId] = info
	}
	return
}

func (c *Coordinator) taskDone(tasks []*Task) (err error) {

	for _, t := range tasks {
		task, ok := c.Tasks[t.TaskId]
		if !ok {
			continue
		}
		task.State = t.State
		task.Output = append(task.Output, t.Output...)
		if task.FnName == MapOpName {
			c.mapTaskWG.Done()
		}
	}
	return
}

func (c *Coordinator) applyWorker() (worker *WorkerInfo) {

	for {
		if len(c.Workers) == 0 {
			log.Println("not found running worker")
			time.Sleep(5 * time.Second)
			continue
		}
		// todo 加锁
		for _, w := range c.Workers {
			if w.State == Running {
				return w
			}
		}
	}
}

func (c *Coordinator) assignMapTask(assignMapTasks *AssignMapTasks) {

	// 协调者寻找合适的Worker分配任务
	for _, task := range assignMapTasks.Tasks {
		if task.State != Ready {
			continue
		}
		worker := c.applyWorker()
		task.WorkerId = worker.WorkerId
		c.Tasks[task.TaskId] = task
		worker.Client().Call(WorkerInvoke, &InvokeReq{Tasks: []*Task{task}}, &InvokeResp{})
		//task.State = InProgress
		log.Printf("assign map task to worker:%d \n", task.WorkerId)
	}
}

func (c *Coordinator) assignReduceTask(assignReduceTask *AssignReduceTasks) {

	outputs := make(map[string][]string)
	for _, t := range c.Tasks {
		if t.FnName == MapOpName && t.State != Completed {
			return
		}
		for _, output := range t.Output {
			split := strings.Split(output, "-")
			reduceIdx := split[len(split)-1]

			outs, ok := outputs[reduceIdx]
			if !ok {
				outs = make([]string, 0)
				outputs[reduceIdx] = outs
			}
			if IndexOf(outs, output) == -1 {
				outputs[reduceIdx] = append(outputs[reduceIdx], output)
			}
		}
	}

	// 构建M个MapTask, 推送给协调者
	reduceTasks := make(map[string]*Task)
	for i, output := range outputs {

		task, ok := reduceTasks[i]
		if !ok {
			reduceTasks[i] = &Task{
				TaskId: ReduceOpName + i,
				FnName: ReduceOpName,
				State:  Ready,
			}
			task = reduceTasks[i]
		}
		task.Inputs = append(task.Inputs, output...)
		worker := c.applyWorker()
		task.WorkerId = worker.WorkerId
		c.Tasks[task.TaskId] = task
	}

	for _, task := range reduceTasks {
		worker, ok := c.Workers[task.WorkerId]
		if !ok {
			continue
		}
		worker.Client().Call(WorkerInvoke, &InvokeReq{Tasks: []*Task{task}}, &InvokeResp{})
		task.State = InProgress
		log.Printf("assign reduce task to worker:%d \n", task.WorkerId)
	}
}
