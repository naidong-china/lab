package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"path"
)
import "net/rpc"

type Worker struct {
	WorkerInfo
	CoordinatorClient *RpcClient
	MapOp             MapOp
	ReduceOp          ReduceOp
	Tasks             map[string]*Task
}

// NewWorker main/mrworker.go calls this function.
func NewWorker(mapOp MapOp, reduceOp ReduceOp) (w *Worker) {

	w = &Worker{MapOp: mapOp, ReduceOp: reduceOp}
	// todo 测试workerId
	w.WorkerId = 1

	// 创建rpc服务
	w.server()

	// 连接协调者, 发送心跳上报Worker状态及地址元信息
	w.CoordinatorClient = NewRpcClient("unix", coordinatorSock())
	go w.register()

	return
}

func (w *Worker) server() {
	_ = rpc.Register(w)
	rpc.HandleHTTP()

	w.Network, w.Addr = "unix", workerSock(w.WorkerId)
	_ = os.Remove(w.Addr)

	l, e := net.Listen(w.Network, w.Addr)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	w.State = Running
	log.Printf("worker rpc server listen on: %v \n", w.Addr)
	go func() { _ = http.Serve(l, nil) }()
}

// use ihash(key) % NReduce to choose the ReduceTask number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (w *Worker) register() {

	req := &ReportReq{List: []*WorkerInfo{&w.WorkerInfo}}
	resp := &ReportResp{}
	w.CoordinatorClient.Call(CoordinatorReport, req, resp)
}

func (w *Worker) invoke(req *InvokeReq) (err error) {

	for _, task := range req.Tasks {
		switch task.FnName {
		case MapOpName:
			w.invokeMapTask(task)
		case ReduceOpName:
			w.invokeReduceTask(task)
		}
	}
	return
}

func (w *Worker) invokeMapTask(task *Task) {
	log.Printf("invoke map task. id:%s \n", task.TaskId)
	var NReduce = task.NReduce

	intermediate := make(map[int]*os.File)
	for i := 1; i <= NReduce; i++ {
		oname := fmt.Sprintf("mr-worker-%d-reduce-in-%d", w.WorkerId, i)
		output := path.Join(w.Addr, oname)
		f, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)
		if err != nil {
			log.Println(err)
			return
		}
		intermediate[i] = f
		task.Output = append(task.Output, output)
	}

	for _, filename := range task.Inputs {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

		kva := w.MapOp(filename, string(content))
		log.Printf("MapOp output: %d \n", len(kva))
		for _, kv := range kva {
			idx := ihash(kv.Key) % NReduce
			f, ok := intermediate[idx]
			if !ok {
				log.Printf("not found output file. key:%s, idx:%d \n", kv.Key, idx)
				continue
			}
			if _, err := f.Write(kv.Bytes()); err != nil {
				log.Printf("write output file. key:%s, err:%s", kv.Key, err.Error())
			}
		}
	}

	task.State = Completed
	req := &TaskDoneReq{Tasks: []*Task{task}}
	resp := &TaskDoneResp{}
	w.CoordinatorClient.Call(CoordinatorTaskDone, req, resp)
}

func (w *Worker) invokeReduceTask(task *Task) {

}
