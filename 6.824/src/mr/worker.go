package mr

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
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

	bytes, _ := json.Marshal(w.WorkerInfo)
	req := &ReportReq{
		ReportType: RegisterWorker,
		JsonData:   string(bytes),
	}
	resp := &ReportResp{}
	w.CoordinatorClient.Call(CoordinatorReport, req, resp)
}

func (w *Worker) invoke(req *InvokeReq) (err error) {

	for _, task := range req.Tasks {
		switch task.FnName {
		case MapOpName:
			w.invokeMapTask(task)
		case ReduceOpName:
			go w.invokeReduceTask(task)
		}
	}
	return
}

func (w *Worker) invokeMapTask(task *Task) {
	log.Printf("invoke map task. id:%d \n", task.TaskId)
	var NReduce = task.NReduce

	intermediate := make(map[int][]KeyValue)
	for i := 1; i <= NReduce; i++ {
		intermediate[i] = make([]KeyValue, 1000)
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
			_, ok := intermediate[idx]
			if ok {
				intermediate[idx] = append(intermediate[idx], kv)
			}
		}
	}
}

func (w *Worker) invokeReduceTask(task *Task) {

}
