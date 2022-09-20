package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := fmt.Sprintf("/var/tmp/824mr-uid%d-c", os.Getuid())
	log.Printf("get coordinator sock: %s \n", s)
	return s
}

func workerSock(workerId int64) string {
	s := fmt.Sprintf("/var/tmp/824mr-uid%d-w%d", os.Getuid(), workerId)
	log.Printf("get worker sock: %s \n", s)
	return s
}

type RpcClient struct {
	network string
	addr    string
}

func NewRpcClient(network, addr string) *RpcClient {
	r := &RpcClient{network: network, addr: addr}
	if len(network) == 0 {
		r.network = "unix"
	}
	if len(addr) == 0 {
		addr = coordinatorSock()
	}
	return r
}

// Call method send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func (r *RpcClient) Call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP(r.network, r.addr)
	if err != nil {
		log.Fatalf("dialing:%v, network:%s, addr:%s \n", err, r.network, r.addr)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	log.Printf("call rpc:%s req:%+v err:%v \n", rpcname, args, err)
	return err == nil
}

type ReportReq struct {
	List []*WorkerInfo
}

type ReportResp struct{}

type WorkerInfo struct {
	WorkerId int64
	Network  string
	Addr     string
	State    State
}

func (w *WorkerInfo) Client() *RpcClient {
	return NewRpcClient(w.Network, w.Addr)
}

const CoordinatorReport = "Coordinator.Report"

// Report 接受worker上报服务元信息, 任务状态/输出元信息
func (c *Coordinator) Report(req *ReportReq, resp *ReportResp) (err error) {
	log.Printf("coordinator recv report. req:%+v \n", req)
	return c.updateWorkers(req.List)
}

type TaskDoneReq struct {
	Tasks []*Task
}

type TaskDoneResp struct {
}

const CoordinatorTaskDone = "Coordinator.TaskDone"

func (c *Coordinator) TaskDone(req *TaskDoneReq, resp *TaskDoneResp) (err error) {
	log.Printf("coordinator recv task done. req:%+v \n", req)
	return c.taskDone(req.Tasks)
}

type InvokeReq struct {
	Tasks []*Task
}

type InvokeResp struct {
}

const WorkerInvoke = "Worker.Invoke"

// Invoke 执行来自协调者分配的任务
func (w *Worker) Invoke(req *InvokeReq, resp *InvokeResp) (err error) {
	return w.invoke(req)
}
