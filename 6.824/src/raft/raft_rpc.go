package raft

import (
	"context"
	"time"
)

type CommonArgs struct {
}

type CommonReply struct {
	OK bool
}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Server int
	Term   int
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Ok bool
}

type RequestNewLeader struct {
	Leader int
	Term   int
}

type RequestHeartBeat struct {
	Leader int
	Term   int
}

// RequestVote
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// todo 是否有必要为候选者才能投票
	/*if rf.role == Candidate {
		return
	}*/
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.hasVoted {
		//DPrintf("get vote fail from%d. term%d server%d ", rf.me, args.Term, args.Server)
		return
	}
	rf.hasVoted = true
	reply.Ok = true
	//DPrintf("get vote ok from%d. term%d server%d ", rf.me, args.Term, args.Server)
}

func (rf *Raft) RequestHeartBeat(args *RequestHeartBeat, reply *CommonReply) {
	if rf.term > args.Term && rf.leader != args.Leader {
		rf.ChangeRole(Follower)
		rf.leader = args.Leader
		rf.term = args.Term
	}
	rf.SetLastHeartBeat()
	reply.OK = true
	return
}

func (rf *Raft) RequestNewLeader(args *RequestNewLeader, reply *CommonReply) {
	rf.ChangeRole(Follower)
	rf.leader = args.Leader
	rf.term = args.Term
	rf.electionEnd = true
	rf.SetLastHeartBeat()
	reply.OK = true
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) (res bool) {
	if server < 0 || server >= len(rf.peers) {
		return false
	}
	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Millisecond)
	defer cancel()

	ch := make(chan struct{})
	go func() {
		res = rf.peers[server].Call("Raft.RequestVote", args, reply)
		ch <- struct{}{}
	}()

	select {
	case <-ch:
		return res
	case <-ctx.Done():
		return res
	}
}

func (rf *Raft) sendRequestHeartBeat(server int, args *RequestHeartBeat, reply *CommonReply) (res bool) {
	if server < 0 || server >= len(rf.peers) {
		return false
	}
	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Millisecond)
	defer cancel()

	ch := make(chan struct{})
	go func() {
		res = rf.peers[server].Call("Raft.RequestHeartBeat", args, reply)
		ch <- struct{}{}
	}()

	select {
	case <-ch:
		return res
	case <-ctx.Done():
		//DPrintf("heart beat timeout")
		return res
	}
}

func (rf *Raft) sendRequestNewLeader(server int, args *RequestNewLeader, reply *CommonReply) (res bool) {
	if server < 0 || server >= len(rf.peers) {
		return false
	}

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Millisecond)
	defer cancel()

	ch := make(chan struct{})
	go func() {
		res = rf.peers[server].Call("Raft.RequestNewLeader", args, reply)
		ch <- struct{}{}
	}()

	select {
	case <-ch:
		return res
	case <-ctx.Done():
		//DPrintf("heart beat timeout")
		return res
	}
}
