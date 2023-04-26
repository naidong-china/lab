package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labrpc"
	"sync"
	"sync/atomic"
	"time"
)

type Role uint8

const (
	Follower  Role = 0
	Candidate Role = 1
	Leader    Role = 2

	NoLeader = -1

	HeartBeatInterval = 50 * time.Millisecond
)

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state

	dead          int32 // set by Kill()
	me            int   // this peer's index into peers[]
	leader        int   // current leader
	term          int   // current term
	electionEnd   bool
	role          Role  // 如果转变为候选者, 那么leader及term都要重置, 只有候选者能进行投票
	lastHeartBeat int64 // last heart beat time

	votes    int  // the num of vote
	hasVoted bool // already vote
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// todo 考虑数据竞争
	return rf.term, rf.leader == rf.me
}

func (rf *Raft) ChangeRole(toRole Role) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	switch {
	case rf.role == Candidate && toRole == Leader:
		rf.leader = rf.me
		rf.role = Leader
		DPrintf("leader is server:%d in term:%d", rf.me, rf.term)
	case rf.role == Follower && toRole == Candidate:
		rf.leader = NoLeader
		rf.term += 1
		rf.role = Candidate
		rf.hasVoted = true
		rf.votes = 1
	case rf.role == Candidate && toRole == Candidate:
		rf.hasVoted = false
		rf.votes = 0
	case toRole == Follower:
		rf.role = Follower
		rf.hasVoted = false
		rf.votes = 0
	default:
		DPrintf("unsupported %v to role:%v", rf.role, toRole)
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// Kill
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		if rf.role == Leader {
			time.Sleep(HeartBeatInterval + RandomizeSleepTime(10, 50))
			continue
		}

		// todo ask heartbeat or send heartbeat
		resp := &CommonReply{}
		ok := rf.sendRequestHeartBeat(rf.leader, &CommonArgs{}, resp, 5*time.Millisecond)
		if ok && resp.OK {
			//DPrintf("heart beat ok. leader%v, server%v", rf.leader, rf.me)
			rf.lastHeartBeat = time.Now().UnixNano() / 1e6
			time.Sleep(HeartBeatInterval + RandomizeSleepTime(10, 50))
			continue
		} else {
			rf.electionEnd = false
		}
		ms := RandomizeSleepTime(10, 500)
		DPrintf("heartbeat loss, start election after %v. leader%v, server%v", ms, rf.leader, rf.me)
		time.Sleep(ms)

		if rf.electionEnd {
			continue
		}

		// 心跳超时后, 转变为候选者, 开启新一轮投票
		rf.ChangeRole(Candidate)

		var wg sync.WaitGroup
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			wg.Add(1)
			go func(server int) {
				defer wg.Done()
				req := &RequestVoteArgs{Server: rf.me, Term: rf.term}
				rsp := &RequestVoteReply{}
				ok := rf.sendRequestVote(server, req, rsp)
				if ok && rsp.Ok {
					rf.mu.Lock()
					rf.mu.Unlock()
					rf.votes += 1
				}
				DPrintf("request vote. me%d term%d server%d res:%v", rf.me, rf.term, server, rsp.Ok)
			}(i)
		}
		wg.Wait()

		majority := Majority(len(rf.peers))
		DPrintf("vote result. server%d vote:%d majority:%d", rf.me, rf.votes, majority)
		if rf.votes >= majority {
			rf.ChangeRole(Leader)

			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(server int) {
					req := &RequestNewLeader{Leader: rf.me, Term: rf.term}
					rsp := &CommonReply{}
					ok := rf.sendRequestNewLeader(server, req, rsp)
					DPrintf("notify new leader to server%d res:%v", server, ok)
				}(i)
			}
		}
		time.Sleep(HeartBeatInterval + RandomizeSleepTime(10, 50))
	}
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.leader = NoLeader
	rf.role = Follower

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
