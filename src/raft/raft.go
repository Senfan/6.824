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
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

const (
	LogEntrySize         = 100
	HEARTBEAT_TIMEOUT    = 100 // 心跳超时时间
	VOTED_NIL            = -1
	ELECTION_TIMEOUT_MIN = 500
	ELECTION_TIMEOUT_MAX = 800
)

type State int

const (
	FOLLOWER  State = 1
	LEADER    State = 2
	CANDIDATE State = 3
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state:
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// 超时时间控制
	voteGrantedCnt           int
	winElectionChan          chan bool
	heartbeatChan            chan bool
	requestFromCandidateChan chan bool
	state                    State
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	defer rf.mu.Unlock()
	rf.mu.Lock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm { // 新一轮选举
		rf.state = FOLLOWER
		rf.votedFor = VOTED_NIL
		rf.currentTerm = args.Term
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	lastEntry := rf.getLastLogEntry()
	if (rf.votedFor == VOTED_NIL || rf.votedFor == args.CandidateID) && lastEntry.Index <= args.LastLogIndex {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		rf.requestFromCandidateChan <- true
	}

	return
}

//
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	println(fmt.Sprintf("candidate: %d, send request vote to: %d", args.CandidateID, server))
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		println(fmt.Sprintf("sendRequestVote from %d to %d failed", rf.me, server))
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.state == CANDIDATE && ok && rf.currentTerm == args.Term {
		if reply.VoteGranted {
			rf.voteGrantedCnt++
			println(fmt.Sprintf("candidate: %d, vote: %d", args.CandidateID, rf.voteGrantedCnt))
			if rf.voteGrantedCnt > len(rf.peers)>>1 { // Quorum机制
				rf.winElectionChan <- true
				rf.state = LEADER
				// 初始化相关状态
				rf.persist()
				lastEntry := rf.getLastLogEntry()
				initIndex := lastEntry.Index + 1
				for server := 0; server < len(rf.nextIndex); server++ {
					rf.nextIndex[server] = initIndex
				}
			}
		} else if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = VOTED_NIL
		}
	}
	return ok
}

//
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

//
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
	defer rf.mu.Unlock()
	rf.mu.Lock()
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	defer rf.mu.Unlock()
	rf.mu.Lock()
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
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
	serverCnt := len(peers)
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = FOLLOWER
	rf.votedFor = VOTED_NIL
	rf.heartbeatChan = make(chan bool)
	rf.requestFromCandidateChan = make(chan bool)
	rf.winElectionChan = make(chan bool)
	rf.nextIndex = make([]int, serverCnt)
	rf.matchIndex = make([]int, serverCnt)
	rf.commitIndex = 0
	rf.lastApplied = 0
	// Your initialization code here (2A, 2B, 2C).
	//rf.log = []LogEntry{}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.log == nil {
		rf.log = make([]LogEntry, LogEntrySize)
		rf.log = append(rf.log, LogEntry{})
	}
	go rf.Run()

	return rf
}

func (rf *Raft) Run() {
	for {
		rf.mu.Lock()
		if rf.dead == 1 {
			break
		}
		rf.mu.Unlock()
		switch rf.state { // 分状态指定需要处理的事情
		case FOLLOWER:
			select {
			case <-rf.heartbeatChan:
				println(fmt.Sprintf("follower %d receive heartbeat", rf.me))
			case <-rf.requestFromCandidateChan: // 接收到了投票请求
			case <-time.After(getElectionTimeout()):
				rf.mu.Lock()
				rf.state = CANDIDATE
				rf.votedFor = rf.me
				rf.mu.Unlock()
			}
		case CANDIDATE:
			rf.startElection()
			select {
			case <-rf.heartbeatChan:
				rf.state = FOLLOWER
			case <-rf.winElectionChan:
				println(fmt.Sprintf("Leader: %d win %d term", rf.me, rf.currentTerm))
			case <-time.After(getElectionTimeout()):
			}
		case LEADER:
			// 发送心跳
			println(fmt.Sprintf("leader %d cycle, term: %d", rf.me, rf.currentTerm))
			rf.broadcastHeartbeat()
			time.Sleep(time.Millisecond * time.Duration(HEARTBEAT_TIMEOUT))
			// 发送日志
		}
	}
}

func (rf *Raft) broadcastHeartbeat() {
	defer rf.mu.Unlock()
	rf.mu.Lock()
	baseIndex := rf.log[0].Index

	if rf.state == LEADER {
		for server := range rf.peers {
			if server != rf.me {
				args := &AppendEntriesArgs{}
				args.LeaderCommit = rf.commitIndex
				args.Term = rf.currentTerm
				args.PrevLogIndex = rf.nextIndex[server] - 1
				args.PrevLogTerm = rf.log[args.PrevLogIndex-baseIndex].Term
				if rf.nextIndex[server] <= rf.getLastLogEntry().Index {
					args.Entries = rf.log[rf.nextIndex[server]-baseIndex:]
				}
				args.LeaderCommit = rf.commitIndex
				args.LeaderID = rf.me
				go rf.sendAppendEntries(server, args, &AppendEntriesReply{})
			}
		}
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	NewtTryIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm { // todo: 什么情况下会出现？
		println(fmt.Sprintf("leader term %d, local term %d", args.Term, rf.currentTerm))
		reply.Term = rf.currentTerm
		reply.NewtTryIndex = rf.getLastLogEntry().Index + 1
		reply.Success = false
		return
	}
	println(fmt.Sprintf("server %d before update term %d: args term - %d", rf.me, rf.currentTerm, args.Term))
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		println(fmt.Sprintf("server %d update to term %d", rf.me, rf.currentTerm))
		rf.state = FOLLOWER
		rf.votedFor = VOTED_NIL
	}

	// 心跳检测
	if args.Entries == nil || len(args.Entries) == 0 { // heartbeat
		rf.heartbeatChan <- true
	}

	reply.Term = rf.currentTerm
	// 数据同步
	lastEntry := rf.getLastLogEntry()
	if args.PrevLogIndex > lastEntry.Index {
		reply.Success = false
		reply.NewtTryIndex = rf.getLastLogEntry().Index + 1
		return
	}

	baseIndex := rf.log[0].Index
	if args.PrevLogIndex >= baseIndex - 1 {
		reply.Success = true
		reply.NewtTryIndex = args.PrevLogIndex + len(args.Entries)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) { // Leader发送心跳或者日志
	println(fmt.Sprintf("sendAppendEntries from %d to %d", rf.me, server))
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if args.Term != rf.currentTerm || rf.state != LEADER {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = VOTED_NIL
			return
		}

		if reply.Success && len(args.Entries) > 0 {
			rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
			rf.matchIndex[server] = args.Entries[len(args.Entries)-1].Index
		}

		if !reply.Success {
			rf.nextIndex[server] = rf.nextIndex[server] - 1
		}
	} else {
		println(fmt.Sprintf("server %d send appendEntries to %d failed", rf.me, server))
	}
}

func (rf *Raft) startElection() {
	args := RequestVoteArgs{}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm++
	println(fmt.Sprintf("server %d start election for term %d", rf.me, rf.currentTerm))
	rf.voteGrantedCnt = 1
	args.Term = rf.currentTerm
	lastEntry := rf.log[len(rf.log)-1]
	args.LastLogTerm = lastEntry.Term
	args.LastLogIndex = lastEntry.Index
	args.CandidateID = rf.me

	for server := range rf.peers {
		if server != rf.me && rf.state == CANDIDATE {
			go rf.sendRequestVote(server, &args, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) getLastLogEntry() LogEntry {
	return rf.log[len(rf.log)-1]
}

func getElectionTimeout() time.Duration {
	timeout := time.Duration(rand.Intn(ELECTION_TIMEOUT_MAX-ELECTION_TIMEOUT_MIN)+ELECTION_TIMEOUT_MIN) * time.Millisecond
	//println(fmt.Sprintf("electionTimeout %d", timeout.Milliseconds()))
	return timeout
}
