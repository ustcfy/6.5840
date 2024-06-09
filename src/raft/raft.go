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
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applycond *sync.Cond
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what state a Raft server must maintain.
	// persistent state on all servers
	currentTerm       int
	votedFor          int
	log               []LogEntry //log[0].Term == LastIncludedTerm
	LastIncludedIndex int
	// volatile state on all servers
	state       string
	commitIndex int
	lastApplied int
	LastHeard   time.Time //LastHeard for followers and candidates
	applyCh     chan ApplyMsg
	// volatile state on leaders
	nextIndex  []int
	matchIndex []int
	// volatile state on followers and candidates
	ElectionTimeout time.Duration
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
	Xterm   int //当前follower的日志中，PrevLogIndex所对应日志项的term
	Xindex  int
}
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}
type InstallSnapshotReply struct {
	Term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == "Leader"
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(snapshot []byte) {
	// Your code here (3C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.LastIncludedIndex)
	raftstate := w.Bytes()
	if snapshot != nil {
		rf.persister.Save(raftstate, snapshot)
	} else {
		rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var LastIncludedIndex int

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&LastIncludedIndex) != nil {
		panic("decode raft state error")
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = make([]LogEntry, len(log))
		copy(rf.log, log)
		rf.LastIncludedIndex = LastIncludedIndex
		rf.commitIndex = LastIncludedIndex
		rf.applycond.Signal()
		Debug(dDrop, "S%d readPersist: term=%v, lastIncludedIndex=%v", rf.me, rf.currentTerm, rf.LastIncludedIndex)
	}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 1. 如果term < currentTerm返回false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist(nil)
		rf.state = "Follower"
	}
	reply.Term = rf.currentTerm
	// 2. 如果votedFor为空或者为candidateId，并且候选人的日志至少和自己一样新，投票
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.LastIncludedIndex+len(rf.log)-1)) {
		rf.votedFor = args.CandidateId
		rf.persist(nil)
		rf.LastHeard = time.Now()
		reply.VoteGranted = true
		return
	}
	// 3. 否则拒绝投票
	reply.VoteGranted = false
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) updateCommitIndex() {
	//如果存在一个满足N > commitIndex的N，并且大多数的matchIndex[i] ≥ N，且log[N].term == currentTerm，将commitIndex更新为这个N
	temp := make([]int, len(rf.matchIndex))
	copy(temp, rf.matchIndex)
	temp[rf.me] = rf.LastIncludedIndex + len(rf.log) - 1
	sort.Ints(temp)
	N := temp[(len(temp)-1)/2]
	if N > rf.commitIndex && rf.log[N-rf.LastIncludedIndex].Term == rf.currentTerm {
		rf.commitIndex = N
		Debug(dLeader, "S%d leader_commit=%v", rf.me, rf.commitIndex)
		rf.applycond.Signal()
	}
}

func (rf *Raft) startApendEntries(i int) {
	rf.mu.Lock()
	if rf.state != "Leader" {
		rf.mu.Unlock()
		return
	}
	if rf.nextIndex[i] <= rf.LastIncludedIndex {
		reply := &InstallSnapshotReply{}
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.LastIncludedIndex,
			LastIncludedTerm:  rf.log[0].Term,
			Data:              make([]byte, rf.persister.SnapshotSize()),
		}
		copy(args.Data, rf.persister.ReadSnapshot())
		rf.mu.Unlock()
		if rf.sendInstallSnapshot(i, args, reply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist(nil)
				rf.state = "Follower"
			} else if rf.currentTerm == args.Term {
				rf.nextIndex[i] = args.LastIncludedIndex + 1
				if args.LastIncludedIndex > rf.matchIndex[i] {
					rf.matchIndex[i] = args.LastIncludedIndex
					rf.updateCommitIndex()
				}
			}
		}
	} else {
		reply := &AppendEntriesReply{}
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[i] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[i]-1-rf.LastIncludedIndex].Term,
			LeaderCommit: rf.commitIndex,
		}
		if rf.LastIncludedIndex+len(rf.log)-1 >= rf.nextIndex[i] {
			args.Entries = make([]LogEntry, len(rf.log[rf.nextIndex[i]-rf.LastIncludedIndex:]))
			copy(args.Entries, rf.log[rf.nextIndex[i]-rf.LastIncludedIndex:])
		} else {
			args.Entries = make([]LogEntry, 0) //args.Entries empty for heartbeat
		}
		rf.mu.Unlock()
		if rf.sendAppendEntries(i, args, reply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist(nil)
				rf.state = "Follower"
			} else if rf.currentTerm == args.Term {
				if reply.Success { //如果成功，更新nextIndex和matchIndex
					rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
					if args.PrevLogIndex+len(args.Entries) > rf.matchIndex[i] {
						rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
						rf.updateCommitIndex()
					}
				} else if reply.Xterm >= 0 && reply.Xindex >= rf.LastIncludedIndex && rf.log[reply.Xindex-rf.LastIncludedIndex].Term == reply.Xterm { //leader has XTerm
					rf.nextIndex[i] = rf.LastIncludedIndex + len(rf.log)
					for j := reply.Xindex + 1; j < rf.LastIncludedIndex+len(rf.log); j++ {
						if rf.log[j-rf.LastIncludedIndex].Term != reply.Xterm {
							rf.nextIndex[i] = j
							break
						}
					}
				} else { //other cases
					rf.nextIndex[i] = reply.Xindex
				}
			}
		}
	}
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Xterm = -1
	reply.Xindex = -1
	reply.Success = false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.LastHeard = time.Now()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist(nil)
		rf.state = "Follower"
	} else if rf.state == "Candidate" {
		rf.state = "Follower"
	}
	reply.Term = rf.currentTerm
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex > rf.LastIncludedIndex+len(rf.log)-1 { //case 1: follower's log is too short
		reply.Xindex = rf.LastIncludedIndex + len(rf.log)
		return
	} else if args.PrevLogIndex < rf.LastIncludedIndex { //case 2: follower has trimed log[args.PrevLogIndex]
		reply.Xindex = rf.LastIncludedIndex + 1
		return
	} else if rf.log[args.PrevLogIndex-rf.LastIncludedIndex].Term != args.PrevLogTerm {
		reply.Xterm = rf.log[args.PrevLogIndex-rf.LastIncludedIndex].Term
		reply.Xindex = rf.LastIncludedIndex + 1 //case 3: the first index's term is equal to reply.Xterm
		for i := args.PrevLogIndex - 1; i >= rf.LastIncludedIndex; i-- {
			if rf.log[i-rf.LastIncludedIndex].Term != reply.Xterm {
				reply.Xindex = i + 1 //case 4: find the first index whose term is not equal to reply.Xterm
				break
			}
		}
		return
	}
	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	for i := args.PrevLogIndex + 1; i < rf.LastIncludedIndex+len(rf.log) && i <= args.PrevLogIndex+len(args.Entries); i++ {
		if rf.log[i-rf.LastIncludedIndex].Term != args.Entries[i-args.PrevLogIndex-1].Term {
			rf.log = rf.log[:i-rf.LastIncludedIndex]
			break
		}
	}
	// Append any new entries not already in the log
	if rf.LastIncludedIndex+len(rf.log)-1 < args.PrevLogIndex+len(args.Entries) {
		rf.log = append(rf.log, args.Entries[rf.LastIncludedIndex+len(rf.log)-args.PrevLogIndex-1:]...)
	}
	rf.persist(nil)
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		if rf.LastIncludedIndex+len(rf.log)-1 >= args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
			Debug(dCommit, "S%d commit=%v", rf.me, rf.commitIndex)
			rf.applycond.Signal()
		} else if rf.LastIncludedIndex+len(rf.log)-1 > rf.commitIndex {
			rf.commitIndex = rf.LastIncludedIndex + len(rf.log) - 1
			Debug(dCommit, "S%d commit=%v", rf.me, rf.commitIndex)
			rf.applycond.Signal()
		}
	}
	reply.Success = true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//只能拍摄比已有的snapshot更新的snapshot; 只能拍摄已经提交的日志
	if index <= rf.LastIncludedIndex || index > rf.commitIndex {
		return
	}
	//更新snapshot，截断日志
	rf.log = append([]LogEntry{{rf.log[index-rf.LastIncludedIndex].Term, nil}}, rf.log[index+1-rf.LastIncludedIndex:]...)
	rf.LastIncludedIndex = index
	rf.persist(snapshot)
}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	rf.LastHeard = time.Now()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist(nil)
		rf.state = "Follower"
	} else if rf.state == "Candidate" {
		rf.state = "Follower"
	}
	reply.Term = rf.currentTerm
	//If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
	if args.LastIncludedIndex >= rf.LastIncludedIndex && rf.LastIncludedIndex+len(rf.log)-1 >= args.LastIncludedIndex && rf.log[args.LastIncludedIndex-rf.LastIncludedIndex].Term == args.LastIncludedTerm {
		rf.log = append([]LogEntry{{args.LastIncludedTerm, nil}}, rf.log[args.LastIncludedIndex-rf.LastIncludedIndex+1:]...)
	} else { //Discard the entire log
		rf.log = make([]LogEntry, 1)
		rf.log[0] = LogEntry{args.LastIncludedTerm, nil}
	}
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.persist(args.Data)
	//Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
		Debug(dCommit, "S%d commit=%v", rf.me, rf.commitIndex)
		rf.applycond.Signal()
	}
	rf.mu.Unlock()
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果不是Leader，返回false
	if rf.state != "Leader" {
		return -1, rf.currentTerm, false
	}
	// 如果是Leader，追加日志
	rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
	rf.persist(nil)
	return rf.LastIncludedIndex + len(rf.log) - 1, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) resetElectionTimeout() {
	rf.LastHeard = time.Now()
	rf.ElectionTimeout = time.Duration(250+rand.Intn(250)) * time.Millisecond
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != "Candidate" {
		return
	}
	rf.currentTerm++    // 1. 递增当前的任期
	rf.votedFor = rf.me // 2. 给自己投票
	rf.persist(nil)
	votes := 1
	Debug(dCandidate, "S%d term=%v start election", rf.me, rf.currentTerm)
	rf.resetElectionTimeout() // 3. 重置选举超时
	// 4. 发送投票请求
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(i int, votes *int) {
				rf.mu.Lock()
				if rf.state != "Candidate" {
					rf.mu.Unlock()
					return
				}
				reply := &RequestVoteReply{}
				args := &RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.LastIncludedIndex + len(rf.log) - 1,
					LastLogTerm:  rf.log[len(rf.log)-1].Term,
				}
				rf.mu.Unlock()
				if rf.sendRequestVote(i, args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist(nil)
						rf.state = "Follower"
					} else if rf.currentTerm == args.Term && rf.state == "Candidate" && reply.VoteGranted {
						*votes++
						if *votes == len(rf.peers)/2+1 {
							rf.state = "Leader"
							Debug(dLeader, "S%d term=%v become Leader", rf.me, rf.currentTerm)
							for j := 0; j < len(rf.peers); j++ {
								rf.nextIndex[j] = rf.LastIncludedIndex + len(rf.log)
								rf.matchIndex[j] = 0
							}
							go rf.ping()
						}
					}
				}
			}(i, &votes)
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state == "Follower" && time.Since(rf.LastHeard) > rf.ElectionTimeout {
			rf.state = "Candidate"
			go rf.startElection()
		} else if rf.state == "Candidate" && time.Since(rf.LastHeard) > rf.ElectionTimeout {
			go rf.startElection()
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350 milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

//leader发送心跳
func (rf *Raft) ping() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != "Leader" {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go rf.startApendEntries(i)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

//it is important that you ensure that this application is only done by one entity.
//Specifically, you will need to either have a dedicated “applier”,
//or to lock around these applies,
//so that some other routine doesn’t also detect that entries need to be applied and also tries to apply.
func (rf *Raft) applier() {
	for !rf.killed() {
		//for all servers
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applycond.Wait()
		}
		msgs := make([]ApplyMsg, rf.commitIndex-rf.lastApplied)
		for rf.lastApplied < rf.commitIndex {
			if rf.lastApplied < rf.LastIncludedIndex {
				rf.lastApplied = rf.LastIncludedIndex
				msg := ApplyMsg{false, nil, 0, true, nil, rf.log[0].Term, rf.LastIncludedIndex}
				msg.Snapshot = make([]byte, rf.persister.SnapshotSize())
				copy(msg.Snapshot, rf.persister.ReadSnapshot())
				Debug(dSnap, "S%d installSnapshot %d", rf.me, msg.SnapshotIndex)
				msgs = append(msgs, msg)
			} else {
				rf.lastApplied++
				msg := ApplyMsg{true, rf.log[rf.lastApplied-rf.LastIncludedIndex].Command, rf.lastApplied, false, nil, 0, 0}
				Debug(dApply, "S%d apply %d", rf.me, msg.CommandIndex)
				msgs = append(msgs, msg)
			}
		}
		rf.mu.Unlock()
		for _, msg := range msgs {
			rf.applyCh <- msg
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	// Your initialization code here (3A, 3B, 3C).
	rf := &Raft{
		peers:             peers,
		persister:         persister,
		me:                me,
		mu:                sync.Mutex{},
		votedFor:          -1,
		currentTerm:       0,
		LastIncludedIndex: 0,
		commitIndex:       0,
		lastApplied:       0,
		state:             "Follower",
		applyCh:           applyCh,
		log:               make([]LogEntry, 1),
		nextIndex:         make([]int, len(peers)),
		matchIndex:        make([]int, len(peers)),
	}
	rf.applycond = sync.NewCond(&rf.mu)
	rf.log[0] = LogEntry{0, nil}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	rf.resetElectionTimeout()
	go rf.ticker()
	go rf.applier()
	return rf
}
