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
	//	"bytes"

	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
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

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

type logEntry struct {
	Index   int
	Term    int
	Command interface{}
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
	state       int // 0:follower, 1:candidate, 2: leader
	currentTerm int
	votedFor    int
	lastTime    time.Time
	log         []logEntry
	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg
	nextIndex   []int
	matchIndex  []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) matchLog(prevLogIndex int, prevLogTerm int) bool {
	if len(rf.log)-1 < prevLogIndex {
		return false
	}
	idx, term := rf.log[prevLogIndex].Index, rf.log[prevLogIndex].Term
	DPrintf("[%v]checking if log match,log:%v, idx:%v, prevLogIndex:%v, term:%v, prevLogTerm:%v", rf.me, rf.log, idx, prevLogIndex, term, prevLogTerm)
	if idx != prevLogIndex {
		return false
	}
	return term == prevLogTerm

}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("[%d] got AppendEntries rpc", rf.me)
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		DPrintf("[%v] turn in to follower", rf.me)
	}
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	reply.Term = rf.currentTerm
	rf.lastTime = time.Now()

	lastIndex, _ := rf.getLastLogIndexAndTerm()
	// DPrintf("log len(%v), PrevLogIndex(%v)", len(rf.log)-1, args.PrevLogIndex)
	if !rf.matchLog(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Term, reply.Success = rf.currentTerm, false

		if lastIndex < args.PrevLogIndex {
			// no entry at PrevLogIndex
			DPrintf("[%v] mismatch, no entry at PrevLogIndex(%v), returns", rf.me, args.PrevLogIndex)
			reply.ConflictIndex, reply.ConflictTerm = lastIndex, -1
			return
		} else {
			// mismatch, need to find out the conflict index
			conflictIndex := int(math.Min(float64(lastIndex), float64(args.PrevLogIndex)))
			conflictTerm := rf.log[conflictIndex].Term
			for ; conflictIndex > rf.commitIndex && rf.log[conflictIndex-1].Term == conflictTerm; conflictIndex-- {
			}
			reply.ConflictIndex = int(math.Max(float64(rf.commitIndex+1), float64(conflictIndex+1)))
			DPrintf("[%v] mismatch, has entry at PrevLogIndex(%v), found conflictIndex(%v)", rf.me, args.PrevLogIndex, reply.ConflictIndex)
			return
		}
	}
	firstIndex, _ := rf.getFirstLogIndexAndTerm()
	for i, entry := range args.Entries {
		if entry.Index-firstIndex >= len(rf.log) || rf.log[entry.Index-firstIndex].Term != entry.Term {
			rf.log = append(rf.log[:entry.Index-firstIndex], args.Entries[i:]...)
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		lastIndex, _ = rf.getLastLogIndexAndTerm()
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(lastIndex)))
	}
	DPrintf("[%d] entries appended, entries:%v, log:%v, commitIndex:%v", rf.me, args.Entries, rf.log, rf.commitIndex)
	reply.Success, reply.ConflictIndex = true, -1

}
func (rf *Raft) callAppendEntries(server int, term int, prevLogIndex int, prevLogTerm int, entries []logEntry, leaderCommit int) bool {
	args := AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, entries, leaderCommit}
	// DPrintf("[%d] is leader, sending heartbeat to %d", rf.me, server)
	var reply AppendEntriesReply
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term != reply.Term {
		return false
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
	}
	// append succeded, update nextIndex and matchIndex
	if reply.Success {
		rf.nextIndex[server] += len(entries)
		rf.matchIndex[server] = prevLogIndex + len(entries)
		DPrintf("append succeded, increment nextIndex(%v) and matchIndex(%v)", rf.nextIndex, rf.matchIndex)
		go rf.checkCommit(term)
	} else {
		rf.nextIndex[server] = int(math.Max(1.0, float64(reply.ConflictIndex)))
		DPrintf("append failed, decrement nextIndex(%v)", rf.nextIndex)
	}
	return reply.Success
}
func (rf *Raft) allocateAppendChecker() {
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.appendChecker(server)
	}
}
func (rf *Raft) appendChecker(server int) {
	for {
		rf.mu.Lock()
		if rf.killed() || rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		lastLogIdx := len(rf.log) - 1
		nextIdx := rf.nextIndex[server]
		term := rf.currentTerm
		prevLogIndex := nextIdx - 1
		prevLogTerm := -1
		if prevLogIndex >= 0 {
			prevLogTerm = rf.log[prevLogIndex].Term
		}
		entries := rf.log[nextIdx:]
		leaderCommit := rf.commitIndex
		rf.mu.Unlock()
		if nextIdx <= lastLogIdx {
			DPrintf("[%d] server is behind, calling AppendEntries, term:%d, nextIdx:%d, prevLogIndex:%d, prevLogTerm:%d", server, term, nextIdx, prevLogIndex, prevLogTerm)
			rf.callAppendEntries(server, term, prevLogIndex, prevLogTerm, entries, leaderCommit)
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

func (rf *Raft) checkCommit(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader || rf.currentTerm != term || rf.killed() {
		return
	}
	for i := rf.commitIndex + 1; i < len(rf.log); i++ {
		cnt := 0
		for _, v := range rf.matchIndex {
			if v >= i {
				cnt++
			}
		}
		if cnt >= len(rf.peers)/2+1 {
			DPrintf("%v servers have replicated, leader commit! commitIndex(%v) is now (%v)", cnt, rf.commitIndex, i)
			rf.commitIndex = i
		}
	}
}

func (rf *Raft) Heartbeats() {
	for {
		rf.mu.Lock()
		// DPrintf("[%d] Heartbeating, state:%d", rf.me, rf.state)
		if rf.killed() || rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		term := rf.currentTerm

		leaderCommit := rf.commitIndex
		rf.mu.Unlock()
		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			go func(server int) {
				nextIdx := rf.nextIndex[server]
				prevLogIndex := nextIdx - 1
				prevLogTerm := -1
				if prevLogIndex >= 0 {
					prevLogTerm = rf.log[prevLogIndex].Term
				}
				rf.callAppendEntries(server, term, prevLogIndex, prevLogTerm, make([]logEntry, 0), leaderCommit)
			}(server)
		}

		time.Sleep(time.Duration(150) * time.Millisecond)
	}

}

// candidate won the election, with mutex held
func (rf *Raft) winElection() {
	rf.state = Leader

	// reinitialize nextIndex and matchIndex

	lastLogIndex, _ := rf.getLastLogIndexAndTerm()
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = lastLogIndex + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.matchIndex[rf.me] = lastLogIndex
	DPrintf("[%v] just won the election, initialization finished,log:%v, nextIndex:%v, matchIndex:%v", rf.me, rf.log, rf.nextIndex, rf.matchIndex)
	go rf.Heartbeats()
	go rf.allocateAppendChecker()
}

func (rf *Raft) AttemptElection() {
	rf.mu.Lock()
	DPrintf("[%d] starts Attempt", rf.me)
	rf.currentTerm += 1
	term := rf.currentTerm
	lastLogIndex, lastLogTerm := rf.getLastLogIndexAndTerm()
	rf.lastTime = time.Now()
	rf.votedFor = rf.me
	votes := 1
	rf.state = Candidate
	done := false
	rf.mu.Unlock()
	// var voteMutex sync.Mutex
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			gotVote := rf.CallRequestVote(server, term, lastLogIndex, lastLogTerm)
			if !gotVote {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			votes += 1
			DPrintf("[%d] got vote from %d", rf.me, server)
			if done || votes <= len(rf.peers)/2 {
				return
			}
			if rf.currentTerm != term || rf.state != Candidate {
				return
			}
			DPrintf("[%d] got enough votes, I am now the leader (currentTerm = %d)", rf.me, rf.currentTerm)

			rf.winElection()
			done = true

		}(server)
	}
}

func (rf *Raft) isLogUpToDate(lastLogIndex int, lastLogTerm int) bool {
	idx, term := rf.getLastLogIndexAndTerm()
	if lastLogTerm == term {
		return lastLogIndex >= idx
	}
	return lastLogTerm > term
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("[%v] got RequestVote RPC", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	if !rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	rf.lastTime = time.Now()
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true

}

func (rf *Raft) CallRequestVote(server int, term int, lastLogIndex int, lastLogTerm int) bool {
	args := RequestVoteArgs{term, rf.me, lastLogIndex, lastLogTerm}
	var reply RequestVoteReply
	ok := rf.sendRequestVote(server, &args, &reply)
	if !ok {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term != rf.currentTerm {
		return false
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = Follower
	}
	return reply.VoteGranted
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) apply() {
	for {
		time.Sleep(time.Duration(10) * time.Millisecond)
		rf.mu.Lock()
		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}
		DPrintf("[term:%v]: server[%v] apply log entry, lastApplied:%v, commitIndex:%v,", rf.currentTerm, rf.me, rf.lastApplied, rf.commitIndex)
		rf.lastApplied++
		cmtIdx := rf.lastApplied
		cmd := rf.log[cmtIdx].Command
		rf.mu.Unlock()
		msg := ApplyMsg{
			CommandValid: true,
			Command:      cmd,
			CommandIndex: cmtIdx,
		}
		rf.applyCh <- msg
		DPrintf("append to applyCh successful")
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := rf.nextIndex[rf.me]
	term := rf.currentTerm
	isLeader := rf.state == Leader

	// Your code here (2B).
	if !isLeader || rf.killed() {
		return index, term, isLeader
	}
	rf.log = append(rf.log, logEntry{rf.nextIndex[rf.me], rf.currentTerm, command})
	rf.nextIndex[rf.me]++
	rf.matchIndex[rf.me] = index
	DPrintf("[%d] Leader appended to its log. Current status: leader log: %v, nextIndex:%v", rf.me, rf.log, rf.nextIndex)
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
	for {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		if rf.killed() {
			break
		}
		time.Sleep(time.Duration(50) * time.Millisecond)
		rf.mu.Lock()
		// DPrintf("[%d] ticking... current state:%v", rf.me, rf.state)

		ra := rand.Intn(200) + 300
		// DPrintf("printing time, %v %v", time.Since(rf.lastTime), time.Duration(ra)*time.Millisecond)
		if time.Since(rf.lastTime) > time.Duration(ra)*time.Millisecond && rf.state != Leader {
			DPrintf("election timeouts![%v] AttemptElection!\n", rf.me)
			go rf.AttemptElection()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) getFirstLogIndexAndTerm() (int, int) {
	if len(rf.log) <= 0 {
		return -1, -1
	}
	l := rf.log[0]
	return l.Index, l.Term
}

func (rf *Raft) getLastLogIndexAndTerm() (int, int) {
	if len(rf.log) <= 0 {
		return -1, -1
	}
	l := rf.log[len(rf.log)-1]
	return l.Index, l.Term
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
	// Your initialization code here (2A, 2B, 2C).
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		lastTime:    time.Now(),
		currentTerm: 0,
		dead:        0,
		state:       Follower,
		votedFor:    -1,
		lastApplied: 0,
		applyCh:     applyCh,
		mu:          sync.Mutex{},
		log:         make([]logEntry, 1),
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.apply()
	return rf
}
