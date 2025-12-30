package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"fmt"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type raftState string

const (
	leader    raftState = "leader"
	candidate raftState = "candidate"
	follower  raftState = "follower"
)

type LogEntry struct {
	Data  interface{}
	Index int
	Term  int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's raftState
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted raftState
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Persistent states
	currentTerm int
	votedFor    int
	logs        []LogEntry

	// Volatile states
	commitIndex int
	lastApplied int
	state       raftState
	beat        atomic.Bool

	// Leader states
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == leader
}

// save Raft's persistent raftState to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted raftState.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any raftState?
		return
	}
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	Candidate    int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) acceptVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.votedFor = args.Candidate
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
}

// rejectVote must be used with lock
func (rf *Raft) rejectVote(reply *RequestVoteReply) {
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.maybeBecomeFollower(args.Term)
	switch {
	case args.Term < rf.currentTerm:
		rf.rejectVote(reply)
	case args.LastLogIndex < len(rf.logs)-1:
		rf.rejectVote(reply)
	case rf.votedFor == -1 || rf.votedFor == args.Candidate:
		rf.beat.Store(true)
		rf.acceptVote(args, reply)
	default:
		rf.rejectVote(reply)
	}
}

// getLastLogIdxTerm must be used with lock
// Get the last log index and term
func (rf *Raft) getLastLogIdxTerm() (int, int) {
	idx := len(rf.logs) - 1
	term := 0
	if idx != -1 {
		term = rf.logs[idx].Term
	}
	return idx, term
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

// sendRequestVoteToAll returns condition variable, votes, done
// The condition variable broadcasts when a vote request is done
// votes holds the number of granted vote
// done holds the number of finished request
func (rf *Raft) sendRequestVoteToAll(args RequestVoteArgs, replies []RequestVoteReply) (*sync.Cond, *int, *int) {
	cond := sync.Cond{L: &rf.mu}
	votes := 1
	done := 1
	for i := range len(rf.peers) {
		if i == rf.me {
			continue
		}
		go func(i int) {
			ok := rf.sendRequestVote(i, &args, &replies[i])
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.maybeBecomeFollower(replies[i].Term) {
				return
			}
			if ok && replies[i].VoteGranted {
				votes++
			}
			done++
			cond.Broadcast()
		}(i)
	}
	return &cond, &votes, &done
}

type AppendEntriesArgs struct {
	Term         int
	Leader       int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.maybeBecomeFollower(args.Term)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.beat.Store(true)
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) startHeartbeatLoop() {
	defer tester.Annotate(fmt.Sprintf("Server %v", rf.me), "stops beating", "stops beating")
	for !rf.killed() {
		for i := range len(rf.peers) {
			if i == rf.me {
				continue
			}
			rf.mu.Lock()
			notLeader := rf.state != leader
			rf.mu.Unlock()
			if notLeader {
				return
			}
			go func(i int) {
				rf.mu.Lock()
				idx, term := rf.getLastLogIdxTerm()
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					Leader:       rf.me,
					PrevLogIndex: idx,
					PrevLogTerm:  term,
					Entries:      rf.logs[idx+1:],
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{}
				rf.mu.Unlock()
				if rf.sendAppendEntries(i, &args, &reply) {
					rf.mu.Lock()
					rf.maybeBecomeFollower(reply.Term)
					rf.mu.Unlock()
				}
			}(i)
		}
		time.Sleep(100 * time.Millisecond)
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
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

// prepareElection must be used with lock
func (rf *Raft) prepareElection() (RequestVoteArgs, []RequestVoteReply) {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = candidate
	idx, term := rf.getLastLogIdxTerm()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		Candidate:    rf.votedFor,
		LastLogIndex: idx,
		LastLogTerm:  term,
	}
	replies := make([]RequestVoteReply, len(rf.peers))
	return args, replies
}

// maybeBecomeFollower must be used with lock
func (rf *Raft) maybeBecomeFollower(term int) bool {
	if term > rf.currentTerm {
		tester.Annotate(fmt.Sprintf("Server %v", rf.me), "-> follower", "becomes follower")
		rf.beat.Store(true)
		rf.state = follower
		rf.currentTerm = term
		rf.votedFor = -1
		return true
	}
	return false
}

// becomeLeader must be used with lock
func (rf *Raft) becomeLeader() {
	tester.Annotate(fmt.Sprintf("Server %v", rf.me), "-> leader", "becomes leader")
	rf.state = leader
	lastLogIndex, _ := rf.getLastLogIdxTerm()
	for i := range len(rf.peers) {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
	go rf.startHeartbeatLoop()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	tester.Annotate(fmt.Sprintf("Server %v", rf.me), "-> candidate", "becomes candidate")
	args, replies := rf.prepareElection()
	currentTerm := rf.currentTerm
	quorum := len(rf.peers)
	majority := quorum / 2
	rf.mu.Unlock()
	cond, votes, done := rf.sendRequestVoteToAll(args, replies)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	for *votes <= majority && *done < quorum {
		cond.Wait()
	}

	if currentTerm != rf.currentTerm || rf.state != candidate {
		return
	}
	if *votes > majority {
		rf.becomeLeader()
	}
}

func (rf *Raft) startElectionLoop() {
	for rf.killed() == false {
		if !rf.beat.Load() {
			rf.mu.Lock()
			notLeader := rf.state != leader
			rf.mu.Unlock()
			if notLeader {
				go rf.startElection()
			}
		} else {
			rf.beat.Store(false)
		}

		ms := 300 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0)
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.state = follower
	rf.beat.Store(false)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start startElectionLoop goroutine to start elections
	go rf.startElectionLoop()

	return rf
}
