package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"fmt"

	//	"bytes"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

const (
	leader           raftState = "leader"
	candidate        raftState = "candidate"
	follower         raftState = "follower"
	heartbeatTimeout           = 100 * time.Millisecond
)

type LogEntry struct {
	Data  interface{}
	Index int
	Term  int
}

type Snapshot struct {
	Data      []byte
	LastIndex int
	LastTerm  int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted raftState
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan raftapi.ApplyMsg

	// Persistent states
	currentTerm int
	votedFor    int
	logs        []LogEntry
	snapshot    Snapshot

	// Volatile states
	commitIndex    int
	lastApplied    int
	state          raftState
	stateF         stateFunc
	eQ             chan raftEvent
	killedChan     chan struct{}
	grantedVote    int
	electionTimer  <-chan time.Time
	heartbeatTimer <-chan time.Time

	// Leader states
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (r *Raft) GetState() (int, bool) {
	done := make(chan struct {
		isLeader bool
		term     int
	})
	r.eQ <- &getStateEvent{done: done}
	result := <-done
	return result.term, result.isLeader
}

// save Raft's persistent raftState to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
//
// Must be used with lock
func (r *Raft) persist() {
	b := new(bytes.Buffer)
	e := labgob.NewEncoder(b)
	err := e.Encode(r.currentTerm)
	if err != nil {
		panic(err)
	}
	err = e.Encode(r.votedFor)
	if err != nil {
		panic(err)
	}
	err = e.Encode(r.logs)
	if err != nil {
		panic(err)
	}
	err = e.Encode(r.snapshot.LastIndex)
	if err != nil {
		panic(err)
	}
	err = e.Encode(r.snapshot.LastTerm)
	if err != nil {
		panic(err)
	}
	s := b.Bytes()
	r.persister.Save(s, r.snapshot.Data)
}

// restore previously persisted raftState.
func (r *Raft) readPersist(data []byte) {
	if len(data) > 0 {
		b := bytes.NewBuffer(data)
		d := labgob.NewDecoder(b)
		var currentTerm, votedFor, sIdx, sTerm int
		var logs []LogEntry
		err := d.Decode(&currentTerm)
		if err != nil {
			panic(err)
		}
		err = d.Decode(&votedFor)
		if err != nil {
			panic(err)
		}
		err = d.Decode(&logs)
		if err != nil {
			panic(err)
		}
		err = d.Decode(&sIdx)
		if err != nil {
			panic(err)
		}
		err = d.Decode(&sTerm)
		if err != nil {
			panic(err)
		}
		data = r.persister.ReadSnapshot()
		r.currentTerm = currentTerm
		r.votedFor = votedFor
		r.logs = logs
		r.snapshot = Snapshot{data, sIdx, sTerm}
	}
}

// how many bytes in Raft's persisted log?
func (r *Raft) PersistBytes() int {
	done := make(chan int)
	r.eQ <- &PersistBytesEvent{done: done}
	return <-done
}

type InstallSnapshotArgs struct {
	Snapshot Snapshot
	Term     int
}

type InstallSnapshotReply struct {
	Success bool
	Term    int
}

func (r *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	done := make(chan struct{})
	r.eQ <- &installSnapshotEvent{args: args, reply: reply, done: done}
	<-done
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (r *Raft) Snapshot(index int, snapshot []byte) {
	done := make(chan struct{})
	r.eQ <- &snapshotEvent{index: index, snapshot: snapshot, done: done}
	<-done
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

// isLogUpToDate checks whether the argument idx and term is up to local state.
func (r *Raft) isLogUpToDate(idx int, term int) bool {
	last := len(r.logs) - 1
	myIdx := r.logs[last].Index
	myTerm := r.logs[last].Term
	switch {
	case myTerm > term:
		return false
	case myTerm < term:
		return true
	default:
		return myIdx <= idx
	}
}

// example RequestVote RPC handler.
func (r *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	done := make(chan struct{})
	r.eQ <- &requestVoteEvent{args: args, reply: reply, done: done}
	<-done
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
	Term         int
	PrevLogIndex int
	XTerm        int // conflicting term
	XIndex       int // conflicting index
	Success      bool
}

func (r *AppendEntriesReply) isFailedBecauseOldTerm() bool {
	return r.PrevLogIndex == -1
}

func (r *AppendEntriesReply) isFailedBecauseFarIndex() bool {
	return !r.isFailedBecauseOldTerm() && r.XTerm == -1 && r.XIndex == -1
}

func (r *AppendEntriesReply) isFailedBecauseWrongTerm() bool {
	return r.XTerm != -1 && r.XIndex != -1
}

func (r *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	done := make(chan struct{})
	r.eQ <- &appendEntriesEvent{args: args, reply: reply, done: done}
	<-done
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
func (r *Raft) Start(command interface{}) (int, int, bool) {
	done := make(chan struct {
		index    int
		term     int
		isLeader bool
	})
	r.eQ <- &startEvent{command: command, done: done}
	result := <-done
	return result.index, result.term, result.isLeader
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
func (r *Raft) Kill() {
	r.killedChan <- struct{}{}
	atomic.StoreInt32(&r.dead, 1)
	// Your code here, if desired.
}

func (r *Raft) killed() bool {
	z := atomic.LoadInt32(&r.dead)
	return z == 1
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
	r := &Raft{}
	r.peers = peers
	r.persister = persister
	r.me = me
	r.dead = 0
	r.applyCh = applyCh

	r.currentTerm = 0
	r.votedFor = -1
	r.logs = []LogEntry{{Term: 0, Index: 0}} // Sentinel

	r.commitIndex = 0
	r.lastApplied = 0
	r.state = follower
	r.stateF = stateFollower
	r.eQ = make(chan raftEvent)
	r.killedChan = make(chan struct{})
	r.grantedVote = 0

	r.nextIndex = make([]int, len(r.peers))
	r.matchIndex = make([]int, len(r.peers))

	// initialize from state persisted before a crash
	r.readPersist(persister.ReadRaftState())

	// start event loop
	go func() {
		for r.stateF != nil {
			r.stateF = r.stateF(r)
			tester.Annotate(fmt.Sprintf("Server %v", r.me), "state", string(r.state))
		}
	}()

	return r
}
