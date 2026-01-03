package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"fmt"
	"slices"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type raftState string

const (
	leader           raftState = "leader"
	candidate        raftState = "candidate"
	follower         raftState = "follower"
	heartbeatTimeout           = 100 * time.Millisecond
	commitTimeout              = 200 * time.Millisecond
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
	mu        sync.Mutex          // Lock to protect shared access to this peer's raftState
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
//
// Must be used with lock
func (rf *Raft) persist() {
	b := new(bytes.Buffer)
	e := labgob.NewEncoder(b)
	err := e.Encode(rf.currentTerm)
	if err != nil {
		panic(err)
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		panic(err)
	}
	err = e.Encode(rf.logs)
	if err != nil {
		panic(err)
	}
	err = e.Encode(rf.snapshot)
	if err != nil {
		panic(err)
	}
	s := b.Bytes()
	rf.persister.Save(s, nil)
}

// restore previously persisted raftState.
func (rf *Raft) readPersist(data []byte) {
	if len(data) > 0 {
		b := bytes.NewBuffer(data)
		d := labgob.NewDecoder(b)
		var currentTerm, votedFor int
		var logs []LogEntry
		var snapshot Snapshot
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
		err = d.Decode(&snapshot)
		if err != nil {
			panic(err)
		}
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.snapshot = snapshot
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

type InstallSnapshotArgs struct {
	Snapshot Snapshot
	Term     int
}

type InstallSnapshotReply struct {
	Success bool
	Term    int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.maybeBecomeFollower(args.Term)
	rf.beat.Store(true)

	s := args.Snapshot
	lIdx, p := rf.getIdxInLogs(s.LastIndex)
	if p == after {
		rf.snapshot = s
		rf.logs = []LogEntry{{Index: s.LastIndex, Term: s.LastTerm}}
		if rf.commitIndex < s.LastIndex {
			rf.commitIndex = s.LastIndex
		}
		rf.persist()
		reply.Success = true
		return
	}

	if p == inside {
		rf.snapshot = s
		if rf.logs[lIdx].Term == s.LastTerm {
			rf.cutLogs(s.LastIndex-1, -1)
		} else {
			rf.logs = []LogEntry{{Index: s.LastIndex, Term: s.LastTerm}}
		}
		if rf.commitIndex < s.LastIndex {
			rf.commitIndex = s.LastIndex
		}
		rf.persist()
		reply.Success = true
		return
	}

	reply.Success = false
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index > rf.lastApplied {
		panic(fmt.Sprintf("snapshot index out of range: %v", index))
	}
	if index <= rf.snapshot.LastIndex {
		return
	}
	lIdx, _ := rf.getIdxInLogs(index) // must be in range
	s := Snapshot{Data: snapshot, LastIndex: index, LastTerm: rf.logs[lIdx].Term}
	rf.cutLogs(-1, index)
	rf.snapshot = s
	rf.persist()
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
	rf.persist()
}

// rejectVote must be used with lock
func (rf *Raft) rejectVote(reply *RequestVoteReply) {
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

// isLogUpToDate checks whether the argument idx and term is up to local state. Must be used with lock.
func (rf *Raft) isLogUpToDate(idx int, term int) bool {
	myIdx, myTerm := rf.getLastLogIdxTerm()
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
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.maybeBecomeFollower(args.Term)
	switch {
	case args.Term < rf.currentTerm:
		rf.rejectVote(reply)
	case (rf.votedFor == -1 || rf.votedFor == args.Candidate) && rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm):
		rf.beat.Store(true)
		rf.acceptVote(args, reply)
	default:
		rf.rejectVote(reply)
	}
}

const (
	before = iota
	inside
	after
)

// getIdxInLogs finds the real index of a log entry in logs.
// It must be used with lock.
// idx is the index of the target entry.
// The first return value is the real index. The second return value indicates whether idx exists in logs.
//
// If idx < first index in logs -> before, first index <= idx <= last index -> inside, last index < idx -> after.
func (rf *Raft) getIdxInLogs(idx int) (int, int) {
	l := rf.logs[0].Index
	r := rf.logs[len(rf.logs)-1].Index
	if idx <= l {
		return -1, before
	} else if idx > r {
		return -1, after
	}
	return idx - l, inside
}

// getTerm must be called with lock.
// Return term, exist
func (rf *Raft) getTerm(idx int) (int, bool) {
	idx, p := rf.getIdxInLogs(idx)
	switch p {
	case after:
		return -1, false
	case inside:
		return rf.logs[idx].Term, true
	case before:
		if rf.snapshot.LastIndex == idx {
			return rf.snapshot.LastTerm, true
		}
		return -1, false
	default:
		panic("invalid position")
	}
}

// Cut logs so the result logs is [l:r). l and r are LogEntry indices, not indices in logs.
// Set l, r to -1 to let them unbounded.
// It must be used with lock.
func (rf *Raft) cutLogs(l int, r int) {
	if l == -1 && r == -1 {
		return
	} else if l == -1 {
		logsR, ok := rf.getIdxInLogs(r)
		if ok != inside {
			panic(fmt.Sprintf("invalid cut index r: %v, position: %v", r, ok))
		}
		rf.logs = rf.logs[:logsR]
	} else if r == -1 {
		logsL, ok := rf.getIdxInLogs(l)
		if ok != inside {
			panic(fmt.Sprintf("invalid cut index l: %v, position: %v", l, ok))
		}
		rf.logs = rf.logs[logsL:]
	} else {
		logsL, ok := rf.getIdxInLogs(l)
		if ok != inside {
			panic(fmt.Sprintf("invalid cut index l: %v, position: %v", l, ok))
		}
		logsR, ok := rf.getIdxInLogs(r)
		if ok != inside {
			panic(fmt.Sprintf("invalid cut index r: %v, position: %v", r, ok))
		}
		rf.logs = rf.logs[logsL:logsR]
	}
}

// getLastLogIdxTerm must be used with lock
// Get the last log index and term
func (rf *Raft) getLastLogIdxTerm() (int, int) {
	idx := rf.logs[len(rf.logs)-1].Index
	term := rf.logs[len(rf.logs)-1].Term
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
// handler function on the server side does not return. Thus, there
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
				rf.beat.Store(true)
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
	Term         int
	PrevLogIndex int
	MatchIndex   int
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

// rejectAppendEntries must be used with lock
func (rf *Raft) rejectAppendEntries(reply *AppendEntriesReply, idx int, xTerm int, xIndex int) {
	reply.Term = rf.currentTerm
	reply.PrevLogIndex = idx
	reply.MatchIndex = -1
	reply.XTerm = xTerm
	reply.XIndex = xIndex
	reply.Success = false
}

func (rf *Raft) rejectAppendEntriesOldTerm(reply *AppendEntriesReply) {
	rf.rejectAppendEntries(reply, -1, -1, -1)
}

func (rf *Raft) rejectAppendEntriesFarIndex(reply *AppendEntriesReply) {
	lastIdx, _ := rf.getLastLogIdxTerm()
	rf.rejectAppendEntries(reply, lastIdx, -1, -1)
}

func (rf *Raft) rejectAppendEntriesWrongTerm(reply *AppendEntriesReply, xTerm int, xIndex int) {
	rf.rejectAppendEntries(reply, xIndex, xTerm, xIndex)
}

// trimAppendEntries removes entries that are in the snapshot.
// It must be used with lock.
// Return the size of the result entries.
func (rf *Raft) trimAppendEntries(args *AppendEntriesArgs) int {
	s := len(args.Entries)
	if s == 0 {
		return 0
	}
	l := args.Entries[0].Index
	r := args.Entries[s-1].Index
	_, p := rf.getIdxInLogs(r)
	if p == before {
		args.Entries = []LogEntry{}
		return 0
	}
	d := rf.snapshot.LastIndex - l
	if d >= 0 {
		if d+1 >= s {
			args.Entries = []LogEntry{}
			return 0
		}
		args.PrevLogIndex = args.Entries[d].Index
		args.PrevLogTerm = args.Entries[d].Term
		args.Entries = args.Entries[d+1:]
		return len(args.Entries)
	}
	return s
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.maybeBecomeFollower(args.Term) {
		rf.beat.Store(true)
	}
	lastIdx, _ := rf.getLastLogIdxTerm()
	if args.Term < rf.currentTerm {
		rf.rejectAppendEntriesOldTerm(reply)
		return
	}
	rf.beat.Store(true)
	if lastIdx < args.PrevLogIndex {
		rf.rejectAppendEntriesFarIndex(reply)
		tester.Annotate(fmt.Sprintf("Server %v", rf.me), "rejects append", "far index")
		return
	}
	if rf.trimAppendEntries(args) == 0 {
		reply.Term = rf.currentTerm
		reply.Success = true
		reply.PrevLogIndex = rf.snapshot.LastIndex
		reply.MatchIndex = rf.snapshot.LastIndex
	}
	if t, _ := rf.getTerm(args.PrevLogIndex); t != args.PrevLogTerm {
		xTerm := t
		xIdx := args.PrevLogIndex
		for ; xIdx >= 1; xIdx-- {
			if xT, ok := rf.getTerm(xIdx - 1); !ok || xT != xTerm {
				break
			}
		}
		rf.rejectAppendEntriesWrongTerm(reply, xTerm, xIdx)
		tester.Annotate(fmt.Sprintf("Server %v", rf.me), "rejects append", "incorrect term")
		return
	}
	updatedIdx := args.PrevLogIndex
	if len(args.Entries) > 0 {
		updatedIdx = args.Entries[len(args.Entries)-1].Index
	}
	for i, entry := range args.Entries {
		if entry.Index > lastIdx {
			rf.logs = append(rf.logs, args.Entries[i:]...)
			tester.Annotate(fmt.Sprintf("Server %v", rf.me), "appends", fmt.Sprintf("appends left: %v right: %v", entry.Index, args.Entries[len(args.Entries)-1].Index))
			break
		} else if t, _ := rf.getTerm(entry.Index); entry.Term != t {
			tester.Annotate(fmt.Sprintf("Server %v", rf.me), "appends", fmt.Sprintf("appends left: %v right: %v", entry.Index, args.Entries[len(args.Entries)-1].Index))
			rf.cutLogs(-1, entry.Index)
			rf.logs = append(rf.logs, args.Entries[i:]...)
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if updatedIdx < args.LeaderCommit {
			rf.commitIndex = updatedIdx
		}
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	reply.PrevLogIndex = updatedIdx
	reply.MatchIndex = updatedIdx
	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// prepareAppendEntries must be used with lock
func (rf *Raft) prepareAppendEntries(server int) (AppendEntriesArgs, AppendEntriesReply) {
	idx := rf.nextIndex[server] - 1
	term, _ := rf.getTerm(idx)
	nextLogIdx, _ := rf.getIdxInLogs(rf.nextIndex[server])
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		Leader:       rf.me,
		PrevLogIndex: idx,
		PrevLogTerm:  term,
		Entries:      rf.logs[nextLogIdx:],
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	return args, reply
}

// handleAppendEntriesReply must be used with lock
func (rf *Raft) handleAppendEntriesReply(server int, reply AppendEntriesReply) {
	switch {
	case rf.maybeBecomeFollower(reply.Term):
		rf.beat.Store(true)
	case reply.Success:
		tester.Annotate(fmt.Sprintf("Server %v", rf.me), "append successes", fmt.Sprintf("server: %v, next: %v, match: %v", server, reply.PrevLogIndex+1, reply.MatchIndex))
		if rf.matchIndex[server] < reply.MatchIndex {
			rf.matchIndex[server] = reply.MatchIndex
			rf.nextIndex[server] = reply.PrevLogIndex + 1
		}
	case reply.isFailedBecauseFarIndex():
		rf.nextIndex[server] = reply.PrevLogIndex + 1
	case reply.isFailedBecauseWrongTerm():
		idx := -1
		for i := len(rf.logs) - 1; i >= 0; i-- {
			if rf.logs[i].Term == reply.XTerm {
				idx = i
				break
			}
		}
		if idx == -1 {
			rf.nextIndex[server] = reply.XIndex
		} else {
			rf.nextIndex[server] = idx + 1
		}
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
}

func (rf *Raft) tryAppendEntries(server int) {
	rf.mu.Lock()
	if rf.nextIndex[server] <= rf.snapshot.LastIndex {
		args := InstallSnapshotArgs{Snapshot: rf.snapshot, Term: rf.currentTerm}
		reply := InstallSnapshotReply{}
		rf.mu.Unlock()
	} else {
		args, reply := rf.prepareAppendEntries(server)
		rf.mu.Unlock()
		if rf.sendAppendEntries(server, &args, &reply) {
			rf.mu.Lock()
			rf.handleAppendEntriesReply(server, reply)
			rf.mu.Unlock()
		}
	}
}

// startAppendEntriesLoop starts a loop that sends an AppendEntries request to server.
// Since Call is blocking, requests are sent in a parallelized manner.
func (rf *Raft) startAppendEntriesLoop(server int) {
	for !rf.killed() {
		if _, isLeader := rf.GetState(); isLeader {
			go rf.tryAppendEntries(server)
		}
		time.Sleep(heartbeatTimeout)
	}
}

// maybeCommit tries to commit log at idx. It must be used with lock
func (rf *Raft) maybeCommit(idx int) bool {
	if rf.logs[idx].Term != rf.currentTerm {
		return false
	}
	majority := len(rf.peers) / 2
	matches := 0
	for _, lastIdx := range rf.matchIndex {
		if lastIdx >= idx {
			matches++
		}
		if matches > majority {
			rf.commitIndex = idx
			return true
		}
	}
	return false
}

// startCommitLoop starts an infinite loop that try to commit logs in a timeout
func (rf *Raft) startCommitLoop() {
	defer tester.Annotate(fmt.Sprintf("Server %v", rf.me), "stops committing", "stops committing")
	for !rf.killed() {
		_, isLeader := rf.GetState()
		if isLeader {
			rf.mu.Lock()
			lastIdx, _ := rf.getLastLogIdxTerm()
			left, right := lastIdx, -1
			for i := rf.commitIndex + 1; i <= lastIdx; i++ {
				if rf.maybeCommit(i) {
					right = i
				}
			}
			rf.mu.Unlock()
			if right != -1 {
				tester.Annotate(fmt.Sprintf("Server %v", rf.me), "commits", fmt.Sprintf("commits left: %v, right: %v", left, right))
			}
		}
		time.Sleep(commitTimeout)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.state == leader
	if !isLeader {
		return -1, -1, false
	}
	lastIdx, _ := rf.getLastLogIdxTerm()
	tester.Annotate(fmt.Sprintf("Server %v", rf.me), "starts", fmt.Sprintf("starts %v at index %v", command, lastIdx+1))
	rf.logs = append(rf.logs, LogEntry{Data: command, Term: rf.currentTerm, Index: lastIdx + 1})
	rf.nextIndex[rf.me] = lastIdx + 2
	rf.matchIndex[rf.me] = lastIdx + 1
	index, term := rf.getLastLogIdxTerm()
	rf.persist()
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
	rf.persist()
	return args, replies
}

// maybeBecomeFollower must be used with lock
func (rf *Raft) maybeBecomeFollower(term int) bool {
	if term > rf.currentTerm {
		tester.Annotate(fmt.Sprintf("Server %v", rf.me), "-> follower", "becomes follower")
		rf.state = follower
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
		return true
	}
	return false
}

// becomeLeader must be used with lock
func (rf *Raft) becomeLeader() {
	tester.Annotate(fmt.Sprintf("Server %v", rf.me), "-> leader", "becomes leader")
	rf.state = leader
	idx, _ := rf.getLastLogIdxTerm()
	for i := range len(rf.peers) {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = idx + 1
		rf.matchIndex[i] = 0
	}
	rf.persist()
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
	for *votes <= majority && *done-*votes <= majority {
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
		tester.Annotate(fmt.Sprintf("Server %v", rf.me), "tries election", "tries election")
		if rf.beat.Load() == false {
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

func (rf *Raft) shouldApply() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex > rf.lastApplied
}

// startApplierLoop is an infinite loop that tries to apply committed logs.
// Here should be the only place to apply.
func (rf *Raft) startApplierLoop() {
	for !rf.killed() {
		for rf.shouldApply() {
			if rf.snapshot.LastIndex > rf.lastApplied {
				rf.mu.Lock()
				msg := raftapi.ApplyMsg{
					CommandValid:  false,
					SnapshotValid: true,
					SnapshotIndex: rf.snapshot.LastIndex,
					SnapshotTerm:  rf.snapshot.LastTerm,
					Snapshot:      rf.snapshot.Data,
				}
				rf.lastApplied = rf.snapshot.LastIndex
				rf.mu.Unlock()
				rf.applyCh <- msg
			} else {
				rf.mu.Lock()
				l, _ := rf.getIdxInLogs(rf.lastApplied + 1)
				r, _ := rf.getIdxInLogs(rf.commitIndex)
				rf.lastApplied = r
				r++
				toApply := slices.Clone(rf.logs[l:r])
				rf.mu.Unlock()
				for _, entry := range toApply {
					rf.applyCh <- raftapi.ApplyMsg{
						CommandValid: true,
						Command:      entry.Data,
						CommandIndex: entry.Index,
					}
				}
			}
		}
		time.Sleep(commitTimeout)
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
	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = []LogEntry{{Term: 0, Index: 0}} // Sentinel
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = follower
	rf.beat.Store(false)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start startElectionLoop goroutine to start elections
	go rf.startElectionLoop()
	go rf.startCommitLoop()
	go rf.startApplierLoop()
	for i := range rf.peers {
		if i != rf.me {
			go rf.startAppendEntriesLoop(i)
		}
	}

	return rf
}
