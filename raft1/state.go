package raft

import (
	"fmt"
	"math/rand"
	"time"

	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type raftState string

type stateFunc func(*Raft) stateFunc

type stateHandleEvent func(*Raft, raftEvent)

func maybeBecomeFollower(r *Raft, term int, included bool) bool {
	var b bool
	if included {
		b = term >= r.currentTerm
	} else {
		b = term > r.currentTerm
	}
	if b {
		tester.Annotate(fmt.Sprintf("Server %v", r.me), "-> follower", "becomes follower")
		r.state = follower
		r.currentTerm = term
		r.votedFor = -1
		r.persist()
		return true
	}
	return false
}

// General handlers

func emptyHandler(r *Raft, e raftEvent) {
	// Do nothing
}

func handleSnapshotEvent(r *Raft, e raftEvent) {
	se, ok := e.(*snapshotEvent)
	if !ok {
		panic("not a snapshotEvent")
	}
	if se.index > r.lastApplied {
		panic(fmt.Sprintf("snapshot index out of range: %v", se.index))
	}
	if se.index <= r.snapshot.LastIndex {
		return
	}
	i := se.index - r.logs[0].Index
	s := Snapshot{Data: se.snapshot, LastIndex: se.index, LastTerm: r.logs[i].Term}
	r.logs = r.logs[i:]
	r.snapshot = s
	r.persist()
	go func() {
		se.done <- struct{}{}
	}()
}

func prepareApply(r *Raft) (applyCh chan raftapi.ApplyMsg, applyMsg raftapi.ApplyMsg) {
	if r.lastApplied < r.commitIndex {
		applyCh = r.applyCh
		if r.snapshot.LastIndex > r.lastApplied {
			applyMsg = raftapi.ApplyMsg{
				CommandValid:  false,
				SnapshotValid: true,
				SnapshotIndex: r.snapshot.LastIndex,
				SnapshotTerm:  r.snapshot.LastTerm,
				Snapshot:      r.snapshot.Data,
			}
		} else {
			i := r.lastApplied - r.logs[0].Index + 1
			applyMsg = raftapi.ApplyMsg{
				CommandValid:  true,
				SnapshotValid: false,
				Command:       r.logs[i].Data,
				CommandIndex:  r.lastApplied + 1,
			}
		}
		return
	}
	return nil, applyMsg
}

func handleStart(r *Raft, e raftEvent) {
	se, ok := e.(*startEvent)
	if !ok {
		panic("not a startEvent")
	}
	go func() {
		se.done <- struct {
			index    int
			term     int
			isLeader bool
		}{-1, -1, r.state == leader}
	}()
}

func handleGetState(r *Raft, e raftEvent) {
	ge, ok := e.(*getStateEvent)
	if !ok {
		panic("not a getStateEvent")
	}
	go func() {
		ge.done <- struct {
			isLeader bool
			term     int
		}{r.state == leader, r.currentTerm}
	}()
}

func handlePersistBytes(r *Raft, e raftEvent) {
	pe, ok := e.(*PersistBytesEvent)
	if !ok {
		panic("not a PersistBytesEvent")
	}
	go func() {
		pe.done <- r.persister.RaftStateSize()
	}()
}

// Leader's handlers

func appendEntriesTo(r *Raft, server int) {
	prev := r.nextIndex[server] - 1
	i := prev - r.logs[0].Index
	args := &AppendEntriesArgs{
		Term:         r.currentTerm,
		Leader:       r.me,
		PrevLogIndex: prev,
		PrevLogTerm:  r.logs[i].Term,
		Entries:      r.logs[i+1:],
		LeaderCommit: r.commitIndex,
	}
	reply := &AppendEntriesReply{}
	go func() {
		r.peers[server].Call("Raft.AppendEntries", args, reply)
		r.eQ <- &appendEntriesReplyEvent{args: args, reply: reply, server: server}
	}()
}

func installSnapshotTo(r *Raft, server int) {
	args := &InstallSnapshotArgs{
		Term:     r.currentTerm,
		Snapshot: r.snapshot,
	}
	reply := &InstallSnapshotReply{}
	go func() {
		r.peers[server].Call("Raft.InstallSnapshot", args, reply)
		r.eQ <- &installSnapshotReplyEvent{args: args, reply: reply, server: server}
	}()
}

func leaderHandleSendHeartbeat(r *Raft, e raftEvent) {
	_, ok := e.(*sendHeartbeatEvent)
	if !ok {
		panic("not a sendHeartbeatEvent")
	}
	for server := range len(r.peers) {
		if server == r.me {
			continue
		}
		prev := r.nextIndex[server] - 1
		if prev < r.logs[0].Index {
			installSnapshotTo(r, server)
		} else {
			appendEntriesTo(r, server)
		}
	}
}

func leaderHandleAppendEntries(r *Raft, e raftEvent) {
	ae, ok := e.(*appendEntriesEvent)
	if !ok {
		panic("not a appendEntriesEvent")
	}
	args := ae.args
	if maybeBecomeFollower(r, args.Term, false) {
		go func() {
			r.eQ <- e
		}()
	} else {
		reply := ae.reply
		reply.Term = r.currentTerm
		reply.PrevLogIndex = -1
		reply.XTerm = -1
		reply.XIndex = -1
		reply.Success = false
		go func() {
			ae.done <- struct{}{}
		}()
	}
}

func tryCommit(r *Raft, last int) {
	tester.Annotate(fmt.Sprintf("Server %v", r.me),
		"try commit",
		fmt.Sprintf("%v", last))
	if last < r.commitIndex {
		return
	}
	majority := len(r.peers) / 2
	matches := 0
	for _, i := range r.matchIndex {
		if i >= last {
			matches++
		}
		if matches > majority {
			break
		}
	}
	if matches <= majority {
		return
	}
	tester.Annotate(fmt.Sprintf("Server %v", r.me),
		"commits",
		fmt.Sprintf("commits left: %v, right: %v", r.commitIndex, last))
	r.commitIndex = last
}

func leaderHandleAppendEntriesReply(r *Raft, e raftEvent) {
	re, ok := e.(*appendEntriesReplyEvent)
	if !ok {
		panic("not a appendEntriesReplyEvent")
	}
	args := re.args
	reply := re.reply
	server := re.server
	tester.Annotate(
		fmt.Sprintf("Server %v", r.me),
		"append reply",
		fmt.Sprintf("server: %v, success: %v, term: %v, prev: %v, xTerm: %v, xIndex: %v", server, reply.Success, reply.Term, reply.PrevLogIndex, reply.XTerm, reply.XIndex),
	)
	switch {
	case maybeBecomeFollower(r, reply.Term, false):
		return
	case args.Term != r.currentTerm:
		return
	case reply.Success:
		if reply.PrevLogIndex > r.matchIndex[server] {
			r.matchIndex[server] = reply.PrevLogIndex
			r.nextIndex[server] = reply.PrevLogIndex + 1
			tryCommit(r, reply.PrevLogIndex)
		}
	case reply.PrevLogIndex == -1 && reply.XTerm == -1:
		return
	case reply.PrevLogIndex != -1:
		r.nextIndex[server] = reply.PrevLogIndex + 1
	case reply.XTerm != -1:
		lastFromXTerm := -1
		for i := len(r.logs) - 1; i >= 0; i-- {
			if r.logs[i].Term == reply.XTerm {
				lastFromXTerm = i
				break
			}
		}
		if lastFromXTerm == -1 {
			r.nextIndex[server] = reply.XIndex
		} else {
			r.nextIndex[server] = lastFromXTerm + 1
		}
	default:
		panic("undefined append reply state")
	}
}

func leaderHandleInstallSnapshot(r *Raft, e raftEvent) {
	ie, ok := e.(*installSnapshotEvent)
	if !ok {
		panic("not a installSnapshotEvent")
	}
	args := ie.args
	if maybeBecomeFollower(r, args.Term, false) {
		go func() {
			r.eQ <- e
		}()
	} else {
		reply := ie.reply
		reply.Term = r.currentTerm
		reply.Success = false
		go func() {
			ie.done <- struct{}{}
		}()
	}
}

func leaderHandleInstallSnapshotReply(r *Raft, e raftEvent) {
	re, ok := e.(*installSnapshotReplyEvent)
	if !ok {
		panic("not a installSnapshotReplyEvent")
	}
	args := re.args
	reply := re.reply
	server := re.server
	switch {
	case maybeBecomeFollower(r, reply.Term, false):
		return
	case args.Term != r.currentTerm:
		return
	case reply.Success:
		if r.nextIndex[server] < args.Snapshot.LastIndex+1 {
			r.nextIndex[server] = args.Snapshot.LastIndex + 1
		}
		if r.matchIndex[server] < args.Snapshot.LastIndex {
			r.matchIndex[server] = args.Snapshot.LastIndex
		}
	}
}

func leaderHandleRequestVote(r *Raft, e raftEvent) {
	re, ok := e.(*requestVoteEvent)
	if !ok {
		panic("not a requestVoteEvent")
	}
	args := re.args
	reply := re.reply
	if maybeBecomeFollower(r, args.Term, false) {
		go func() {
			r.eQ <- e
		}()
	} else {
		reply.Term = r.currentTerm
		reply.VoteGranted = false
	}
}

func leaderHandleStart(r *Raft, e raftEvent) {
	se, ok := e.(*startEvent)
	if !ok {
		panic("not a startEvent")
	}
	last := len(r.logs) - 1
	i := r.logs[last].Index + 1
	t := r.currentTerm
	r.logs = append(r.logs, LogEntry{Index: i, Term: t, Data: se.command})
	r.nextIndex[r.me] = i + 1
	r.matchIndex[r.me] = i
	r.persist()
	go func() {
		se.done <- struct {
			index    int
			term     int
			isLeader bool
		}{i, t, true}
	}()
}

func stateLeader(r *Raft) stateFunc {
	handlers := map[string]stateHandleEvent{
		"sendHeartbeatEvent":        leaderHandleSendHeartbeat,
		"appendEntriesEvent":        leaderHandleAppendEntries,
		"appendEntriesReplyEvent":   leaderHandleAppendEntriesReply,
		"installSnapshotEvent":      leaderHandleInstallSnapshot,
		"installSnapshotReplyEvent": leaderHandleInstallSnapshotReply,
		"startElectionEvent":        emptyHandler,
		"requestVoteEvent":          leaderHandleRequestVote,
		"requestVoteReplyEvent":     emptyHandler,
		"snapshotEvent":             handleSnapshotEvent,
		"startEvent":                leaderHandleStart,
		"getStateEvent":             handleGetState,
		"PersistBytesEvent":         handlePersistBytes,
	}

	last := len(r.logs) - 1
	for i := range len(r.peers) {
		if i != r.me {
			r.nextIndex[i] = r.logs[last].Index
			r.matchIndex[i] = 0
		}
	}
	r.persist()
	leaderHandleSendHeartbeat(r, &sendHeartbeatEvent{})
	heartbeatTimer := time.After(heartbeatTimeout)
	for {
		applyCh, applyMsg := prepareApply(r)
		select {
		case e := <-r.eQ:
			tester.Annotate(fmt.Sprintf("Server %v", r.me), "event", e.name())
			handler, ok := handlers[e.name()]
			if !ok {
				panic(fmt.Sprintf("no handler registered for %s", e.name()))
			}
			handler(r, e)
		case <-heartbeatTimer:
			tester.Annotate(fmt.Sprintf("Server %v", r.me), "heartbeat timeout", "heartbeat timeout")
			go func() {
				r.eQ <- &sendHeartbeatEvent{}
			}()
			heartbeatTimer = time.After(heartbeatTimeout)
		case applyCh <- applyMsg:
			tester.Annotate(fmt.Sprintf("Server %v", r.me), "apply to ch", "apply to ch")
			if applyMsg.SnapshotValid {
				r.lastApplied = applyMsg.SnapshotIndex
			} else {
				r.lastApplied = applyMsg.CommandIndex
			}
		case <-r.killedChan:
			return nil
		}
		switch r.state {
		case leader:
			continue
		case candidate:
			return stateCandidate
		case follower:
			return stateFollower
		default:
			panic("unknown raft state")
		}
	}
}

func candidateHandleAppendEntries(r *Raft, e raftEvent) {
	ae, ok := e.(*appendEntriesEvent)
	if !ok {
		panic("not an appendEntriesEvent")
	}
	if maybeBecomeFollower(r, ae.args.Term, true) {
		go func() {
			r.eQ <- e
		}()
	} else {
		reply := ae.reply
		reply.Term = r.currentTerm
		reply.PrevLogIndex = -1
		reply.XTerm = -1
		reply.XIndex = -1
		reply.Success = false
		go func() {
			ae.done <- struct{}{}
		}()
	}
}

func candidateHandleInstallSnapshot(r *Raft, e raftEvent) {
	ie, ok := e.(*installSnapshotEvent)
	if !ok {
		panic("not a installSnapshotEvent")
	}
	args := ie.args
	if maybeBecomeFollower(r, args.Term, true) {
		go func() {
			r.eQ <- e
		}()
	} else {
		reply := ie.reply
		reply.Term = r.currentTerm
		reply.Success = false
		go func() {
			ie.done <- struct{}{}
		}()
	}
}

func candidateHandleStartElection(r *Raft, e raftEvent) {
	_, ok := e.(*startElectionEvent)
	if !ok {
		panic("not a startElectionEvent")
	}
	r.currentTerm++
	r.votedFor = r.me
	r.persist()
	r.grantedVote = 1
	tester.Annotate(
		fmt.Sprintf("Server %v", r.me),
		"election",
		fmt.Sprintf("starts election for term %v", r.currentTerm))
	last := len(r.logs) - 1
	args := &RequestVoteArgs{
		Term:         r.currentTerm,
		Candidate:    r.me,
		LastLogIndex: r.logs[last].Index,
		LastLogTerm:  r.logs[last].Term,
	}
	for i := range len(r.peers) {
		if i != r.me {
			go func() {
				reply := &RequestVoteReply{}
				r.peers[i].Call("Raft.RequestVote", args, reply)
				r.eQ <- &requestVoteReplyEvent{server: i, args: args, reply: reply}
			}()
		}
	}
	r.electionTimer = time.After(electionTimeout())
}

func candidateHandleRequestVote(r *Raft, e raftEvent) {
	re, ok := e.(*requestVoteEvent)
	if !ok {
		panic("not a requestVoteEvent")
	}
	args := re.args
	if maybeBecomeFollower(r, args.Term, false) {
		go func() {
			r.eQ <- e
		}()
	}
	reply := re.reply
	switch {
	case args.Term < r.currentTerm:
		reply.Term = r.currentTerm
		reply.VoteGranted = false
	case (r.votedFor == -1 || r.votedFor == args.Candidate) && r.isLogUpToDate(args.LastLogIndex, args.LastLogTerm):
		reply.Term = args.Term
		reply.VoteGranted = true
		r.votedFor = args.Candidate
		r.persist()
		r.state = follower
	default:
		reply.Term = args.Term
		reply.VoteGranted = false
	}
	go func() {
		re.done <- struct{}{}
	}()
}

func candidateHandleRequestVoteReply(r *Raft, e raftEvent) {
	re, ok := e.(*requestVoteReplyEvent)
	if !ok {
		panic("not a requestVoteReplyEvent")
	}
	args := re.args
	reply := re.reply
	switch {
	case maybeBecomeFollower(r, args.Term, false):
		return
	case args.Term != r.currentTerm:
		return
	case reply.VoteGranted:
		r.electionTimer = time.After(electionTimeout())
		r.grantedVote++
	}
	majority := len(r.peers) / 2
	if r.grantedVote > majority {
		r.state = leader
		tester.Annotate(fmt.Sprintf("Server %v", r.me), "win", fmt.Sprintf("maj: %v, granted: %v", majority, r.grantedVote))
	}
}

func electionTimeout() time.Duration {
	return time.Duration(300+(rand.Int63()%300)) * time.Millisecond
}

func stateCandidate(r *Raft) stateFunc {
	handlers := map[string]stateHandleEvent{
		"sendHeartbeatEvent":        emptyHandler,
		"appendEntriesEvent":        candidateHandleAppendEntries,
		"appendEntriesReplyEvent":   emptyHandler,
		"installSnapshotEvent":      candidateHandleInstallSnapshot,
		"installSnapshotReplyEvent": emptyHandler,
		"startElectionEvent":        candidateHandleStartElection,
		"requestVoteEvent":          candidateHandleRequestVote,
		"requestVoteReplyEvent":     candidateHandleRequestVoteReply,
		"snapshotEvent":             handleSnapshotEvent,
		"startEvent":                handleStart,
		"getStateEvent":             handleGetState,
		"PersistBytesEvent":         handlePersistBytes,
	}

	candidateHandleStartElection(r, &startElectionEvent{})
	r.electionTimer = time.After(electionTimeout())
	for {
		applyCh, applyMsg := prepareApply(r)
		select {
		case e := <-r.eQ:
			tester.Annotate(fmt.Sprintf("Server %v", r.me), "event", e.name())
			handler, ok := handlers[e.name()]
			if !ok {
				panic(fmt.Sprintf("no handler registered for %s", e.name()))
			}
			handler(r, e)
		case <-r.electionTimer:
			tester.Annotate(fmt.Sprintf("Server %v", r.me), "election timeout", "election timeout")
			go func() {
				r.eQ <- &startElectionEvent{}
			}()
			r.electionTimer = time.After(electionTimeout())
		case applyCh <- applyMsg:
			tester.Annotate(fmt.Sprintf("Server %v", r.me), "apply to ch", "apply to ch")
			if applyMsg.SnapshotValid {
				r.lastApplied = applyMsg.SnapshotIndex
			} else {
				r.lastApplied = applyMsg.CommandIndex
			}
		case <-r.killedChan:
			return nil
		}
		switch r.state {
		case leader:
			return stateLeader
		case candidate:
			continue
		case follower:
			return stateFollower
		default:
			panic("unknown raft state")
		}
	}
}

func followerHandleAppendEntries(r *Raft, e raftEvent) {
	ae, ok := e.(*appendEntriesEvent)
	if !ok {
		panic("not an appendEntriesEvent")
	}
	args := ae.args
	reply := ae.reply
	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.PrevLogIndex = -1
		reply.XTerm = -1
		reply.XIndex = -1
		reply.Success = false
		go func() {
			ae.done <- struct{}{}
		}()
		return
	}
	last := len(r.logs) - 1
	if r.logs[last].Index < args.PrevLogIndex {
		reply.Term = r.currentTerm
		reply.PrevLogIndex = r.logs[last].Index
		reply.XTerm = -1
		reply.XIndex = -1
		reply.Success = false
		go func() {
			ae.done <- struct{}{}
		}()
		return
	}

	// Remove entries before snapshot
	if len(args.Entries) > 0 {
		right := args.Entries[len(args.Entries)-1].Index
		if right <= r.snapshot.LastIndex {
			args.Entries = []LogEntry{}
			args.PrevLogIndex = r.snapshot.LastIndex
			args.PrevLogTerm = r.snapshot.LastTerm
		} else {
			i := r.snapshot.LastIndex - args.Entries[0].Index
			if i >= 0 {
				args.PrevLogIndex = args.Entries[i].Index
				args.PrevLogTerm = args.Entries[i].Term
				args.Entries = args.Entries[i+1:]
			}
		}
	}

	if args.PrevLogIndex < r.logs[0].Index {
		// this is an old request
		reply.Term = r.currentTerm
		reply.PrevLogIndex = -1
		reply.XTerm = -1
		reply.XIndex = -1
		reply.Success = false
		go func() {
			ae.done <- struct{}{}
		}()
		return
	}

	p := args.PrevLogIndex - r.logs[0].Index
	if r.logs[p].Term != args.PrevLogTerm {
		xTerm := r.logs[p].Term
		xIdx := r.logs[p].Index
		for ; xIdx > r.logs[0].Index; xIdx-- {
			t := r.logs[xIdx-r.logs[0].Index-1].Term
			if t != xTerm {
				break
			}
		}
		reply.Term = r.currentTerm
		reply.PrevLogIndex = -1
		reply.XTerm = xTerm
		reply.XIndex = xIdx
		reply.Success = false
		go func() {
			ae.done <- struct{}{}
		}()
		return
	}

	updatedIdx := args.PrevLogIndex
	if len(args.Entries) > 0 {
		updatedIdx = args.Entries[len(args.Entries)-1].Index
	}
	for i, entry := range args.Entries {
		if entry.Index > r.logs[last].Index {
			r.logs = append(r.logs, args.Entries[i:]...)
			tester.Annotate(fmt.Sprintf(
				"Server %v", r.me),
				"appends",
				fmt.Sprintf("appends left: %v right: %v", entry.Index, args.Entries[len(args.Entries)-1].Index),
			)
			break
		} else if t := r.logs[entry.Index-r.logs[0].Index].Term; entry.Term != t {
			tester.Annotate(
				fmt.Sprintf("Server %v", r.me),
				"appends",
				fmt.Sprintf("appends left: %v right: %v", entry.Index, args.Entries[len(args.Entries)-1].Index),
			)
			pivot := entry.Index - r.logs[0].Index
			r.logs = append(r.logs[:pivot], args.Entries[i:]...)
			break
		}
	}
	if args.LeaderCommit > r.commitIndex {
		r.commitIndex = args.LeaderCommit
		if updatedIdx < args.LeaderCommit {
			r.commitIndex = updatedIdx
		}
		tester.Annotate(
			fmt.Sprintf("Server %v", r.me),
			"update commit",
			fmt.Sprintf("update commit index: %v", r.commitIndex),
		)
	}
	r.electionTimer = time.After(electionTimeout())
	reply.Term = r.currentTerm
	reply.Success = true
	reply.PrevLogIndex = updatedIdx
	r.persist()
	go func() {
		ae.done <- struct{}{}
	}()
}

func followerHandleInstallSnapshot(r *Raft, e raftEvent) {
	ie, ok := e.(*installSnapshotEvent)
	if !ok {
		panic("not an installSnapshotEvent")
	}
	args := ie.args
	reply := ie.reply
	s := args.Snapshot
	last := len(r.logs) - 1
	switch {
	case s.LastIndex > r.logs[last].Index:
		r.snapshot = s
		r.logs = []LogEntry{{Index: s.LastIndex, Term: s.LastTerm}}
		if r.commitIndex < s.LastIndex {
			r.commitIndex = s.LastIndex
		}
		r.persist()
	case s.LastIndex > r.logs[0].Index:
		r.snapshot = s
		i := s.LastIndex - r.logs[0].Index
		if r.logs[i].Term == s.LastTerm {
			r.logs = r.logs[i:]
		} else {
			r.logs = []LogEntry{{Index: s.LastIndex, Term: s.LastTerm}}
		}
		if r.commitIndex < s.LastIndex {
			r.commitIndex = s.LastIndex
		}
		r.persist()
	}

	r.electionTimer = time.After(electionTimeout())
	tester.Annotate(
		fmt.Sprintf("Server %v", r.me),
		"install success",
		fmt.Sprintf("last idx: %v, last term: %v, term: %v", args.Snapshot.LastIndex, args.Snapshot.LastTerm, args.Term),
	)
	reply.Success = true
	go func() {
		ie.done <- struct{}{}
	}()
}

func followerHandleRequestVote(r *Raft, e raftEvent) {
	re, ok := e.(*requestVoteEvent)
	if !ok {
		panic("not a requestVoteEvent")
	}
	args := re.args
	if args.Term > r.currentTerm {
		r.currentTerm = args.Term
		r.votedFor = -1
		r.persist()
	}
	reply := re.reply
	switch {
	case args.Term < r.currentTerm:
		reply.Term = r.currentTerm
		reply.VoteGranted = false
		tester.Annotate(fmt.Sprintf("Server %v", r.me), "reject", fmt.Sprintf("%v", args.Candidate))
	case (r.votedFor == -1 || r.votedFor == args.Candidate) && r.isLogUpToDate(args.LastLogIndex, args.LastLogTerm):
		reply.Term = args.Term
		reply.VoteGranted = true
		r.votedFor = args.Candidate
		r.persist()
		tester.Annotate(fmt.Sprintf("Server %v", r.me), "grant", fmt.Sprintf("%v", args.Candidate))
		r.electionTimer = time.After(electionTimeout())
	default:
		reply.Term = args.Term
		reply.VoteGranted = false
		tester.Annotate(fmt.Sprintf("Server %v", r.me), "reject", fmt.Sprintf("%v", args.Candidate))
	}
	go func() {
		re.done <- struct{}{}
	}()
}

func stateFollower(r *Raft) stateFunc {
	handlers := map[string]stateHandleEvent{
		"sendHeartbeatEvent":        emptyHandler,
		"appendEntriesEvent":        followerHandleAppendEntries,
		"appendEntriesReplyEvent":   emptyHandler,
		"installSnapshotEvent":      followerHandleInstallSnapshot,
		"installSnapshotReplyEvent": emptyHandler,
		"startElectionEvent":        emptyHandler,
		"requestVoteEvent":          followerHandleRequestVote,
		"requestVoteReplyEvent":     emptyHandler,
		"snapshotEvent":             handleSnapshotEvent,
		"startEvent":                handleStart,
		"getStateEvent":             handleGetState,
		"PersistBytesEvent":         handlePersistBytes,
	}

	r.electionTimer = time.After(electionTimeout())
	for {
		applyCh, applyMsg := prepareApply(r)
		select {
		case e := <-r.eQ:
			tester.Annotate(fmt.Sprintf("Server %v", r.me), "event", e.name())
			handler, ok := handlers[e.name()]
			if !ok {
				panic(fmt.Sprintf("no handler registered for %s", e.name()))
			}
			handler(r, e)
		case <-r.electionTimer:
			tester.Annotate(fmt.Sprintf("Server %v", r.me), "election timeout", "election timeout")
			r.state = candidate
		case applyCh <- applyMsg:
			tester.Annotate(fmt.Sprintf("Server %v", r.me), "apply to ch", "apply to ch")
			if applyMsg.SnapshotValid {
				r.lastApplied = applyMsg.SnapshotIndex
			} else {
				r.lastApplied = applyMsg.CommandIndex
			}
		case <-r.killedChan:
			return nil
		}
		switch r.state {
		case leader:
			return stateLeader
		case candidate:
			return stateCandidate
		case follower:
			continue
		default:
			panic("unknown raft state")
		}
	}
}
