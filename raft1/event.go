package raft

type raftEvent interface {
	name() string
}

type sendHeartbeatEvent struct{}

func (e *sendHeartbeatEvent) name() string {
	return "sendHeartbeatEvent"
}

type appendEntriesEvent struct {
	args  *AppendEntriesArgs
	reply *AppendEntriesReply
	done  chan struct{}
}

func (e *appendEntriesEvent) name() string {
	return "appendEntriesEvent"
}

type appendEntriesReplyEvent struct {
	args   *AppendEntriesArgs
	reply  *AppendEntriesReply
	server int
}

func (e *appendEntriesReplyEvent) name() string {
	return "appendEntriesReplyEvent"
}

type installSnapshotEvent struct {
	args  *InstallSnapshotArgs
	reply *InstallSnapshotReply
	done  chan struct{}
}

func (e *installSnapshotEvent) name() string {
	return "installSnapshotEvent"
}

type installSnapshotReplyEvent struct {
	args   *InstallSnapshotArgs
	reply  *InstallSnapshotReply
	server int
}

func (e *installSnapshotReplyEvent) name() string {
	return "installSnapshotReplyEvent"
}

type startElectionEvent struct{}

func (e *startElectionEvent) name() string {
	return "startElectionEvent"
}

type requestVoteEvent struct {
	args  *RequestVoteArgs
	reply *RequestVoteReply
	done  chan struct{}
}

func (e *requestVoteEvent) name() string {
	return "requestVoteEvent"
}

type requestVoteReplyEvent struct {
	args   *RequestVoteArgs
	reply  *RequestVoteReply
	server int
}

func (e *requestVoteReplyEvent) name() string {
	return "requestVoteReplyEvent"
}

type snapshotEvent struct {
	index    int
	snapshot []byte
	done     chan struct{}
}

func (e *snapshotEvent) name() string {
	return "snapshotEvent"
}

type startEvent struct {
	command interface{}
	done    chan struct {
		index    int
		term     int
		isLeader bool
	}
}

func (e *startEvent) name() string {
	return "startEvent"
}

type getStateEvent struct {
	done chan struct {
		isLeader bool
		term     int
	}
}

func (e *getStateEvent) name() string {
	return "getStateEvent"
}

type PersistBytesEvent struct {
	done chan int
}

func (e *PersistBytesEvent) name() string {
	return "PersistBytesEvent"
}
