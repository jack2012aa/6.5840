package rsm

import (
	"fmt"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	Command any
	Me      int
	Id      int
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type waitingEntry struct {
	ch    chan any
	index int
	id    int
}

type RSM struct {
	mu             sync.Mutex
	me             int
	rf             raftapi.Raft
	applyCh        chan raftapi.ApplyMsg
	killedCh       chan struct{}
	maxraftstate   int // snapshot if log grows this big
	sm             StateMachine
	increasingId   int
	waitingIndices map[int]waitingEntry
	persister      *tester.Persister
	lastApplied    int
	snapshotCh     chan struct{}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:             me,
		mu:             sync.Mutex{},
		maxraftstate:   maxraftstate,
		applyCh:        make(chan raftapi.ApplyMsg),
		killedCh:       make(chan struct{}),
		sm:             sm,
		increasingId:   0,
		waitingIndices: make(map[int]waitingEntry),
		persister:      persister,
		snapshotCh:     make(chan struct{}),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.applier()
	go rsm.SnapshotChecker()
	return rsm
}

func (rsm *RSM) applier() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			index := msg.CommandIndex
			op := msg.Command.(Op)
			result := rsm.sm.DoOp(op.Command)
			tester.Annotate(fmt.Sprintf("Server %v", rsm.me), fmt.Sprintf("rsm apply index: %v", index), "")
			rsm.mu.Lock()
			rsm.lastApplied = msg.CommandIndex
			entry, ok := rsm.waitingIndices[index]
			if ok {
				if op.Me == rsm.me {
					if op.Id != entry.id {
						panic("undefined behavior")
					}
					entry.ch <- result
				} else {
					close(entry.ch)
				}
				delete(rsm.waitingIndices, index)
			}
			rsm.mu.Unlock()
		} else {
			s := msg.Snapshot
			rsm.sm.Restore(s)
			tester.Annotate(fmt.Sprintf("Server %v", rsm.me), fmt.Sprintf("rsm apply snapshot index: %v", msg.SnapshotIndex), "")
			rsm.mu.Lock()
			rsm.lastApplied = msg.SnapshotIndex
			for i, entry := range rsm.waitingIndices {
				if i <= msg.SnapshotIndex {
					close(entry.ch)
					delete(rsm.waitingIndices, i)
				}
			}
			rsm.mu.Unlock()
		}
		go func() {
			select {
			case rsm.snapshotCh <- struct{}{}:
			case <-rsm.killedCh:
			}
		}()
	}
	close(rsm.killedCh)
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) SnapshotChecker() {
	for {
		select {
		case <-rsm.snapshotCh:
			size := rsm.rf.PersistBytes()
			rsm.mu.Lock()
			if rsm.maxraftstate != -1 && size >= rsm.maxraftstate {
				tester.Annotate(fmt.Sprintf("Server %v", rsm.me), fmt.Sprintf("rsm snapshot size: %v", size), "")
				s := rsm.sm.Snapshot()
				rsm.rf.Snapshot(rsm.lastApplied, s)
			}
			rsm.mu.Unlock()
		case <-rsm.killedCh:
			return
		}
	}
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	rsm.mu.Lock()
	go func() {
		select {
		case rsm.snapshotCh <- struct{}{}:
		case <-rsm.killedCh:
		}
	}()
	op := Op{Me: rsm.me, Id: rsm.increasingId, Command: req}
	rsm.increasingId++
	tester.Annotate(fmt.Sprintf("Server %v", rsm.me), fmt.Sprintf("rsm submit id: %v", op.Id), "")
	index, _, ok := rsm.rf.Start(op)
	if !ok {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	entry, exists := rsm.waitingIndices[index]
	if exists {
		close(entry.ch)
	}
	ch := make(chan any, 1)
	rsm.waitingIndices[index] = waitingEntry{ch: ch, index: index, id: op.Id}
	rsm.mu.Unlock()
	select {
	case result, ok := <-ch:
		tester.Annotate(fmt.Sprintf("Server %v", rsm.me), fmt.Sprintf("rsm receive result: %v", result), "")
		if !ok {
			return rpc.ErrWrongLeader, nil
		}
		return rpc.OK, result
	case <-rsm.killedCh:
		return rpc.ErrWrongLeader, nil
	case <-time.After(500 * time.Millisecond):
		tester.Annotate(fmt.Sprintf("Server %v", rsm.me), "rsm receive timeout", "")
		rsm.mu.Lock()
		delete(rsm.waitingIndices, index)
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
}
