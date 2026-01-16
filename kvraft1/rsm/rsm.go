package rsm

import (
	"fmt"
	"sync"

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

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	submitCh     chan submitEvent
	killedCh     chan struct{}
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	increasingId int
	indexToCh    map[int]chan any
	indexToId    map[int]int
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
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		submitCh:     make(chan submitEvent),
		killedCh:     make(chan struct{}),
		sm:           sm,
		increasingId: 0,
		indexToCh:    make(map[int]chan any),
		indexToId:    make(map[int]int),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.eventLoop()
	return rsm
}

func (rsm *RSM) cancelAll() {
	for _, ch := range rsm.indexToCh {
		go func(ch chan any) {
			close(ch)
		}(ch)
	}
	clear(rsm.indexToCh)
	clear(rsm.indexToId)
}

func (rsm *RSM) eventLoop() {
	for {
		select {
		case msg, ok := <-rsm.applyCh:
			if !ok {
				select {
				case <-rsm.killedCh:
					// Is killed
				default:
					close(rsm.killedCh)
					tester.Annotate(fmt.Sprintf("Server %v", rsm.me), "close", "close")
				}
				return
			}
			index := msg.CommandIndex
			op := msg.Command.(Op)
			result := rsm.sm.DoOp(op.Command)
			ch, exists := rsm.indexToCh[index]
			if exists {
				if op.Me == rsm.me {
					if op.Id != rsm.indexToId[index] {
						panic("undefined behavior")
					}
					tester.Annotate(fmt.Sprintf("Server %v", rsm.me), "apply", fmt.Sprintf("index: %v, id: %v", index, rsm.indexToId[index]))
					delete(rsm.indexToCh, index)
					delete(rsm.indexToId, index)
					go func() {
						ch <- result
					}()
				} else {
					tester.Annotate(fmt.Sprintf("Server %v", rsm.me), "cancel", fmt.Sprintf("index: %v, id: %v", index, rsm.indexToId[index]))
					rsm.cancelAll()
				}
			}
		case e := <-rsm.submitCh:
			id := rsm.increasingId
			rsm.increasingId++
			op := Op{Me: rsm.me, Id: id, Command: e.req}
			index, _, ok := rsm.rf.Start(op)
			if !ok {
				close(e.resultCh)
			} else {
				tester.Annotate(fmt.Sprintf("Server %v", rsm.me), "submit", fmt.Sprintf("index: %v, id: %d", index, id))
				ch, exists := rsm.indexToCh[index]
				if exists {
					close(ch)
				}
				rsm.indexToCh[index] = e.resultCh
				rsm.indexToId[index] = id
			}
		}
	}
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	ch := make(chan any, 1)
	event := submitEvent{req, ch}
	select {
	case rsm.submitCh <- event:
		// continue
	case <-rsm.killedCh:
		return rpc.ErrWrongLeader, nil
	}
	select {
	case result, ok := <-ch:
		if !ok || result == nil {
			return rpc.ErrWrongLeader, nil
		}
		return rpc.OK, result
	case <-rsm.killedCh:
		return rpc.ErrWrongLeader, nil
	}
}

type submitEvent struct {
	req      any
	resultCh chan any
}
