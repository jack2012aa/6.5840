package kvraft

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type Entry struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	mu   sync.Mutex
	db   map[string]Entry
}

type PutRequest struct {
	Key   string
	Entry Entry
}

type GetRequest struct {
	Key string
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Perform
	// If the server is waiting for the request, reply to the channel
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch req.(type) {
	case PutRequest:
		putReq := req.(PutRequest)
		if v, ok := kv.db[putReq.Key]; ok {
			if v.Version != putReq.Entry.Version {
				return rpc.PutReply{Err: rpc.ErrVersion}
			}
			putReq.Entry.Version++
			kv.db[putReq.Key] = putReq.Entry
			tester.Annotate(fmt.Sprintf("Server %v", kv.me), fmt.Sprintf("apply put %v", putReq.Entry), "")
			return rpc.PutReply{Err: rpc.OK}
		}
		if putReq.Entry.Version != 0 {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}
		putReq.Entry.Version++
		kv.db[putReq.Key] = putReq.Entry
		tester.Annotate(fmt.Sprintf("Server %v", kv.me), fmt.Sprintf("apply put %v", putReq.Entry), "")
		return rpc.PutReply{Err: rpc.OK}
	case GetRequest:
		getReq := req.(GetRequest)
		entry, ok := kv.db[getReq.Key]
		if !ok {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
		return rpc.GetReply{Value: entry.Value, Version: entry.Version, Err: rpc.OK}
	default:
		fmt.Printf("Unknown req %v", req)
		panic("unknown req type")
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	buf := new(bytes.Buffer)
	enc := labgob.NewEncoder(buf)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	err := enc.Encode(kv.db)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	if len(data) == 0 {
		return
	}
	buf := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buf)
	var db map[string]Entry
	err := dec.Decode(&db)
	if err != nil {
		panic(err)
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.db = db
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	req := GetRequest{args.Key}
	err, result := kv.rsm.Submit(req)
	if err != rpc.OK {
		reply.Err = rpc.ErrWrongLeader
	} else {
		reply.Err = result.(rpc.GetReply).Err
		reply.Value = result.(rpc.GetReply).Value
		reply.Version = result.(rpc.GetReply).Version
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	req := PutRequest{Key: args.Key, Entry: Entry{Value: args.Value, Version: args.Version}}
	tester.Annotate(fmt.Sprintf("Server %v", kv.me), fmt.Sprintf("try put key: %v, value: %v, version: %v", args.Key, args.Value, args.Version), "")
	err, result := kv.rsm.Submit(req)
	if err != rpc.OK {
		reply.Err = rpc.ErrWrongLeader
	} else {
		reply.Err = result.(rpc.PutReply).Err
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(PutRequest{})
	labgob.Register(GetRequest{})
	labgob.Register(Entry{})
	var buf []byte
	labgob.Register(buf)
	db := make(map[string]Entry)
	labgob.Register(db)
	labgob.Register(1)

	kv := &KVServer{me: me, mu: sync.Mutex{}, db: db}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
