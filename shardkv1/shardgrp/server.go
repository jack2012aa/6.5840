package shardgrp

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

type Entry struct {
	Value   string
	Version rpc.Tversion
}

type PutRequest struct {
	Key   string
	Entry Entry
}

type GetRequest struct {
	Key string
}

type FreezeRequest struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
}

type InstallShardRequest struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
	State []byte
}

type DeleteShardRequest struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
}

type KVServer struct {
	me         int
	dead       int32 // set by Kill()
	rsm        *rsm.RSM
	gid        tester.Tgid
	mu         sync.Mutex
	db         map[string]Entry
	configNum  shardcfg.Tnum
	frozenKeys map[string]bool
	shards     map[shardcfg.Tshid]bool
}

func (kv *KVServer) DoOp(req any) any {
	// Perform
	// If the server is waiting for the request, reply to the channel
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch req.(type) {
	case PutRequest:
		putReq := req.(PutRequest)
		shard := shardcfg.Key2Shard(putReq.Key)
		if !kv.shards[shard] || kv.frozenKeys[putReq.Key] {
			return rpc.PutReply{Err: rpc.ErrWrongGroup}
		}
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
		shard := shardcfg.Key2Shard(getReq.Key)
		if !kv.shards[shard] || kv.frozenKeys[getReq.Key] {
			return rpc.GetReply{Err: rpc.ErrWrongGroup}
		}
		entry, ok := kv.db[getReq.Key]
		if !ok {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
		return rpc.GetReply{Value: entry.Value, Version: entry.Version, Err: rpc.OK}
	case FreezeRequest:
		freezeReq := req.(FreezeRequest)
		if freezeReq.Num < kv.configNum || freezeReq.Num > kv.configNum+1 {
			return shardrpc.FreezeShardReply{Num: kv.configNum, Err: rpc.ErrVersion}
		}
		if !kv.shards[freezeReq.Shard] {
			return shardrpc.FreezeShardReply{Num: kv.configNum + 1, Err: rpc.ErrWrongGroup}
		}
		fmt.Printf("Server %v-%v froze shard: %v\n", kv.gid, kv.me, freezeReq.Shard)
		kv.configNum = freezeReq.Num
		state := make(map[string]Entry)
		for k, v := range kv.db {
			shard := shardcfg.Key2Shard(k)
			if !kv.frozenKeys[k] && shard == freezeReq.Shard {
				kv.frozenKeys[k] = true
				state[k] = v
			}
		}
		buf := new(bytes.Buffer)
		enc := labgob.NewEncoder(buf)
		err := enc.Encode(state)
		if err != nil {
			panic(err)
		}
		return shardrpc.FreezeShardReply{Num: kv.configNum, Err: rpc.OK, State: buf.Bytes()}
	case InstallShardRequest:
		installShardReq := req.(InstallShardRequest)
		if installShardReq.Num < kv.configNum || installShardReq.Num > kv.configNum+1 {
			return shardrpc.InstallShardReply{Err: rpc.ErrVersion}
		}
		fmt.Printf("Server %v-%v installed shard: %v\n", kv.gid, kv.me, installShardReq.Shard)
		kv.shards[installShardReq.Shard] = true
		kv.configNum = installShardReq.Num
		buf := bytes.NewBuffer(installShardReq.State)
		dec := labgob.NewDecoder(buf)
		var s map[string]Entry
		err := dec.Decode(&s)
		if err != nil {
			panic(err)
		}
		for k, v := range s {
			kv.db[k] = v
		}
		return shardrpc.InstallShardReply{Err: rpc.OK}
	case DeleteShardRequest:
		deleteShardReq := req.(DeleteShardRequest)
		if deleteShardReq.Num != kv.configNum {
			return shardrpc.DeleteShardReply{Err: rpc.ErrVersion}
		}
		if !kv.shards[deleteShardReq.Shard] {
			return shardrpc.DeleteShardReply{Err: rpc.ErrWrongGroup}
		}
		fmt.Printf("Server %v-%v deleted shard: %v\n", kv.gid, kv.me, deleteShardReq.Shard)
		for k, _ := range kv.frozenKeys {
			if shardcfg.Key2Shard(k) == deleteShardReq.Shard {
				delete(kv.frozenKeys, k)
				delete(kv.db, k)
			}
		}
		return shardrpc.DeleteShardReply{Err: rpc.OK}
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

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	req := FreezeRequest{Shard: args.Shard, Num: args.Num}
	tester.Annotate(fmt.Sprintf("Server %v", kv.me), fmt.Sprintf("try freeze shard: %v, num: %v", args.Shard, args.Num), "")
	err, result := kv.rsm.Submit(req)
	if err != rpc.OK {
		reply.Err = rpc.ErrWrongLeader
	} else {
		r := result.(shardrpc.FreezeShardReply)
		reply.Err = r.Err
		reply.Num = r.Num
		reply.State = r.State
	}
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	req := InstallShardRequest{Shard: args.Shard, Num: args.Num, State: args.State}
	tester.Annotate(fmt.Sprintf("Server %v", kv.me), fmt.Sprintf("try install shard: %v, num: %v", args.Shard, args.Num), "")
	err, result := kv.rsm.Submit(req)
	if err != rpc.OK {
		reply.Err = rpc.ErrWrongLeader
	} else {
		reply.Err = result.(shardrpc.InstallShardReply).Err
	}
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	req := DeleteShardRequest{Shard: args.Shard, Num: args.Num}
	tester.Annotate(fmt.Sprintf("Server %v", kv.me), fmt.Sprintf("try delete shard: %v, num: %v", args.Shard, args.Num), "")
	err, result := kv.rsm.Submit(req)
	if err != rpc.OK {
		reply.Err = rpc.ErrWrongLeader
	} else {
		reply.Err = result.(shardrpc.DeleteShardReply).Err
	}
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.FreezeShardReply{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.InstallShardReply{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(shardrpc.DeleteShardReply{})
	labgob.Register(rsm.Op{})
	labgob.Register(PutRequest{})
	labgob.Register(GetRequest{})
	labgob.Register(Entry{})
	labgob.Register(FreezeRequest{})
	labgob.Register(InstallShardRequest{})
	labgob.Register(DeleteShardRequest{})

	kv := &KVServer{gid: gid, me: me, db: make(map[string]Entry), configNum: 1}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	kv.frozenKeys = make(map[string]bool)
	kv.shards = make(map[shardcfg.Tshid]bool)
	if gid == shardcfg.Gid1 {
		for i := 0; i < shardcfg.NShards; i++ {
			kv.shards[shardcfg.Tshid(i)] = true
		}
	}

	return []tester.IService{kv, kv.rsm.Raft()}
}
