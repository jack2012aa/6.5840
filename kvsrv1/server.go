package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type value struct {
	val string
	ver rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex
	m  map[string]value
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.m = make(map[string]value)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if v, ok := kv.m[args.Key]; ok {
		reply.Value = v.val
		reply.Version = v.ver
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

func (kv *KVServer) update(k string, val string, ver rpc.Tversion) {
	kv.m[k] = value{val: val, ver: ver}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if v, ok := kv.m[args.Key]; ok {
		if v.ver != args.Version {
			reply.Err = rpc.ErrVersion
		} else {
			kv.update(args.Key, args.Value, args.Version+1)
		}
	} else {
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
		} else {
			kv.update(args.Key, args.Value, 1)
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
