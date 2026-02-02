package shardgrp

import (
	"fmt"
	"sync"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int
	mu      sync.Mutex
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers, mu: sync.Mutex{}}
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := &rpc.GetArgs{Key: key}
	reply := &rpc.GetReply{}
	ck.mu.Lock()
	target := ck.leader
	ck.mu.Unlock()
	for {
		tester.Annotate("Client", "try get", fmt.Sprintf("to %v", target))
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Get", args, reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			break
		}
		ck.mu.Lock()
		ck.leader = (ck.leader + 1) % len(ck.servers)
		target = ck.leader
		ck.mu.Unlock()
	}
	return reply.Value, reply.Version, reply.Err
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := &rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := &rpc.PutReply{}
	retry := false
	ck.mu.Lock()
	target := ck.leader
	ck.mu.Unlock()
	for {
		tester.Annotate("Client", fmt.Sprintf("try put to %v", target), "")
		ok := ck.clnt.Call(ck.servers[target], "KVServer.Put", args, reply)
		if ok {
			switch reply.Err {
			case rpc.ErrNoKey, rpc.OK:
				tester.Annotate("Client", fmt.Sprintf("result: %v", reply.Err), "")
				return reply.Err
			case rpc.ErrVersion:
				if retry {
					tester.Annotate("Client", fmt.Sprintf("try put to %v", rpc.ErrMaybe), "")
					return rpc.ErrMaybe
				}
				tester.Annotate("Client", fmt.Sprintf("try put to %v", rpc.ErrVersion), "")
				return rpc.ErrVersion
			}
		}
		ck.mu.Lock()
		ck.leader = (ck.leader + 1) % len(ck.servers)
		target = ck.leader
		ck.mu.Unlock()
		retry = true
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	return nil, ""
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}
