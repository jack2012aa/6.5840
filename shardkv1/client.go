package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	cks  map[tester.Tgid]kvtest.IKVClerk
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
		cks:  make(map[tester.Tgid]kvtest.IKVClerk),
	}
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) ckOf(key string) kvtest.IKVClerk {
	sh := shardcfg.Key2Shard(key)
	cfg := ck.sck.Query()
	gid := cfg.Shards[sh]
	if ck.cks[gid] == nil {
		_, servers, _ := cfg.GidServers(sh)
		c := shardgrp.MakeClerk(ck.clnt, servers)
		ck.cks[gid] = c
	}
	return ck.cks[gid]
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		//fmt.Printf("Get key: %v, shard: %v in servers %v\n", key, shardcfg.Key2Shard(key), servers)
		shardgrpCk := ck.ckOf(key)
		val, v, err := shardgrpCk.Get(key)
		if err != rpc.ErrWrongGroup {
			return val, v, err
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	for {
		shardgrpCk := ck.ckOf(key)
		err := shardgrpCk.Put(key, value, version)
		//fmt.Println("Put:", key, servers, err)
		if err != rpc.ErrWrongGroup {
			return err
		}
		time.Sleep(200 * time.Millisecond)
	}

}
