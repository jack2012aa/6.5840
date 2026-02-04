package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"sync"

	"6.5840/kvsrv1"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

const configKey = "config"

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk
	killed int32 // set by Kill()
	mu     sync.Mutex
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	sck.mu = sync.Mutex{}
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	sck.IKVClerk.Put(configKey, cfg.String(), 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	oldCfgString, v, _ := sck.IKVClerk.Get(configKey)
	oldCfg := shardcfg.FromString(oldCfgString)
	freeze := &sync.WaitGroup{}
	var states [shardcfg.NShards][]byte

	// Freeze
	for i := 0; i < shardcfg.NShards; i++ {
		if oldCfg.Shards[i] != new.Shards[i] {
			//fmt.Printf("Moving shard %v from %v to %v\n", i, oldCfg.Shards[i], new.Shards[i])
			freeze.Add(1)
			go func(i int) {
				servers := oldCfg.Groups[oldCfg.Shards[i]]
				ck := shardgrp.MakeClerk(sck.clnt, servers).(*shardgrp.Clerk)
				state, _ := ck.FreezeShard(shardcfg.Tshid(i), new.Num)
				sck.mu.Lock()
				states[i] = state
				sck.mu.Unlock()
				freeze.Done()
			}(i)
		}
	}
	freeze.Wait()

	// Install
	install := &sync.WaitGroup{}
	for i := 0; i < shardcfg.NShards; i++ {
		if oldCfg.Shards[i] != new.Shards[i] {
			install.Add(1)
			go func(i int) {
				servers := new.Groups[new.Shards[i]]
				ck := shardgrp.MakeClerk(sck.clnt, servers).(*shardgrp.Clerk)
				sck.mu.Lock()
				state := states[i]
				sck.mu.Unlock()
				ck.InstallShard(shardcfg.Tshid(i), state, new.Num)
				install.Done()
			}(i)
		}
	}
	install.Wait()

	// Delete
	del := &sync.WaitGroup{}
	for i := 0; i < shardcfg.NShards; i++ {
		if oldCfg.Shards[i] != new.Shards[i] {
			del.Add(1)
			go func(i int) {
				servers := oldCfg.Groups[oldCfg.Shards[i]]
				ck := shardgrp.MakeClerk(sck.clnt, servers).(*shardgrp.Clerk)
				ck.DeleteShard(shardcfg.Tshid(i), new.Num)
				del.Done()
			}(i)
		}
	}
	del.Wait()

	sck.IKVClerk.Put(configKey, new.String(), v)
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	s, _, _ := sck.IKVClerk.Get("config")
	return shardcfg.FromString(s)
}
