package lock

import (
	"fmt"
	"log"
	"strings"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	tester "6.5840/tester1"
)

const (
	free   = "free"
	locked = "locked"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck      kvtest.IKVClerk
	l       string
	lockSig string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, l: l}
	lk.init()
	return lk
}

func (lk *Lock) init() {
	_, _, err := lk.ck.Get(lk.l)
	if err == rpc.ErrNoKey {
		_, val := lk.sign(free)
		for {
			err = lk.ck.Put(lk.l, val, 0)
			if err != rpc.ErrMaybe {
				lk.lockSig = val
				return
			}
		}
	}
}

// Return lockSig, val
func (lk *Lock) sign(stat string) (string, string) {
	sig := tester.Randstring(16)
	val := fmt.Sprintf("%v_%v", stat, sig)
	return sig, val
}

// Return stat, lockSig
func (lk *Lock) parseStat(s string) (string, string) {
	stat := strings.Split(s, "_")[0]
	sig := strings.Split(s, "_")[1]
	return stat, sig
}

func (lk *Lock) Acquire() {
	_, myVal := lk.sign(locked)
	lk.lockSig = myVal
	for {
		lastVal, ver, err := lk.ck.Get(lk.l)
		lastStat, _ := lk.parseStat(lastVal)
		if err != rpc.OK {
			log.Fatalf("Lock Acquire: got err=%v", err)
		}
		if lastStat == locked {
			time.Sleep(time.Microsecond)
		} else {
			switch err = lk.ck.Put(lk.l, myVal, ver); err {
			case rpc.OK:
				return
			case rpc.ErrNoKey:
				log.Fatalf("Lock Acquire: got err=%v", err)
			case rpc.ErrVersion:
				continue
			case rpc.ErrMaybe:
				lastVal, ver, err = lk.ck.Get(lk.l)
				if lastVal == myVal {
					return
				}
			default:
				log.Fatalf("Lock Acquire: got err=%v", err)
			}
		}
	}
}

func (lk *Lock) Release() {
	_, myVal := lk.sign(free)
	for {
		lastVal, ver, err := lk.ck.Get(lk.l)
		if err != rpc.OK {
			log.Fatalf("Lock Release: got err=%v", err)
		}
		lastStat, _ := lk.parseStat(lastVal)
		if lastStat == free {
			return
		}
		if lastStat == locked && lastVal != lk.lockSig {
			return
		}
		switch err = lk.ck.Put(lk.l, myVal, ver); err {
		case rpc.OK:
			return
		case rpc.ErrNoKey:
			log.Fatalf("Lock Release: got err=%v", err)
		case rpc.ErrVersion:
			continue
		case rpc.ErrMaybe:
			lastVal, ver, err = lk.ck.Get(lk.l)
			if lastVal == myVal {
				return
			}
		default:
			log.Fatalf("Lock Release: got err=%v", err)
		}
	}
}
