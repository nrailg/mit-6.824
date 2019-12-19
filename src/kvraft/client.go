package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"time"
)

const (
	noLeaderSleepTime = 100 * time.Millisecond
	noLeaderRetries   = 10
)

type Clerk struct {
	servers      []*labrpc.ClientEnd
	recentLeader int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.recentLeader = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	n := len(ck.servers)
	for {
		lastLeader := ck.recentLeader
		for j := 0; j < n; j++ {
			i := (lastLeader + j) % n
			req := GetArgs{key}
			reply := GetReply{}
			ok := ck.servers[i].Call("RaftKV.Get", &req, &reply)
			if !ok {
				continue
			}
			if !reply.IsLeader {
				continue
			} else {
				ck.recentLeader = i
			}
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err == ErrNoKey {
				return ""
			} else {
				continue
			}
		}
		time.Sleep(noLeaderSleepTime)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	n := len(ck.servers)
	for {
		lastLeader := ck.recentLeader
		for j := 0; j < n; j++ {
			i := (lastLeader + j) % n
			req := PutAppendArgs{key, value, op}
			reply := PutAppendReply{}
			ok := ck.servers[i].Call("RaftKV.PutAppend", &req, &reply)
			if !ok {
				continue
			}
			if !reply.IsLeader {
				continue
			}
			if reply.Err == OK {
				return
			}
		}
		time.Sleep(noLeaderSleepTime)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}
