package raftkv

import (
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	OpGet               = "Get"
	OpPut               = "Put"
	OpAppend            = "Append"
	PutAppendWaitCommit = 100 * time.Millisecond
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op    string
	Key   string
	Value string
}

type RaftKV struct {
	mtx          sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	maxRaftState int // snapshot if log grows this big

	// Your definitions here.
	kvs         map[string]string
	commitIndex int
}

func (kv *RaftKV) Get(req *GetArgs, reply *GetReply) {
	// Your code here.
	//DPrintf("Get %+v", req)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.IsLeader = false
		reply.Err = OK
		reply.Value = ""
		return
	}
	// TODO actually might already loses leadership
	reply.IsLeader = true
	reply.Err = OK

	kv.mtx.Lock()
	reply.Value = kv.kvs[req.Key]
	kv.mtx.Unlock()
}

func (kv *RaftKV) PutAppend(req *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	cmd := Op{req.Op, req.Key, req.Value}
	index, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.IsLeader = false
		reply.Err = OK
		//DPrintf("kv[%d] not leader", kv.me)
		return
	}
	reply.IsLeader = true
	reply.Err = OK

	//DPrintf("kv[%d] PutAppend %+v", kv.me, req)
	//DPrintf("kv[%d] wait commit", kv.me)
	for {
		kv.mtx.Lock()
		ci := kv.commitIndex
		kv.mtx.Unlock()
		if ci >= index {
			break
		}
		time.Sleep(PutAppendWaitCommit)
	}
	//DPrintf("kv[%d] done commit", kv.me)
	//DPrintf("kv.kvs[%s] = %s", req.Key, kv.kvs[req.Key])
	return
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *RaftKV) run() {
	for applyMsg := range kv.applyCh {
		op := applyMsg.Command.(Op)
		//DPrintf("kv[%d] apply %+v", kv.me, applyMsg)
		if op.Op == OpPut {
			kv.mtx.Lock()
			kv.kvs[op.Key] = op.Value
			kv.mtx.Unlock()

		} else if op.Op == OpAppend {
			kv.mtx.Lock()
			kv.kvs[op.Key] += op.Value
			kv.mtx.Unlock()

		} else {
			panic(fmt.Sprintf("unknown op = %s", op))
		}

		if applyMsg.Index <= kv.commitIndex {
			panic("applyMsg.Index <= kv.commitIndex")
		}
		kv.mtx.Lock()
		kv.commitIndex = applyMsg.Index
		kv.mtx.Unlock()
	}
	//DPrintf("kv[%d] run done", kv.me)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxRaftState bytes,
// in order to allow Raft to garbage-collect its log. if maxRaftState is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxRaftState int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxRaftState = maxRaftState

	// Your initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvs = make(map[string]string)
	kv.commitIndex = 0

	go kv.run()
	return kv
}
