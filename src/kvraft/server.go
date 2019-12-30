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

var nxtReqId int = 0
var reqIdMtx sync.Mutex

func nextReqId() int {
	reqIdMtx.Lock()
	defer reqIdMtx.Unlock()
	nxtReqId++
	return nxtReqId
}

const (
	OpGet               = "Get"
	OpPut               = "Put"
	OpAppend            = "Append"
	PutAppendWaitCommit = 10 * time.Millisecond
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op      string
	Key     string
	Value   string
	ClerkId int
	CmdId   int
}

type RaftKV struct {
	mtx          sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	maxRaftState int // snapshot if log grows this big

	// Your definitions here.
	cond        *sync.Cond
	lastApplied int
	kvs         map[string]string
	cmdHistory  map[int]int
}

func (kv *RaftKV) Lock() {
	kv.cond.L.Lock()
}

func (kv *RaftKV) Unlock() {
	kv.cond.L.Unlock()
}

func (kv *RaftKV) commitLogEntry(op Op) (index int, term int, isLeader bool) {
	index, curTerm, isLeader := kv.rf.Start(op)
	if !isLeader {
		return -1, curTerm, false
	}

	// If the ex-leader is partitioned by itself, it won't know about new leaders; but any client in the same partition
	// won't be able to talk to a new leader either, so it's OK in this case for the server and client to wait
	// indefinitely until the partition heals.

	onCommitDone := make(chan struct{})
	onLoseLeadership := make(chan struct{})

	go func() {
		kv.Lock()
		defer kv.Unlock()
		for kv.lastApplied < index {
			kv.cond.Wait()
			select {
			case <-onLoseLeadership:
				return
			default:
			}
		}
		close(onCommitDone)
	}()

	go func() {
	L2:
		for {
			t := time.NewTimer(PutAppendWaitCommit)
			select {
			case <-t.C:
			case <-onCommitDone:
				t.Stop()
				break L2
			}
			newTerm, isLeader := kv.rf.GetState()
			if !isLeader || curTerm != newTerm {
				close(onLoseLeadership)
				kv.cond.Broadcast()
				break L2
			}
		}
	}()

	select {
	case <-onLoseLeadership:
		return -1, curTerm, false
	case <-onCommitDone:
		ok, newTerm := kv.rf.GetLogEntryTerm(index)
		if !ok || newTerm != curTerm {
			return -1, curTerm, false
		} else {
			return index, curTerm, true
		}
	}
}

func (kv *RaftKV) Get(req *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{OpGet, req.Key, "", -1, -1}
	_, _, isLeader := kv.commitLogEntry(op)
	if !isLeader {
		reply.IsLeader = false
		reply.Err = ""
		reply.Value = ""
	} else {
		reply.IsLeader = true
		// we can only offer sequential consistency.
		kv.Lock()
		val, ok := kv.kvs[req.Key]
		if ok {
			reply.Err = OK
			reply.Value = val
		} else {
			reply.Err = ErrNoKey
			reply.Value = ""
		}
		kv.Unlock()
	}
}

func (kv *RaftKV) PutAppend(req *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{req.Op, req.Key, req.Value, req.ClerkId, req.CmdId}
	_, _, isLeader := kv.commitLogEntry(op)
	//DPrintf("kv[%d] Start, index=%d, curTerm=%d, isLeader=%v", kv.me, index, curTerm, isLeader)
	if !isLeader {
		reply.IsLeader = false
		reply.Err = ""
	} else {
		reply.IsLeader = true
		reply.Err = OK
	}
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
		if histCmdId, ok := kv.cmdHistory[op.ClerkId]; !ok || histCmdId < op.CmdId {
			kv.cmdHistory[op.ClerkId] = op.CmdId

			if op.Op == OpGet {
			} else if op.Op == OpPut {
				kv.Lock()
				kv.kvs[op.Key] = op.Value
				kv.Unlock()
			} else if op.Op == OpAppend {
				kv.Lock()
				kv.kvs[op.Key] += op.Value
				kv.Unlock()
			} else {
				panic(fmt.Sprintf("unknown op = %s", op.Op))
			}
		}

		kv.Lock()
		if applyMsg.Index <= kv.lastApplied {
			panic("applyMsg.Index <= kv.lastApplied")
		}
		kv.lastApplied = applyMsg.Index
		kv.cond.Broadcast()
		kv.Unlock()
	}
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
	kv.cond = sync.NewCond(&kv.mtx)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.lastApplied = 0
	kv.kvs = make(map[string]string)
	kv.cmdHistory = make(map[int]int)

	go kv.run()
	return kv
}
