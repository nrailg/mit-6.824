package raftkv

import (
	"bytes"
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
	persister   *raft.Persister
	isKilled    bool
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
		// 最初是检查 term 是否匹配，但是 snapshot 之后检查不到了。
		// ```golang
		// ok, newTerm := kv.rf.GetLogEntryTerm(index)
		// if !ok || newTerm != curTerm {
		//     return -1, curTerm, false
		// } else {
		//     return index, curTerm, true
		// }
		// ```
		newTerm, isLeader := kv.rf.GetState()
		if !isLeader || curTerm != newTerm {
			// 可能在经过两次选举之后重新夺回 leadership，但是依旧回传 `isLeader=false`。重复利用
			// 客户端的失败处理逻辑。
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
	kv.Lock()
	kv.isKilled = true
	kv.Unlock()
	kv.cond.Broadcast()
}

func (kv *RaftKV) run() {
	for applyMsg := range kv.applyCh {
		if applyMsg.UseSnapshot {
			// 当 rf 已经调整了 `LastIncludedIndex` 并且通过 chan 传递的时候，此时 kv 还没跟上同步状态，
			// 此时其他 goroutine 读取 `lastApplied` 会发现 kv 和 rf 的不一致。
			r := bytes.NewBuffer(applyMsg.Snapshot)
			d := gob.NewDecoder(r)
			kv.Lock()
			d.Decode(&kv.kvs)
			d.Decode(&kv.cmdHistory)
			d.Decode(&kv.lastApplied)
			kv.Unlock()
			//DPrintf("kv[%d].run1, lastApplied=%d", kv.me, kv.lastApplied)

		} else {
			op := applyMsg.Command.(Op)
			//DPrintf("kv[%d] apply %+v", kv.me, applyMsg)
			kv.Lock()
			histCmdId, ok := kv.cmdHistory[op.ClerkId]

			if !ok || histCmdId < op.CmdId {
				kv.cmdHistory[op.ClerkId] = op.CmdId

				if op.Op == OpGet {
				} else if op.Op == OpPut {
					kv.kvs[op.Key] = op.Value
				} else if op.Op == OpAppend {
					kv.kvs[op.Key] += op.Value
				} else {
					panic(fmt.Sprintf("unknown op = %s", op.Op))
				}
			}

			if applyMsg.Index <= kv.lastApplied {
				panic("applyMsg.Index <= kv.lastApplied")
			}
			kv.lastApplied = applyMsg.Index
			//DPrintf("kv[%d].run2, lastApplied=%d", kv.me, kv.lastApplied)
			kv.cond.Broadcast()
			kv.Unlock()
			// 为何不在此处 snapshot？因为 `applyCh` 和 `rf.inLinkCh` 会循环等待发生死锁。= = my bad.
		}
	}
}

func (kv *RaftKV) snapshotIfNecessary() {
	if kv.maxRaftState <= 0 {
		return
	}
	// 为什么放在一个单独的 routine 而不是 `Get/Put/Append` 后面呢？因为只有 leader 会执行 `Get/Put/Append`。
	for {
		kv.Lock()
		kv.cond.Wait()
		if kv.isKilled {
			kv.Unlock()
			return
		}
		kv.Unlock()

		if kv.persister.RaftStateSize() >= kv.maxRaftState {
			w := new(bytes.Buffer)
			e := gob.NewEncoder(w)
			kv.Lock()
			e.Encode(kv.kvs)
			e.Encode(kv.cmdHistory)
			lastApplied := kv.lastApplied
			e.Encode(lastApplied)
			kv.Unlock()
			snapshot := w.Bytes()
			// 放在和 `RaftKV.run` 独立的 routine 里，可能会在 `applyCh` 里的 `useSnapshot` apply 之前 snapshot，
			// 会发现 `LastIncludedIndex` 冲突。
			// 如果 lastApplied 相同，那么 snapshot 理应相同。更小的 lastApplied 的 snapshot 可以被抛弃。就用这么一个
			// 比较 dirty 的方案解决。
			kv.rf.Snapshot(lastApplied, snapshot)
			//DPrintf("kv[%d].snapshot, lastApplied=%d, size=%d", kv.me, lastApplied, kv.persister.RaftStateSize())
			//DPrintf("num of goroutines: %d", runtime.NumGoroutine())
		}
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
	snapshot := persister.ReadSnapshot()
	if snapshot == nil {
		kv.kvs = make(map[string]string)
		kv.cmdHistory = make(map[int]int)
		kv.lastApplied = 0
	} else {
		r := bytes.NewBuffer(snapshot)
		d := gob.NewDecoder(r)
		d.Decode(&kv.kvs)
		d.Decode(&kv.cmdHistory)
		d.Decode(&kv.lastApplied)
	}
	//DPrintf("kv[%d].Make, lastApplied=%d", kv.me, kv.lastApplied)

	kv.cond = sync.NewCond(&kv.mtx)
	kv.isKilled = false
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.run()
	go kv.snapshotIfNecessary()
	return kv
}
