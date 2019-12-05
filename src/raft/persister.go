package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import "sync"

type Persister struct {
	mtx       sync.Mutex
	raftState []byte
	snapshot  []byte
}

func MakePersister() *Persister {
	return &Persister{}
}

func (ps *Persister) Copy() *Persister {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()
	np := MakePersister()
	np.raftState = ps.raftState
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) SaveRaftState(data []byte) {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()
	ps.raftState = data
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()
	return ps.raftState
}

func (ps *Persister) RaftStateSize() int {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()
	return len(ps.raftState)
}

func (ps *Persister) SaveSnapshot(snapshot []byte) {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()
	ps.snapshot = snapshot
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()
	return ps.snapshot
}
