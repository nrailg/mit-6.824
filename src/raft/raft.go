package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

type state int

const (
	eFollower state = iota
	eCandidate
	eLeader
)

const (
	electionTimeoutMin = 150
	electionTimeoutMax = 300
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mutex        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	electionTimeout time.Duration
	state state
	heartbeatCh chan int

	currentTerm int
	votedFor int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	// Your code here.
	rf.mutex.Lock()
	defer rf.mutex.Unlock()
	term = rf.currentTerm
	isleader = rf.state == eLeader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}


//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(req RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	// TODO up to date

	rf.mutex.Lock()
	defer rf.mutex.Unlock()

	if req.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

	} else if req.Term == rf.currentTerm {
		reply.Term = rf.currentTerm
		if rf.votedFor == -1 {
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
		go func() {
			rf.heartbeatCh <- req.Term
		}()

	} else {
		rf.currentTerm = req.Term
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		go func() {
			rf.heartbeatCh <- req.Term
		}()
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, req RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", req, reply)
	return ok
}


type AppendEntriesReq struct {
	Term int
	LeaderId int
	//PrevLogIndex int
	//PrevLogTerm int
	//LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(req AppendEntriesReq, reply *AppendEntriesReply) {
	rf.mutex.Lock()
	defer rf.mutex.Unlock()

	if req.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false

	} else if req.Term == rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = true
		go func() {
			rf.heartbeatCh <- req.Term
		}()

	} else {
		rf.currentTerm = req.Term
		reply.Term = rf.currentTerm
		reply.Success = true
		go func() {
			rf.heartbeatCh <- req.Term
		}()
	}
}

func (rf *Raft) sendAppendEntries(server int, req AppendEntriesReq, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", req, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) runAsFollower() {
	select {
	case <- rf.heartbeatCh:
	case <- time.After(rf.electionTimeout):
		rf.mutex.Lock()
		rf.state = eCandidate
		rf.mutex.Unlock()
	}
}

func (rf *Raft) runAsCandidate() {
	n := len(rf.peers) // TODO variable
	voteCh := make(chan bool)
	becomeLeaderCh := make(chan bool)

	func() {
		rf.mutex.Lock()
		defer rf.mutex.Unlock()

		rf.currentTerm++
		rf.votedFor = rf.me

		args := RequestVoteArgs{
			rf.currentTerm, rf.me, 0, 0, // TODO
		}
		for i := 0; i < n; i++ {
			if i == rf.me {
				continue
			}
			go func(pi int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(pi, args, &reply)
				if !ok {
					voteCh <- false
				} else {
					rf.mutex.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.mutex.Unlock()
						rf.heartbeatCh <- reply.Term
					} else {
						rf.mutex.Unlock()
					}

					if reply.VoteGranted {
						voteCh <- true
					} else {
						voteCh <- false
					}
				}
			}(i)
		}
	}()

	go func() {
		if n == 1 {
			becomeLeaderCh <- true
			return
		}
		quorum := n / 2 + 1
		nVote := 1
		for i := 0; i < n - 1; i++ {
			vote := <-voteCh
			if vote {
				nVote++
				if nVote == quorum {
					becomeLeaderCh <- true
				}
			}
		}
		if nVote < quorum {
			becomeLeaderCh <- false
		}
	}()

	select {
	case becomeLeader := <-becomeLeaderCh:
		func() {
			rf.mutex.Lock()
			defer rf.mutex.Unlock()
			if becomeLeader {
				rf.state = eLeader
			} else {
				rf.state = eFollower
			}
		}()

	case <- rf.heartbeatCh:
		func() {
			rf.mutex.Lock()
			defer rf.mutex.Unlock()
			rf.state = eFollower
		}()

	case <- time.After(rf.electionTimeout):
		func() {
			rf.mutex.Lock()
			defer rf.mutex.Unlock()
			rf.state = eFollower
		}()
	}
}

func (rf *Raft) runAsLeader() {
	nPeers := len(rf.peers)

	req := AppendEntriesReq{
		rf.currentTerm, rf.me,
	}
	for i := 0; i < nPeers; i++ {
		if i == rf.me {
			continue
		}
		go func(pi int) {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(pi, req, &reply)
			if ok {
				rf.mutex.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
				}
				rf.mutex.Unlock()
			}
		}(i)
	}
}

func (rf *Raft) run() {
	for {
		a, b := electionTimeoutMin, electionTimeoutMax
		rf.electionTimeout = time.Duration((rand.Intn(b - a) + a)) * time.Millisecond

		switch rf.state {
		case eFollower:
			rf.runAsFollower()
		case eCandidate:
			rf.runAsCandidate()
		case eLeader:
			rf.runAsLeader()
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartbeatCh = make(chan int)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.run()

	return rf
}
