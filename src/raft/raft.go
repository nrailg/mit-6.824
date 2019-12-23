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
	"bytes"
	"encoding/gob"
	"labrpc"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

type state int
type AppendEntriesError int

const (
	eFollower state = iota
	eCandidate
	eLeader
)

const (
	eAppendEntriesOk              AppendEntriesError = 1
	eAppendEntriesErr                                = 2
	eAppendEntriesLessTerm                           = 3
	eAppendEntriesLogInconsistent                    = 4
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

type inLink struct {
	req     interface{}
	replyCh chan interface{}
}

func newInLink(req interface{}) inLink {
	return inLink{
		req, make(chan interface{}),
	}
}

type outLink struct {
	reqs            map[int]interface{}
	replyCh         chan interface{}
	ignoreRepliesCh chan struct{}
}

func newOutLink(reqs map[int]interface{}, replyCh chan interface{}) outLink {
	return outLink{
		reqs, replyCh, make(chan struct{}),
	}
}

func (link *outLink) ignoreReplies() {
	close(link.ignoreRepliesCh)
}

type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	me        int // index into peers[]
	mtx       sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state                 state
	killed                chan struct{}
	applyCh               chan ApplyMsg
	inLinkCh              chan inLink
	outLinkCh             chan outLink
	appendEntriesJustSent []time.Time

	currentTerm int
	votedFor    int
	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
}

type getStateReq struct{}

type getStateReply struct {
	term     int
	isLeader bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	link := newInLink(getStateReq{})
	select {
	case <-rf.killed:
		return 0, false
	case rf.inLinkCh <- link:
	}

	select {
	case <-rf.killed:
		return 0, false
	case iReply := <-link.replyCh:
		reply := iReply.(getStateReply)
		return reply.term, reply.isLeader
	}
}

type getLogEntryTermReq struct {
	index int
}

type getLogEntryTermReply struct {
	ok   bool
	term int
}

func (rf *Raft) GetLogEntryTerm(index int) (bool, int) {
	link := newInLink(getLogEntryTermReq{})
	select {
	case <-rf.killed:
		return false, 0
	case rf.inLinkCh <- link:
	}

	select {
	case <-rf.killed:
		return false, 0
	case iReply := <-link.replyCh:
		reply := iReply.(getLogEntryTermReply)
		return reply.ok, reply.term
	}
}

func (rf *Raft) handleGetLogEntryTermReq(link inLink) getLogEntryTermReply {
	req := link.req.(getLogEntryTermReq)
	if req.index >= len(rf.log) {
		return getLogEntryTermReply{false, 0}
	}
	e := rf.log[req.index]
	return getLogEntryTermReply{true, e.Term}
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

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteReq struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(req RequestVoteReq, reply *RequestVoteReply) {
	// Your code here.
	link := newInLink(req)
	select {
	case <-rf.killed:
		return
	case rf.inLinkCh <- link:
	}

	select {
	case <-rf.killed:
		return
	case iReply := <-link.replyCh:
		*reply = iReply.(RequestVoteReply)
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
func (rf *Raft) sendRequestVote(server int, req RequestVoteReq, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", req, reply)
	return ok
}

func (rf *Raft) upToDate(lastLogIndex int, lastLogTerm int) bool {
	myLastLogIndex := len(rf.log) - 1
	var myLastLogTerm int
	if myLastLogIndex == -1 { // maybe more assertions
		myLastLogTerm = 0
	} else {
		myLastLogTerm = rf.log[myLastLogIndex].Term
	}
	if myLastLogTerm != lastLogTerm {
		return myLastLogTerm < lastLogTerm
	}
	return myLastLogIndex <= lastLogIndex
}

func (rf *Raft) handleRequestVoteReq(link inLink) (reply RequestVoteReply, suppressed bool) {
	req := link.req.(RequestVoteReq)
	reply = RequestVoteReply{}
	suppressed = false

	if req.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

	} else if req.Term == rf.currentTerm {
		reply.Term = rf.currentTerm

		if rf.upToDate(req.LastLogIndex, req.LastLogTerm) && (rf.votedFor == -1 || rf.votedFor == req.CandidateId) {
			rf.votedFor = req.CandidateId
			rf.persist()
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}

	} else {
		rf.currentTerm = req.Term
		reply.Term = rf.currentTerm

		if rf.upToDate(req.LastLogIndex, req.LastLogTerm) {
			rf.votedFor = req.CandidateId
			reply.VoteGranted = true
		} else {
			rf.votedFor = -1
			reply.VoteGranted = false
		}
		suppressed = true
		rf.persist()
	}
	return
}

type startReq struct {
	command interface{}
}

type startReply struct {
	index    int
	term     int
	isLeader bool
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
	link := newInLink(startReq{command})
	select {
	case <-rf.killed:
		return -1, 0, false
	case rf.inLinkCh <- link:
	}

	select {
	case <-rf.killed:
		return -1, 0, false
	case iReply := <-link.replyCh:
		reply := iReply.(startReply)
		return reply.index, reply.term, reply.isLeader
	}
}

type AppendEntriesReq struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       AppendEntriesError
	ConflictFirst int
	me            int
	req           AppendEntriesReq
}

func (rf *Raft) AppendEntries(req AppendEntriesReq, reply *AppendEntriesReply) {
	// Your code here.
	link := newInLink(req)
	select {
	case <-rf.killed:
		return
	case rf.inLinkCh <- link:
	}

	select {
	case <-rf.killed:
		return
	case iReply := <-link.replyCh:
		*reply = iReply.(AppendEntriesReply)
	}
}

func (rf *Raft) sendAppendEntries(server int, req AppendEntriesReq, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", req, reply)
	return ok
}

func (rf *Raft) checkPrevLogEntry(prevLogIndex int, prevLogTerm int) bool {
	if prevLogIndex < -1 {
		panic("prevLogIndex < -1")
	}
	if prevLogIndex == -1 {
		return true
	}
	if prevLogIndex >= len(rf.log) {
		return false
	}
	return rf.log[prevLogIndex].Term == prevLogTerm
}

func (rf *Raft) appendEntriesToLocal(prevLogIndex int, entries []LogEntry) int {
	for i := 0; i < len(entries); i++ {
		j := prevLogIndex + 1 + i
		if j < len(rf.log) {
			if rf.log[j].Term != entries[i].Term {
				rf.log = rf.log[:j]
				rf.log = append(rf.log, entries[i])
			} else {
				if rf.log[j].Command != entries[i].Command {
					panic("same index and term but different command")
				}
			}
		} else {
			rf.log = append(rf.log, entries[i])
		}
	}
	rf.persist()
	return prevLogIndex + len(entries)
}

func (rf *Raft) handleAppendEntriesReq(link inLink) (reply AppendEntriesReply, suppressed bool) {
	req := link.req.(AppendEntriesReq)
	reply = AppendEntriesReply{}
	suppressed = false

	if req.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = eAppendEntriesLessTerm

	} else {
		suppressed = true
		if req.Term == rf.currentTerm {
			reply.Term = rf.currentTerm
		} else {
			rf.currentTerm = req.Term
			rf.votedFor = -1
			rf.persist()
			reply.Term = rf.currentTerm
		}

		if !rf.checkPrevLogEntry(req.PrevLogIndex, req.PrevLogTerm) {
			reply.Success = eAppendEntriesLogInconsistent

			var cf int
			if req.PrevLogIndex < len(rf.log) {
				cf = req.PrevLogIndex
			} else {
				cf = len(rf.log) - 1
			}
			if cf < 0 {
				cf = 0
			} else {
				ct := rf.log[cf].Term
				for cf > 0 && rf.log[cf-1].Term == ct {
					cf -= 1
				}
			}
			reply.ConflictFirst = cf
		} else {
			reply.Success = eAppendEntriesOk
			lastNewEntry := rf.appendEntriesToLocal(req.PrevLogIndex, req.Entries)
			if req.LeaderCommit > rf.commitIndex {
				if lastNewEntry < req.LeaderCommit {
					if rf.commitIndex < lastNewEntry {
						rf.commitIndex = lastNewEntry
					}
				} else {
					rf.commitIndex = req.LeaderCommit
				}
			}
			rf.applyIfPossible()
		}
	}
	return
}

type killReq struct{}

type killReply struct{}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	link := newInLink(killReq{})
	select {
	case <-rf.killed:
		return // wtf
	case rf.inLinkCh <- link:
	}
	<-rf.killed
}

func (rf *Raft) sendRequests() {
	for {
		select {
		case <-rf.killed:
			return

		case link := <-rf.outLinkCh:
			// DPrintf("raft[%d] sendRequests outLink = %+v", rf.me, link)
			for peer, iReq := range link.reqs {
				switch iReq.(type) {

				case RequestVoteReq:
					go func(i1 int, req RequestVoteReq) {
						reply := RequestVoteReply{}
						ok := rf.sendRequestVote(i1, req, &reply)
						if !ok {
							reply.Term = 0
							reply.VoteGranted = false
						}
						select {
						case <-rf.killed:
						case <-link.ignoreRepliesCh:
						case link.replyCh <- reply:
						}
					}(peer, iReq.(RequestVoteReq))

				case AppendEntriesReq:
					go func(i1 int, req AppendEntriesReq) {
						reply := AppendEntriesReply{}
						ok := rf.sendAppendEntries(i1, req, &reply)
						if !ok {
							reply.Term = 0
							reply.Success = eAppendEntriesErr
						}
						reply.me = i1
						reply.req = req
						select {
						case <-rf.killed:
						case <-link.ignoreRepliesCh:
						case link.replyCh <- reply:
						}
					}(peer, iReq.(AppendEntriesReq))

				default:
					panic("unknown req to send")
				}
			}
		}
	}
}

func (rf *Raft) run() {
	for {
		select {
		case <-rf.killed:
			close(rf.applyCh)
			return
		default:
		}

		switch rf.state {
		case eFollower:
			rf.runAsFollower()
		case eCandidate:
			rf.runAsCandidate()
		case eLeader:
			rf.runAsLeader()
		default:
			panic("unknown state")
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
	rf.state = eFollower
	rf.killed = make(chan struct{})
	rf.applyCh = applyCh
	rf.inLinkCh = make(chan inLink)
	rf.outLinkCh = make(chan outLink)

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = -1
	rf.lastApplied = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.run()
	go rf.sendRequests()
	return rf
}
