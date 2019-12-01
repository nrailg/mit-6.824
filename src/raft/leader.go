package raft

import (
	"fmt"
	"time"
)

const (
	leaderIdle = 50
)

func (rf *Raft) sendHeartbeatToPeers(prevOutLink outLink) outLink {
	//DPrintf("raft[%d] sendHeartbeatToPeers", rf.me)
	n := len(rf.peers)
	reqs := make(map[int]interface{})
	for i := 0; i < n; i++ {
		if i == rf.me {
			continue
		}
		prevLogIndex := rf.nextIndex[i] - 1
		prevLogTerm := 0
		if prevLogIndex >= 0 {
			prevLogTerm = rf.log[prevLogIndex].Term
		}
		req := AppendEntriesReq{
			rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, make([]LogEntry, 0), rf.commitIndex,
		}
		reqs[i] = req
	}
	olink := newOutLinkWithReplyCh(reqs, prevOutLink.replyCh)
	select {
	case <-rf.killed:
		panic("killed")
	case rf.outLinkCh <- olink:
	}
	return olink
}

func (rf *Raft) sendAppendEntriesToPeers(prevOutLink outLink) outLink {
	n := len(rf.peers)
	reqs := make(map[int]interface{})
	for i := 0; i < n; i++ {
		if i == rf.me {
			continue
		}
		if len(rf.log) > rf.nextIndex[i] {
			prevLogIndex := rf.nextIndex[i] - 1
			if prevLogIndex < -1 {
				panic(fmt.Sprintf("prevLogIndex[%d] < -1", i))
			}
			prevLogTerm := 0
			if prevLogIndex >= 0 {
				prevLogTerm = rf.log[prevLogIndex].Term
			}
			nEntries := len(rf.log) - rf.nextIndex[i]
			entries := make([]LogEntry, nEntries)
			si := rf.nextIndex[i]
			sj := si + nEntries
			copy(entries, rf.log[si:sj])
			req := AppendEntriesReq{
				rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, entries, rf.commitIndex,
			}
			reqs[i] = req
		}
	}
	olink := newOutLinkWithReplyCh(reqs, prevOutLink.replyCh)
	select {
	case <-rf.killed:
		panic("killed")
	case rf.outLinkCh <- olink:
	}
	return olink
}

func (rf *Raft) updateNextIndexAndMatchIndex(reply AppendEntriesReply) {
	req := reply.req
	peer := reply.me

	if reply.Success == eAppendEntriesOk {
		nAppended := len(req.Entries)
		rf.nextIndex[peer] = req.PrevLogIndex + nAppended + 1
		rf.matchIndex[peer] = rf.nextIndex[peer] - 1

	} else if reply.Success == eAppendEntriesLogInconsistent {
		// TODO more carefully
		if rf.nextIndex[peer] > req.PrevLogIndex {
			rf.nextIndex[peer] = req.PrevLogIndex
		}

	} else {
		//DPrintf("rf[%d].updateNextIndexAndMatchIndex: reply = %v", rf.me, reply)
	}
}

func (rf *Raft) updateCommitIndex() {
	n := len(rf.peers)
	maj := n/2 + 1
	for nci := rf.commitIndex + 1; nci < len(rf.log); nci++ {
		cnt := 1
		for i := 0; i < n; i++ {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= nci {
				cnt++
			}
		}
		if cnt >= maj && rf.log[nci].Term == rf.currentTerm {
			rf.commitIndex = nci
		}
	}
}

func (rf *Raft) applyIfPossible() {
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		applyMsg := ApplyMsg{i + 1, rf.log[i].Command, false, nil}
		//DPrintf("raft[%d] apply %+v", rf.me, applyMsg)
		rf.applyCh <- applyMsg
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) handleAppendEntriesReply(reply AppendEntriesReply) (suppressed bool) {
	if reply.Term < rf.currentTerm {
		// TODO actually should not be here, confusing, bad.
		return false
	} else if reply.Term == rf.currentTerm {
		rf.updateNextIndexAndMatchIndex(reply)
		rf.updateCommitIndex()
		rf.applyIfPossible()
		return false
	} else {
		if reply.Success != eAppendEntriesLessTerm {
			panic("reply.Success != eAppendEntriesLessTerm")
		}
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		return true
	}
}

func (rf *Raft) runAsLeader() {
	//DPrintf("raft[%d] runAsLeader", rf.me)

	n := len(rf.peers)
	rf.nextIndex = make([]int, n)
	rf.matchIndex = make([]int, n)
	for i := 0; i < n; i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = -1
	}

	// TODO use only 1 replyCh, don't ignore any longer.
	olink := newOutLink(make(map[int]interface{}))
	olink = rf.sendHeartbeatToPeers(olink)
	defer olink.ignoreReplies()

	ticker := time.NewTicker(time.Duration(leaderIdle) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-rf.killed:
			panic("killed")

		case <-ticker.C: // time for next heartbeat
			olink = rf.sendHeartbeatToPeers(olink)

		case iReply := <-olink.replyCh:
			reply := iReply.(AppendEntriesReply)
			suppressed := rf.handleAppendEntriesReply(reply)
			if suppressed {
				rf.state = eFollower
				return
			}
			olink = rf.sendAppendEntriesToPeers(olink)

		case ilink := <-rf.inLinkCh:
			//DPrintf("raft[%d](leader) ilink %+v", rf.me, ilink)
			req := ilink.req

			switch req.(type) {
			case killReq:
				close(rf.killed)
				return

			case getStateReq:
				select {
				case <-rf.killed:
					return
				case ilink.replyCh <- getStateReply{rf.currentTerm, true}:
				}

			case startReq:
				reply := startReply{len(rf.log), rf.currentTerm, true}
				select {
				case <-rf.killed:
					return
				case ilink.replyCh <- reply:
				}
				sreq := req.(startReq)
				rf.log = append(rf.log, LogEntry{sreq.command, rf.currentTerm})
				olink = rf.sendAppendEntriesToPeers(olink)

			case RequestVoteReq:
				reply, suppressed := rf.handleRequestVoteReq(ilink)
				select {
				case <-rf.killed:
					return
				case ilink.replyCh <- reply:
				}
				if suppressed {
					rf.state = eFollower
					return
				}

			case AppendEntriesReq:
				reply, suppressed := rf.handleAppendEntriesReq(ilink)
				select {
				case <-rf.killed:
					return
				case ilink.replyCh <- reply:
				}
				if suppressed {
					rf.state = eFollower
					return
				}

			default:
				panic(fmt.Sprintf("unknown req = %+v", req))
			}
		}
	}
}
