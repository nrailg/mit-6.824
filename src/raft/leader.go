package raft

import (
	"fmt"
	"time"
)

const (
	leaderIdle = 50
)

func (rf *Raft) sendHeartbeats() outLink {
	//DPrintf("raft[%d] sendHeartbeats", rf.me)

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
	outLink := newOutLink(reqs)
	select {
	case <-rf.killed:
		panic("unexpected")
	case rf.outLinkCh <- outLink:
	}
	return outLink
}

func (rf *Raft) handleAppendEntriesReply(reply AppendEntriesReply) (suppressed bool) {
	if reply.Term < rf.currentTerm {
		// actually should not be here, confusing, bad.
		return false
	} else if reply.Term == rf.currentTerm {
		//rf.updateNextIndexAndMatchIndex(reply)
		//rf.updateCommitIndex()
		//rf.applyIfPossible()
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

	outLink := rf.sendHeartbeats()
	defer outLink.ignoreReplies()

	ticker := time.NewTicker(time.Duration(leaderIdle) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-rf.killed:
			panic("unexpected")

		case <-ticker.C: // time for next heartbeat
			outLink.ignoreReplies()
			outLink = rf.sendHeartbeats()

		case iReply := <-outLink.replyCh:
			reply := iReply.(AppendEntriesReply)
			suppressed := rf.handleAppendEntriesReply(reply)
			if suppressed {
				rf.state = eFollower
				return
			}

		case inLink := <-rf.inLinkCh:
			//DPrintf("raft[%d](leader) inLink %+v", rf.me, inLink)
			req := inLink.req

			switch req.(type) {
			case killReq:
				close(rf.killed)
				return

			case getStateReq:
				select {
				case <-rf.killed:
					return
				case inLink.replyCh <- getStateReply{rf.currentTerm, true}:
				}

			case RequestVoteReq:
				reply, suppressed := rf.handleRequestVoteReq(inLink)
				select {
				case <-rf.killed:
					return
				case inLink.replyCh <- reply:
				}
				if suppressed {
					rf.state = eFollower
					return
				}

			case AppendEntriesReq:
				reply, suppressed := rf.handleAppendEntriesReq(inLink)
				select {
				case <-rf.killed:
					return
				case inLink.replyCh <- reply:
				}
				if suppressed {
					rf.state = eFollower
					return
				}

			default:
				// TODO panic with details
				panic(fmt.Sprintf("unknown req = %+v", req))
			}
		}
	}
}
