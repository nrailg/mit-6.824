package raft

import (
	"fmt"
	"math/rand"
	"time"
)

func (rf *Raft) runAsCandidate() {
	// DPrintf("raft[%d] runAsCandidate", rf.me)

	rf.currentTerm++
	rf.votedFor = rf.me

	n := len(rf.peers)
	if n == 1 {
		rf.state = eLeader
		return
	}

	// send RequestVote to peers
	lastLogIndex := len(rf.log) - 1
	var lastLogTerm int
	if lastLogIndex == -1 {
		lastLogTerm = 0
	} else {
		lastLogTerm = rf.log[lastLogIndex].Term
	}
	reqs := make(map[int]interface{})
	for i := 0; i < n; i++ {
		if i == rf.me {
			continue
		}
		req := RequestVoteReq{
			rf.currentTerm, rf.me, lastLogIndex, lastLogTerm,
		}
		reqs[i] = req
	}
	outLink := newOutLink(reqs)
	defer outLink.ignoreReplies()
	select {
	case <-rf.killed:
		panic("killed")
	case rf.outLinkCh <- outLink:
	}

	a, b := electionTimeoutMin, electionTimeoutMax
	electionTimeout := time.Duration((rand.Intn(b-a) + a)) * time.Millisecond
	timer := time.NewTimer(electionTimeout)
	defer timer.Stop()

	vote := 1
	quorum := n/2 + 1
	for {
		select {
		case <-rf.killed:
			return

		case <-timer.C:
			return

		case iReply := <-outLink.replyCh:
			reply := iReply.(RequestVoteReply)
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = eFollower
				return
			} else {
				if reply.VoteGranted {
					vote++
					if vote == quorum {
						// DPrintf("raft[%d] (term = %d) become leader", rf.me, rf.currentTerm)
						rf.state = eLeader
						return
					}
				}
			}

		case inLink := <-rf.inLinkCh:
			req := inLink.req

			switch req.(type) {
			case killReq:
				close(rf.killed)
				return

			case getStateReq:
				select {
				case <-rf.killed:
					return
				case inLink.replyCh <- getStateReply{rf.currentTerm, false}:
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
				panic(fmt.Sprintf("unknown req = %+v", req))
			}
		}
	}
}
