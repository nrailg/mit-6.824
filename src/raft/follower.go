package raft

import (
	"fmt"
	"math/rand"
	"time"
)

func (rf *Raft) _runAsFollower() {
	a, b := electionTimeoutMin, electionTimeoutMax
	electionTimeout := time.Duration((rand.Intn(b-a) + a)) * time.Millisecond
	timer := time.NewTimer(electionTimeout)
	defer timer.Stop()

	for {
		select {
		case <-rf.killed:
			panic("killed")

		case <-timer.C:
			rf.state = eCandidate
			return

		case ilink := <-rf.inLinkCh:
			req := ilink.req
			switch req.(type) {
			case killReq:
				close(rf.killed)
				return

			case getStateReq:
				select {
				case <-rf.killed:
					return
				case ilink.replyCh <- getStateReply{rf.currentTerm, false}:
				}

			case startReq:
				select {
				case <-rf.killed:
					return
				case ilink.replyCh <- startReply{-1, rf.currentTerm, false}:
				}

			case snapshotReq:
				reply := rf.handleSnapshotReq(ilink)
				select {
				case <-rf.killed:
					return
				case ilink.replyCh <- reply:
				}

			case RequestVoteReq:
				reply, _ := rf.handleRequestVoteReq(ilink)
				select {
				case <-rf.killed:
				case ilink.replyCh <- reply:
				}
				//DPrintf("raft[%d] suppressed by RequestVoteReq", rf.me)
				return

			case AppendEntriesReq:
				reply, _ := rf.handleAppendEntriesReq(ilink)
				select {
				case <-rf.killed:
				case ilink.replyCh <- reply:
				}
				//DPrintf("raft[%d] suppressed by AppendEntriesReq", rf.me)
				return

			default:
				panic(fmt.Sprintf("unknown req = %+v", req))
			}
		}
	}
}

func (rf *Raft) runAsFollower() {
	//DPrintf("raft[%d] runAsFollower", rf.me)
	for rf.state == eFollower {
		select {
		case <-rf.killed:
			return
		default:
		}
		rf._runAsFollower()
	}
}
