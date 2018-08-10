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

import "sync"
import (
	"math/rand"
	"time"

	//"io"
	//"net/http"
	"labrpc"
	//"bytes"
	//"labgob"
	//"context"
	//"net"
	"fmt"

)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// volatile state on leaders
	nextIndex, matchIndex []int
	// volatile state on all servers
	commitIndex, lastApplied int
	// persistent state on all servers
	log             []LogEntry
	currentTerm     int
	votedFor        int
	votes           int
	state           ServerState
	applyCh         chan ApplyMsg
	debugFlag       bool
	timer           *time.Timer
	winElectionCh   chan bool
	shouldContCh    chan bool
	randDevice      *rand.Rand
	electionTimeout time.Duration
}

type LogEntry struct {
	Command            interface{}
	Term, CommandIndex int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (term int, isLeader bool) {
	//Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	vs := rf.votes
	m := rf.me
	isLeader = false
	if rf.votes > len(rf.peers)/2 {
		isLeader = true
	}
	DPrintf("server %d term: %d votes: %d log: %v\n", m, term, vs, rf.log)
	rf.mu.Unlock()
	return
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term, CandidateId, LastLogIndex, LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	reason := ""
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	var atLeastUptoDate = false
	var canVote = false
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		canVote = true
	} else {
		reason = fmt.Sprintf("has voted for %d\n", rf.votedFor)
	}

	// candidate should more up-to-date
	if args.LastLogTerm != rf.log[len(rf.log)-1].Term {
		if rf.log[len(rf.log)-1].Term <= args.LastLogTerm {
			atLeastUptoDate = true
		} else {
			reason += fmt.Sprintf("candidate%d lastlog term %d < follower%d term : %d\n", args.CandidateId, args.LastLogTerm, rf.me, rf.log[len(rf.log)-1].Term)
		}
	} else {		if rf.log[len(rf.log)-1].CommandIndex <= args.LastLogIndex {
			atLeastUptoDate = true
		} else {
			reason += fmt.Sprintf("candidate%d last logidx %d < follower %d last idx: %d\n", args.CandidateId, args.LastLogIndex, rf.me, len(rf.log))
		}
	}
	//if args.Term > rf.currentTerm {
	//	DPrintf("%d(term :%d) receive %d's higher term %d RV , back to follower\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	//	rf.state = Follower
	//	rf.currentTerm = args.Term
	//	rf.votes = 0
	//	rf.votedFor = -1
	//	rf.timer.Reset(rf.electionTimeout)
	//	if rf.state == Candidate {
	//		rf.winElectionCh <- false
	//	}
	//} else


	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reason += fmt.Sprintf("candidate%d term %d is older than follower %d term:%d\n", args.CandidateId, args.Term, rf.me, rf.currentTerm)
	} else {
		if atLeastUptoDate && canVote {
			rf.currentTerm = args.Term
			rf.timer.Reset(rf.electionTimeout)
			DPrintf("%d votedfor %d --> %d\n", rf.me, rf.votedFor, args.CandidateId)
			rf.votedFor = args.CandidateId
			rf.state = Follower
			rf.votes = 0
			reply.VoteGranted = true
		} else if rf.currentTerm < args.Term{
			rf.state = Follower
			rf.votes = 0
			rf.votedFor = -1
			rf.currentTerm = args.Term
			rf.timer.Reset(rf.electionTimeout)
			reason += "however meet high term, so back to follower\n"
		}
	}
	rf.mu.Unlock()

	if reply.VoteGranted {
		DPrintf("server %d votes for server %d\n", rf.me, args.CandidateId)
	} else {
		DPrintf("server %d not vote for server %d.REASON: %s\n", rf.me, args.CandidateId, reason)
	}
	return
}

type AppendEntryArgs struct {
	Term, LeaderId, PrevLogIndex, PrevLogTerm, LeaderCommit int
	Entries                                                 []LogEntry
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	reply.Term = rf.currentTerm
	reply.Success = true

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	rf.currentTerm = args.Term

	if len(args.Entries) == 0 {
		DPrintf("%d got heartbeat from leader%d\n", rf.me, args.LeaderId)
	} else {
		DPrintf("%d got AE RPC, reset timer.\n", rf.me)
	}

	DPrintf("%d back to follower cause of leader%d leader's term:%d \n", rf.me, args.LeaderId, args.Term)

	rf.mu.Lock()

	if rf.state == Candidate {
		DPrintf("%d send false to winCh\n", rf.me)
		rf.winElectionCh <- false
	}

	rf.state = Follower
	rf.votes = 0
	rf.votedFor = -1


	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
	}

	// begCmpIdx := args.Entries[0].CommandIndex-1
	// lastLog := rf.log[len(rf.log)-1].CommandIndex-1
	// lastEntryIdx := args.Entries[len(args.Entries)-1].CommandIndex-1

	// endCmpidx := 0
	// if lastLog>lastEntryIdx {
	// 	endCmpidx =  lastEntryIdx
	// } else {
	// 	endCmpidx =  lastLog
	// }

	// for i := begCmpIdx; i <= endCmpidx; i++ {
	// 	if rf.log[i].Term != args.Entries[i-begCmpIdx].Term {
	// 		rf.log = rf.log[:i]
	// 		break
	// 	}
	// }
	//
	//for i := args.Entries[0].CommandIndex - 1; i <= args.Entries[len(args.Entries)-1].CommandIndex-1 && i < len(rf.log); i++ {
	//	if rf.log[i].Term != args.Entries[i].Term {
	//		rf.log = rf.log[:i]
	//		break
	//	}
	//}

	for i := 0; i < len(args.Entries); i++ {
		if rf.log[args.Entries[i].CommandIndex-1].Term != args.Entries[i].Term {
			rf.log = rf.log[:args.Entries[i].CommandIndex-1]
			DPrintf("%d drop command idx:%d and following\n", args.Entries[i].CommandIndex-1)
			break
		}
	}
	rf.log = append(rf.log, args.Entries...)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < args.Entries[len(args.Entries)-1].CommandIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = args.Entries[len(args.Entries)-1].CommandIndex
		}
	}
	rf.timer.Reset(rf.electionTimeout)
	rf.mu.Unlock()
	return
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1


	// Your code here (2B).

	return index, term, rf.state == Leader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	rf.debugFlag = false
	rf.mu.Unlock()
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


func (rf *Raft) throwrand() {
	rf.electionTimeout =  (time.Duration(rf.randDevice.Int63()%10 * 40+300)) * time.Millisecond
}

func sendRVRPC(rf *Raft, args *RequestVoteArgs) {
	for {
		DPrintf("%d wait for next round of RV RPC\n" ,rf.me)
		shouldContinue := <- rf.shouldContCh
		if shouldContinue {
			rf.mu.Lock()
			if rf.state == Candidate {
				DPrintf("%d begin new round of RV RPC with term %d\n", rf.me, rf.currentTerm)
				rf.mu.Unlock()
				wg := sync.WaitGroup{}
				wg.Add(len(rf.peers) - 1)
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						go func(idx int) {
							reply := &RequestVoteReply{}
							DPrintf("%d send RV to %d\n", rf.me, idx)
							if ok := rf.sendRequestVote(idx, args, reply); ok && reply.VoteGranted {
								rf.mu.Lock()
								if rf.state == Candidate {
									rf.votes++
									DPrintf("-------server %d got vote from server %d now has %d votes\n", rf.me, idx, rf.votes)
									if rf.votes > len(rf.peers)/2 {
										DPrintf("%d win election\n", rf.me)
										rf.state = Leader
										DPrintf("sending true in win channel\n")
										rf.winElectionCh <- true
										rf.mu.Unlock()
										wg.Done()
										DPrintf("%d become leader.QUIT\n", rf.me)
										return
									}
								} else {
									DPrintf("-------server %d state: %v got vote from server %d BUT IGNORE IT current votes: %d\n", rf.me, rf.state, idx, rf.votes)
								}
								rf.mu.Unlock()
							} else {
								rf.mu.Lock()
								if reply.Term > rf.currentTerm {

									rf.currentTerm = reply.Term
									rf.state = Follower
									rf.votes = 0
									rf.votedFor = -1
									rf.timer.Reset(rf.electionTimeout)

									DPrintf("%d back to follower cause receive reply with Term %d but current term is %d\n", rf.me, reply.Term, rf.currentTerm)
								}
								rf.mu.Unlock()
							}
							wg.Done()
						}(i)
					}
				}
				wg.Wait()

				rf.mu.Lock()
				if rf.state == Candidate && rf.votes > len(rf.peers)/2 {
					DPrintf("%d win election\n", rf.me)
					rf.state = Leader
					DPrintf("sending true in win channel\n")
					rf.winElectionCh <- true
					rf.mu.Unlock()
					DPrintf("%d become leader.QUIT\n", rf.me)
					return
				}
				rf.mu.Unlock()
			} else {
				rf.mu.Unlock()
				DPrintf("%d is no longer candidate.QUIT\n", rf.me)
				return
			}
		} else {
			DPrintf("%d receive false shouldCont so QUIT\n", rf.me)
			return
		}
	}
}

func (rf *Raft)resetTimer() {
	rf.mu.Lock()
	rf.timer.Reset(rf.electionTimeout)
	rf.mu.Unlock()
}

func (rf *Raft)backToFollower() {
	rf.mu.Lock()
	rf.state = Follower
	rf.votedFor = -1
	rf.votes = 0
	DPrintf("%d back to follower\n", rf.me)
	rf.mu.Unlock()
}

func (rf *Raft)becomeCandidate() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.mu.Unlock()
}

func (rf *Raft)becomeLeader() {
	rf.mu.Lock()
	rf.state = Leader
	rf.mu.Unlock()
}

func (rf *Raft)stopTimer() {
	rf.mu.Lock()
	rf.mu.Unlock()
	rf.timer.Stop()
}
const (
	heartBeatInterval = 50 * time.Millisecond
)

func sendPeriodHeartBeat2(rf *Raft) {
	wg := sync.WaitGroup{}
	wg.Add(len(rf.peers)-1)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(idx int)  {
			for {
				rf.mu.Lock()
				if rf.state == Leader {
					args := &AppendEntryArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.log[len(rf.log)-1].CommandIndex,
						PrevLogTerm:  rf.log[len(rf.log)-1].Term,
						LeaderCommit: rf.commitIndex,
						Entries:      []LogEntry{},
					}
					rf.mu.Unlock()
					reply := &AppendEntryReply{}
					DPrintf("woke up.leader%d heartbeat to %d begin\n", rf.me, idx)

					c := make(chan bool, 1)
					go func() {
						_ = rf.sendAppendEntries(idx, args, reply)
						c <- true
					}()

					go func() {
						time.Sleep(heartBeatInterval)
						c <- false
					}()

					if ok := <-c; !ok {
						DPrintf("leader%d heartbeat to %d timeout\n", rf.me, idx)
						continue
					}

					DPrintf("leader%d heartbeat to %d RPC returned\n", rf.me, idx)
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.votes = 0
						rf.votedFor = -1
						rf.timer.Reset(rf.electionTimeout)

						DPrintf("%d back to follower cause heartbeat reply term:%d, but currentTerm: %d\n", rf.me, args.Term, rf.currentTerm)
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
				} else {
					rf.mu.Unlock()
					DPrintf("%d is no longer leader.Hearbeat go routine %d quit \n", rf.me, idx)
					wg.Done()
					return
				}
				DPrintf("leader%d heartbeat to %d finished, go sleep\n", rf.me, idx)
				time.Sleep(heartBeatInterval)
			}
		}(i)
	}

	wg.Wait()

}


func sendPeriodHeartBeat(rf *Raft) {
	L: for {
		rf.mu.Lock()
		switch rf.state {
		case Leader:
			rf.mu.Unlock()
			DPrintf("%d send heartbeats begin\n", rf.me)
			wg := &sync.WaitGroup{}
			wg.Add(len(rf.peers)-1)
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go func(idx int) {
						rf.mu.Lock()
						args := &AppendEntryArgs{
							Term:         rf.currentTerm,
							LeaderId:     rf.me,
							PrevLogIndex: rf.log[len(rf.log)-1].CommandIndex,
							PrevLogTerm:  rf.log[len(rf.log)-1].Term,
							LeaderCommit: rf.commitIndex,
							Entries:      []LogEntry{},
						}
						rf.mu.Unlock()
						reply := &AppendEntryReply{}

						_ = rf.sendAppendEntries(idx, args, reply);
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {

							rf.currentTerm = reply.Term
							rf.state = Follower
							rf.votes = 0
							rf.votedFor = -1
							rf.timer.Reset(rf.electionTimeout)

							DPrintf("%d back to follower cause heartbeat reply term: %d, but currentTerm: %d\n", rf.me, reply.Term, rf.currentTerm)
							rf.mu.Unlock()
							wg.Done()
							return
						} else {
							rf.mu.Unlock()
						}

						wg.Done()
					}(i)
				}
			}
			wg.Wait()
			DPrintf("%d finish send all heartbeats.Going sleep\n", rf.me)
			time.Sleep(heartBeatInterval)
		default:
			rf.mu.Unlock()
			break L
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = Follower
	rf.applyCh = applyCh
	// persisten state on all servers
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{}
	rf.log = append(rf.log, LogEntry{nil, 0, 0})
	// volatile state on all servers
	rf.commitIndex = 0
	rf.lastApplied = 0
	// leader's volatile state
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.debugFlag = true
	rf.randDevice = rand.New(rand.NewSource(int64(rf.me)))
	rf.throwrand()
	rf.timer = time.NewTimer(rf.electionTimeout)
	rf.winElectionCh = make(chan bool, 1)

	DPrintf("%d's timeout: %d\n", rf.me, rf.electionTimeout)
	// Your initialization code here (2A, 2B, 2C).
	go func() {
		rf.resetTimer()
		for {
			switch rf.state {
			case Follower:
				<-rf.timer.C
				// 超时前没有收到AE 和RV RPC，所以变为candidate
				rf.becomeCandidate()
			case Candidate:
				DPrintf("server %d begin election\n", rf.me)
				rf.currentTerm = rf.currentTerm + 1
				rf.votes = 1
				rf.votedFor = rf.me
				rf.timer.Reset(rf.electionTimeout)
				args := &RequestVoteArgs{rf.currentTerm, rf.me, rf.log[len(rf.log)-1].CommandIndex, rf.log[len(rf.log)-1].Term}
				rf.shouldContCh = make(chan bool, 1)
				go sendRVRPC(rf, args)
				rf.shouldContCh <- true
				var res bool
				CandiLoop: for {
					select {
					case <-rf.timer.C:
						DPrintf("%d election time out, cont\n", rf.me)
						rf.mu.Lock()
						rf.currentTerm = rf.currentTerm + 1
						rf.votes = 1
						rf.votedFor = rf.me
						rf.shouldContCh <- false
						rf.shouldContCh = make(chan bool, 1)
						args := &RequestVoteArgs{rf.currentTerm, rf.me, rf.log[len(rf.log)-1].CommandIndex, rf.log[len(rf.log)-1].Term}
						go sendRVRPC(rf, args)
						rf.shouldContCh <- true
						rf.mu.Unlock()
						rf.resetTimer()
					case res = <-rf.winElectionCh:
						DPrintf("%d got %v from winChannel\n", rf.me, res)
						rf.shouldContCh <- false
						if res {
							//赢得选举
							rf.becomeLeader()
							rf.stopTimer()
							break CandiLoop
						} else {
							//收到有高term的leader的AE RPC
							//rf.backToFollower()
							//rf.resetTimer()
							break CandiLoop
						}
					}
				}
			case Leader:
				go sendPeriodHeartBeat2(rf)
				LdLoop: for {
					rf.mu.Lock()
					switch rf.state {
					case Leader:
						rf.mu.Unlock()
						if rf.debugFlag {
							DPrintf("%d is leader now ~~", rf.me)
						}
						time.Sleep(200*time.Millisecond)
					case Follower:
						rf.mu.Unlock()
						DPrintf("%d is no longer leader.Back to follower \n", rf.me)
						break LdLoop
					}
				}
			}
		}	
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
