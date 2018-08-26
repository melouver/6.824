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
	"labgob"
	"sync"
)
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
	log         []LogEntry
	currentTerm int
	votedFor    int
	votes       int
	state       ServerState

	applyCh         chan ApplyMsg
	internalApplyCh chan ApplyMsg

	debugFlag     bool
	timer         *time.Timer
	winElectionCh chan bool
	shouldCont    int // 0: wait | 1: cont
	contCond      *sync.Cond

	shouldSendHB int
	hbCond       *sync.Cond

	shouldSendAE     int
	aeCond           *sync.Cond

	shouldAdvIdxFlag int
	advCommitIdxCond *sync.Cond

	applyCond *sync.Cond
	backToFollowerCond *sync.Cond
	randDevice      *rand.Rand
	electionTimeout time.Duration
	rvArgs          RequestVoteArgs
}

type LogEntry struct {
	Command            interface{}
	Term, CommandIndex int
}

func (l LogEntry)String() string {
	return fmt.Sprintf("idx:%d Term:%d Cmd:%v ", l.CommandIndex, l.Term, l.Command)
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
	if rf.state == Leader {
		isLeader = true
	}
	DPrintf("server %d term: %d votes: %d log: %v state:%d\n", m, term, vs, rf.log, rf.state)
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
			panic("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
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
	defer rf.mu.Unlock()

	reason := ""
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.currentTerm < args.Term { // Rules for all servers
		rf.currentTerm = args.Term
		rf.leaderBackToFollower(fmt.Sprintf("Got high term RV.current is %d RV term is %d", rf.currentTerm, args.Term), false)
		rf.persist()
	}

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

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
			reason += fmt.Sprintf("candidate%d lastlog term %d < follower%d term : %d\n",
				args.CandidateId, args.LastLogTerm, rf.me, rf.log[len(rf.log)-1].Term)
		}
	} else {
		if rf.log[len(rf.log)-1].CommandIndex <= args.LastLogIndex {
			atLeastUptoDate = true
		} else {
			reason += fmt.Sprintf("candidate%d last logidx %d < follower %d last idx: %d\n",
				args.CandidateId, args.LastLogIndex, rf.me, len(rf.log)-1)
		}
	}

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reason += fmt.Sprintf("candidate%d term %d is older than follower %d term:%d\n",
			args.CandidateId, args.Term, rf.me, rf.currentTerm)
	} else {
		if atLeastUptoDate && canVote {
			rf.currentTerm = args.Term
			rf.timer.Reset(rf.electionTimeout)
			DPrintf("%d reset timer.Reason:vote for %d", rf.me, args.CandidateId)
			DPrintf("%d votedfor %d --> %d\n", rf.me, rf.votedFor, args.CandidateId)
			rf.leaderBackToFollower(fmt.Sprintf("vote for %d", args.CandidateId), true)
			rf.votedFor = args.CandidateId
			rf.persist()
			reply.VoteGranted = true
		}
	}

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

func (a *AppendEntryArgs) String() string {
	return fmt.Sprintf("term:%d LeaderId:%d PrevLogIndex:%d PreLogTerm:%d LeaderCommit:%d Entries:%v",
		a.Term, a.LeaderId, a.PrevLogIndex, a.PrevLogTerm, a.LeaderCommit, a.Entries)
}

type AppendEntryReply struct {
	Term    int
	Success bool
	ConflictEntryTerm int//the term of the conflicting entry
	ConflictTermFirstIdx int// the first index it stores for that term
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = true

	if args.Term > rf.currentTerm { // Rules for all servers
		rf.currentTerm = args.Term
		rf.leaderBackToFollower("Got high term AE", false)
		rf.persist()
		DPrintf("%d back to follower cause of leader%d leader's term:%d \n", rf.me, args.LeaderId, args.Term)
	}

	if args.Term < rf.currentTerm {
		reply.Success = false
		DPrintf("AE from %d to %d failed.Reason: args.Term is low", args.LeaderId, rf.me)
		return
	}

	//rf.timer.Reset(rf.electionTimeout) // 从current leader收到AE

	if len(args.Entries) == 0 {
		//DPrintf("%d got heartbeat from leader%d\n", rf.me, args.LeaderId)
	} else {
		//DPrintf("%d got AE RPC, reset timer.\n", rf.me)
	}

	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.timer.Reset(rf.electionTimeout)
		t := ""
		if len(args.Entries) == 0 {
			t = "HB"
		} else {
			t = "AE"
		}
		DPrintf("%d reset timer in %s from %d", rf.me, t, args.LeaderId)
		reply.Success = false
		DPrintf("AE from %d to %d failed.Reason: log doesn't contain any entry at prevLogIndex whose term matches prevLogTerm. Arg is %v rf.log is %v", args.LeaderId, rf.me, args, rf.log)
		return
	}
	if len(args.Entries) != 0 {
		DPrintf("%d receive AE from leader%d, entries are:%v", rf.me, args.LeaderId, args.Entries)
	}

	if len(args.Entries) != 0 {
		DPrintf("%d original entries are :%v", rf.me, rf.log)
	}
	for i := 0; i < len(rf.log); i++ {
		//DPrintf("%d original entry at idx:%d command:%v term:%d==", rf.me, rf.log[i].CommandIndex, rf.log[i].Command, rf.log[i].Term)
	}

	for i := 0; i < len(args.Entries) && args.Entries[i].CommandIndex <= rf.log[len(rf.log)-1].CommandIndex; i++ {
		if rf.log[args.Entries[i].CommandIndex].Term != args.Entries[i].Term {
			rf.log = rf.log[:args.Entries[i].CommandIndex]
			rf.persist()
			DPrintf("%d drop command idx:%d and following", rf.me,args.Entries[i].CommandIndex)
			DPrintf("%d log now are :%v", rf.me, rf.log)
			break
		}
	}

	for i := 0; i < len(args.Entries); i++ {
		if args.Entries[i].CommandIndex >= len(rf.log) {
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			DPrintf("%d append args.Entries from idx %d", rf.me, i)
			break
		}
	}

	//rf.log = append(rf.log, args.Entries...)
	if len(args.Entries) != 0 {
		DPrintf("%d now entries are:%v", rf.me, rf.log)
	}

	if args.LeaderCommit > rf.commitIndex {
		idxOfLastNewEntry := args.PrevLogIndex+len(args.Entries)
		if args.LeaderCommit < idxOfLastNewEntry {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = idxOfLastNewEntry
		}
		rf.applyCond.Signal()
	}
	if len(args.Entries) != 0 {
		DPrintf("%d after append, commitIdx = %d", rf.me, rf.commitIndex)
	}
	rf.timer.Reset(rf.electionTimeout)
	t := ""
	if len(args.Entries) == 0 {
		t = "HB"
	} else {
		t = "AE"
	}
	DPrintf("%d reset timer in %s from %d", rf.me, t, args.LeaderId)

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

func (rf *Raft) Start(command interface{}) (index int, term int, ok bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index = len(rf.log)
	term = rf.currentTerm

	if rf.state != Leader {
		return
	}

	if _, k := command.(int); k {
		DPrintf("Start--%d at %d", command, rf.me)
	}

	ok = true
	cmdIdx := len(rf.log)
	rf.log = append(rf.log, LogEntry{command, rf.currentTerm, cmdIdx})
	rf.persist()
	DPrintf("%d leader entry are: %v", rf.me, rf.log)
	rf.aeCond.Broadcast()

/*
		maj := 0
		majCond := sync.NewCond(&rf.mu)

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(idx int) {
				defer func() {
					DPrintf("Start %v at leader%d AE to %d returned", command, rf.me, idx)
				}()
				//rf.mu.Lock()
				//if len(rf.log)-1 < rf.nextIndex[idx] {
				//	rf.mu.Unlock()
				//	return
				//}
				//rf.mu.Unlock()
				for {
					rf.mu.Lock()
					if rf.state != Leader || rf.nextIndex[idx] == 0 {
						rf.mu.Unlock()
						return
					}

					args := &AppendEntryArgs{
						Term:         curTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.nextIndex[idx] - 1,
						PrevLogTerm:  rf.log[rf.nextIndex[idx]-1].Term,
						Entries:      rf.log[rf.nextIndex[idx]:],
						LeaderCommit: rf.commitIndex,
					}

					DPrintf("leader%d send AE in Start %d to %d", rf.me, command, idx)
					rf.mu.Unlock()

					c := make(chan bool)

					reply := &AppendEntryReply{}

					go func() {
						rpcTimeout := rf.sendAppendEntries(idx, args, reply)
						c <- rpcTimeout
						DPrintf("%d send %v for %d in channel", rf.me, rpcTimeout, idx)
					}()

					go func() {
						time.Sleep(heartBeatInterval)
						c <- false
						DPrintf("%d send false for %d in channel", rf.me, idx)
					}()

					if k := <-c; k == false {
						// timeout so retry
						rf.mu.Lock()
						DPrintf("leader%d send AE in Start %d to %d timeout arg:%v.Cont...", rf.me, command, idx, args)
						rf.mu.Unlock()
						continue
					} else {
						rf.mu.Lock()
						DPrintf("leader%d send AE in Start %d to %d NOT timeout arg:%v", rf.me, command, idx, args)

						if curTerm != rf.currentTerm {
							DPrintf("leader%d term changed after AE %d  RPC, abort\n", rf.me, idx)
							ok = false
							majCond.Signal()
							//wg.Done()
							rf.mu.Unlock()
							return
						}

						if reply.Term > rf.currentTerm {
							DPrintf("%d back to follower cause receive reply from %d with Term %d but current term is %d\n",
								rf.me, idx, reply.Term, rf.currentTerm)
							rf.currentTerm = reply.Term
							if rf.state == Candidate {
								rf.state = Follower
								DPrintf("%d send false to winCh\n", rf.me)
								rf.winElectionCh <- false
							}
							rf.leaderBackToFollower(fmt.Sprintf("In start %v AE from %d to %d,reply has high term",
								command, rf.me, idx), false)
							ok = false
							majCond.Signal()

							rf.mu.Unlock()
							//wg.Done()
							return
						}
						rf.mu.Unlock()

						if reply.Success {
							rf.mu.Lock()
							//wg.Done()
							maj++
							majCond.Signal()
							DPrintf("after AE from %d to %d succ, nextIdx[%d] inc from %d to %d",
								rf.me, idx, idx, rf.nextIndex[idx], args.PrevLogIndex + len(args.Entries) + 1)

							rf.nextIndex[idx] = args.PrevLogIndex + len(args.Entries) + 1
							rf.matchIndex[idx] = args.PrevLogIndex + len(args.Entries)
							rf.mu.Unlock()

							DPrintf("%d AE to %d succ\n", rf.me, idx)
							return
						} else {
							rf.mu.Lock()
							rf.nextIndex[idx]--
							DPrintf("In Start %v leader%d to %d nextIndex dec to %d",command, rf.me, idx, rf.nextIndex[idx])
							rf.mu.Unlock()
							continue
						}
					}
				}

			}(i)
		}

		//wg.Wait()

		rf.mu.Lock()
		for ok && maj < len(rf.peers)/2 {
			DPrintf("%d cont waiting cause :%v maj:%d", rf.me, ok, maj)
			majCond.Wait()
		}

		if ok {
			DPrintf("%d main routine go on, replicated on majority %d of %d", rf.me, maj+1, len(rf.peers))
		} else {
			DPrintf("%d main routine meet bad thing", rf.me)
		}

		if ok && rf.state == Leader {
			for i := len(rf.log) - 1; i > rf.commitIndex; i-- {
				if rf.log[i].Term != rf.currentTerm {
					DPrintf("leader%d logterm at idx%d :%d currentTerm:%d so quit", rf.me, i, rf.log[i].Term, rf.currentTerm)
					break
				}
				maj := 0

				for j := 0; j < len(rf.peers); j++ {
					if rf.matchIndex[j] >= i {
						maj++
					}
				}

				if maj >= len(rf.peers)/2 {
					rf.commitIndex = i
					DPrintf("leader%d commitIndex advance to %d\n", rf.me, i)
					break
				}
			}
		}

		rf.mu.Unlock()*/

	return
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
	rf.electionTimeout = (time.Duration(rf.randDevice.Int63()%8*40 + 300)) * time.Millisecond
}

func sendRVRPC(rf *Raft, args *RequestVoteArgs) {
	for {
		rf.mu.Lock()

		DPrintf("%d wait for next round of RV RPC\n", rf.me)
		for rf.shouldCont == 0 { // 0: wait
			rf.contCond.Wait()
		}
		rf.votes = 1
		rf.votedFor = rf.me
		rf.persist()
		DPrintf("%d begin send RV\n", rf.me)
		rf.shouldCont = 0

		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		args.LastLogIndex = rf.log[len(rf.log)-1].CommandIndex
		args.LastLogTerm = rf.log[len(rf.log)-1].Term

		if rf.state == Candidate {
			DPrintf("%d begin new round of RV RPC with term %d args: %v\n", rf.me, rf.currentTerm, args)
			lastCurTerm := rf.currentTerm
			rf.mu.Unlock()
			wg := &sync.WaitGroup{}

			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					wg.Add(1)
					go func(idx int) {
						reply := &RequestVoteReply{}
						DPrintf("%d send RV to %d\n", rf.me, idx)

						c := make(chan bool)

						go func() {
							ok := rf.sendRequestVote(idx, args, reply)
							c <- ok
						}()

						go func() {
							time.Sleep(rf.electionTimeout)
							c <- false
						}()

						if ok := <-c; !ok {
							wg.Done()
							DPrintf("%d to %d rv RPC timeout\n", rf.me, idx)
							return
						}

						rf.mu.Lock()
						if rf.currentTerm != lastCurTerm {
							rf.mu.Unlock()
							wg.Done()
							DPrintf("%d currentTerm changed since RV %d\n", rf.me, idx)
							return
						}

						if reply.Term > rf.currentTerm { // Rules for all servers
							DPrintf("%d back to follower cause receive reply from %d with Term %d but current term is %d\n",
								rf.me, idx, reply.Term, rf.currentTerm)
							rf.currentTerm = reply.Term
							rf.leaderBackToFollower(fmt.Sprintf("RV reply from %d has high term", idx), false)
							rf.persist()
							wg.Done()
							rf.mu.Unlock()

							return
						}
						rf.mu.Unlock()

						if reply.VoteGranted {
							rf.mu.Lock()
							if rf.state == Candidate {
								rf.votes++
								DPrintf("-------server %d got vote from server %d now has %d votes\n", rf.me, idx, rf.votes)
								if rf.votes > len(rf.peers)/2 {
									DPrintf("%d win election\n", rf.me)
									DPrintf("sending true in win channel\n")
									// avoiding buffer channel impact following election
									if rf.state == Candidate {
										rf.state = Leader
										rf.winElectionCh <- true
									}
									rf.becomeLeader()
									rf.mu.Unlock()
									wg.Done()
									DPrintf("%d become leader.QUIT\n", rf.me)
									return
								}
							} else {
								DPrintf("-------server %d state: %v got vote from server %d \nBUT HAVE BECOME LEADER SO IGNORE IT current votes: %d\n",
									rf.me, rf.state, idx, rf.votes)
							}
							rf.mu.Unlock()
						}
						wg.Done()
					}(i)
				}
			}
			wg.Wait()
		} else {
			DPrintf("%d is no longer candidate.QUIT\n", rf.me)
		}
	}
}


func (rf *Raft) leaderBackToFollower(reason string, rst bool) {
	DPrintf("leader %d back to follower", rf.me)
	if rf.state == Candidate {
		rf.state = Follower
		DPrintf("%d send false to winCh\n", rf.me)
		rf.winElectionCh <- false
	}
	rf.state = Follower
	rf.votedFor = -1
	rf.votes = 0
	rf.shouldSendHB = 0
	rf.shouldCont = 0
	if rst {
		rf.timer.Reset(rf.electionTimeout)
		DPrintf("%d reset timer.Reason:%s", rf.me, reason)
	}
	rf.backToFollowerCond.Signal()

}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	DPrintf("follower%d election timeout, become candidate", rf.me)
	rf.state = Candidate
	rf.mu.Unlock()
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	DPrintf("%d is leader now~~", rf.me)
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	DPrintf("init leader%d's nextIndex all to %d", rf.me, len(rf.log))
	rf.shouldCont = 0 // may be useless
	rf.shouldSendHB = 1
	rf.hbCond.Broadcast()
	rf.aeCond.Broadcast()

	rf.timer.Stop()
}

func (rf *Raft) stopTimer() {
	rf.mu.Lock()
	rf.timer.Stop()
	rf.mu.Unlock()
}

//func (rf *Raft)becomeLeader() {
//
//
//
//}

const (
	heartBeatInterval = 100 * time.Millisecond
)

func sendPeriodHeartBeat(rf *Raft) {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		//DPrintf("%d start heartbeat goroutine %d", rf.me, i)
		go func(idx int) {
			//firstTime := true
			for {
				//if !firstTime {
				//	time.Sleep(heartBeatInterval)
				//}
				//firstTime = false

				rf.mu.Lock()
				for rf.state != Leader || rf.shouldSendHB == 0 {
					rf.hbCond.Wait()
				}

				if rf.state == Leader {
					//DPrintf("leader%d heartbeat to %d len is %d previdx is %d", rf.me, idx, len(rf.log), rf.nextIndex[idx]-1)
					args := &AppendEntryArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.nextIndex[idx]-1,
						PrevLogTerm:  rf.log[rf.nextIndex[idx]-1].Term,
						LeaderCommit: rf.commitIndex,
						Entries:      []LogEntry{},
					}

					lastTerm := rf.currentTerm
					rf.mu.Unlock()

					reply := &AppendEntryReply{}
					DPrintf("woke up.leader%d heartbeat to %d begin\n", rf.me, idx)

					c := make(chan bool, 1)
					go func() {
						rpcTimeout := rf.sendAppendEntries(idx, args, reply)
						c <- rpcTimeout
					}()

					go func() {
						time.Sleep(heartBeatInterval)
						c <- false
					}()

					if ok := <-c; !ok {
						//DPrintf("leader%d heartbeat to %d timeout\n", rf.me, idx)
						continue
					}

					//DPrintf("leader%d heartbeat to %d RPC returned\n", rf.me, idx)
					rf.mu.Lock()

					if lastTerm != rf.currentTerm {
						rf.mu.Unlock()
						DPrintf("%d currentTerm changed since RV %d\n", rf.me, idx)
						continue
					}

					if reply.Term > rf.currentTerm {
						//DPrintf("%d back to follower cause heartbeat reply from %d term:%d, but currentTerm: %d\n", rf.me, idx, reply.Term, rf.currentTerm)
						rf.currentTerm = reply.Term
						rf.leaderBackToFollower(fmt.Sprintf("HB Reply from %d has high term", idx), false)
						rf.persist()
						rf.mu.Unlock()
						continue
					}
					// should HB dec nextIndex ??
					// There may be duplicate dec while HB & Start running at the same time,
					// then dec nextIndex lower than len(log) .Incorrect Behavior!
					//if reply.Success == false {
					//	rf.nextIndex[idx]--
					//	DPrintf("in Heartbaeat leader%d to %d nextIndex dec to %d",rf.me, idx, rf.nextIndex[idx])
					//	rf.mu.Unlock()
					//	continue
					//}

					rf.mu.Unlock()
				} else {
					rf.mu.Unlock()
					DPrintf("%d is no longer leader.Hearbeat go routine %d quit \n", rf.me, idx)
					continue
				}
				DPrintf("leader%d heartbeat to %d finished, go sleep", rf.me, idx)
				time.Sleep(heartBeatInterval)
			}
		}(i)
	}
}

func sendAE(rf *Raft) {
	for i := 0; i < len(rf.peers);i++ {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			for {
				rf.mu.Lock()
				for rf.state != Leader || len(rf.log)-1 < rf.nextIndex[idx] {
					rf.aeCond.Wait()
				}
				curTerm := rf.currentTerm

				args := &AppendEntryArgs{
					Term:         curTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[idx] - 1,
					PrevLogTerm:  rf.log[rf.nextIndex[idx]-1].Term,
					Entries:      rf.log[rf.nextIndex[idx]:],
					LeaderCommit: rf.commitIndex,
				}

				DPrintf("leader%d send AE  to %d entries:%v", rf.me, idx, args.Entries)
				rf.mu.Unlock()

				c := make(chan bool)

				reply := &AppendEntryReply{}

				go func() {
					rpcTimeout := rf.sendAppendEntries(idx, args, reply)
					c <- rpcTimeout
					DPrintf("%d send %v for %d in channel", rf.me, rpcTimeout, idx)
				}()

				go func() {
					time.Sleep(heartBeatInterval)
					c <- false
					DPrintf("%d send false for %d in channel", rf.me, idx)
				}()

				if k := <-c; k == false {
					// timeout so retry
					rf.mu.Lock()
					DPrintf("leader%d send AE  to %d timeout arg:%v.Cont...", rf.me, idx, args)
					rf.mu.Unlock()
					continue
				} else {
					rf.mu.Lock()
					DPrintf("leader%d send AE to %d NOT timeout arg:%v", rf.me, idx, args)

					if curTerm != rf.currentTerm {
						DPrintf("leader%d term changed after AE %d  RPC, abort\n", rf.me, idx)
						//majCond.Signal()
						//wg.Done()
						rf.mu.Unlock()
						continue
					}

					if reply.Term > rf.currentTerm {
						DPrintf("%d back to follower cause receive reply from %d with Term %d but current term is %d\n",
							rf.me, idx, reply.Term, rf.currentTerm)
						rf.currentTerm = reply.Term
						rf.leaderBackToFollower(fmt.Sprintf("Args: %v AE from %d to %d,reply has high term",
							args.Entries, rf.me, idx), false)
						rf.persist()
						//majCond.Signal()
						rf.mu.Unlock()
						//wg.Done()
						continue
					}
					rf.mu.Unlock()

					if reply.Success {
						rf.mu.Lock()
						//wg.Done()
						//maj++
						//majCond.Signal()
						DPrintf("after AE from %d to %d succ, nextIdx[%d] inc from %d to %d",
							rf.me, idx, idx, rf.nextIndex[idx], args.PrevLogIndex + len(args.Entries) + 1)

						rf.nextIndex[idx] = args.PrevLogIndex + len(args.Entries) + 1
						rf.matchIndex[idx] = args.PrevLogIndex + len(args.Entries)
						rf.shouldAdvIdxFlag = 1
						rf.advCommitIdxCond.Signal()

						DPrintf("%d AE to %d succ\n", rf.me, idx)

						rf.mu.Unlock()
						continue
					} else {
						rf.mu.Lock()
						rf.nextIndex[idx]--
						DPrintf("leader%d to %d nextIndex dec to %d",rf.me, idx, rf.nextIndex[idx])
						rf.mu.Unlock()
						continue
					}
				}
			}
		}(i)
	}
}

func advanceCommitIdx(rf *Raft) {
	for {
		rf.mu.Lock()
		for rf.state != Leader || rf.shouldAdvIdxFlag == 0 {
			rf.advCommitIdxCond.Wait()
		}
		DPrintf("%d signaled,  try adv cmt idx", rf.me)

		for i := len(rf.log) - 1; i > rf.commitIndex; i-- {
			if rf.log[i].Term != rf.currentTerm {
				DPrintf("leader%d logterm at idx%d :%d currentTerm:%d so quit", rf.me, i, rf.log[i].Term, rf.currentTerm)
				break
			}
			maj := 0

			for j := 0; j < len(rf.peers); j++ {
				if rf.matchIndex[j] >= i {
					maj++
				}
			}
			// Leader's matchIndex won't update, so iff >= len/2 ,then should be treated as committed
			if maj >= len(rf.peers)/2 {
				rf.commitIndex = i
				rf.applyCond.Signal()
				DPrintf("leader%d commitIndex advance to %d\n", rf.me, i)
				break
			}
		}
		rf.shouldAdvIdxFlag = 0
		rf.mu.Unlock()
	}
}

func candidateInit(rf *Raft) {
	rf.currentTerm++
	rf.votes = 1
	rf.votedFor = rf.me
	rf.persist()
	rf.timer.Reset(rf.electionTimeout)
	rf.rvArgs = RequestVoteArgs{rf.currentTerm, rf.me, rf.log[len(rf.log)-1].CommandIndex, rf.log[len(rf.log)-1].Term}
	rf.shouldCont = 1
	rf.contCond.Signal()
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = Follower
	rf.applyCh = applyCh
	rf.internalApplyCh = make(chan ApplyMsg)
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

	rf.shouldCont = 0
	rf.contCond = sync.NewCond(&rf.mu)

	rf.applyCond = sync.NewCond(&rf.mu)
	rf.backToFollowerCond = sync.NewCond(&rf.mu)

	rf.shouldSendHB = 0
	rf.hbCond = sync.NewCond(&rf.mu)

	rf.shouldSendAE = 0
	rf.aeCond = sync.NewCond(&rf.mu)
	rf.advCommitIdxCond = sync.NewCond(&rf.mu)
	rf.shouldAdvIdxFlag = 0

	go sendRVRPC(rf, &rf.rvArgs)
	go sendPeriodHeartBeat(rf)
	go applyLoop(rf)
	go sendAE(rf)
	go advanceCommitIdx(rf)

	DPrintf("%d's timeout: %d\n", rf.me, rf.electionTimeout)

	// Your initialization code here (2A, 2B, 2C).
	go func() {
		rf.timer.Reset(rf.electionTimeout)
		DPrintf("%d reset timer.Reason:Begin init", rf.me)
		for {
			rf.mu.Lock()
			st := rf.state
			rf.mu.Unlock()
			switch st {
			case Follower:
				<-rf.timer.C
				// 超时前没有收到AE 和RV RPC，所以变为candidate
				rf.becomeCandidate()
			case Candidate:
				rf.mu.Lock()
				DPrintf("server %d begin election\n", rf.me)
				candidateInit(rf)
				DPrintf("%d reset timer.Reason:Begin first election", rf.me)
				rf.mu.Unlock()
			CandiLoop:
				for {
					select {
					case <-rf.timer.C:
						DPrintf("%d election time out, cont\n", rf.me)
						rf.mu.Lock()
						candidateInit(rf)
						DPrintf("%d reset timer.Reason:Begin next round  election", rf.me)
						rf.mu.Unlock()
					case res := <-rf.winElectionCh:
						DPrintf("%d got %v from winChannel\n", rf.me, res)
						break CandiLoop
					}
				}
			case Leader:
				rf.mu.Lock()
				for rf.state == Leader {
					rf.backToFollowerCond.Wait()
				}
				DPrintf("%d is no longer leader.Back to follower \n", rf.me)
				rf.mu.Unlock()
			//LdLoop:
			//	for {
			//		rf.mu.Lock()
			//		switch rf.state {
			//		case Leader:
			//			if rf.debugFlag {
			//				DPrintf("%d is leader now ~~", rf.me)
			//			}
			//			rf.mu.Unlock()
			//			time.Sleep(200 * time.Millisecond)
			//		case Follower:
			//			rf.leaderBackToFollower()
			//			rf.mu.Unlock()
			//
			//			break LdLoop
			//		}
			//	}
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}



func applyLoop(rf *Raft) {
	for {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}

		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++

			msg := ApplyMsg{}
			msg.Command = rf.log[rf.lastApplied].Command
			msg.CommandIndex = rf.log[rf.lastApplied].CommandIndex
			msg.CommandValid = true

			rf.applyCh <- msg
			DPrintf("%d apply command %v at idx:%d \n", rf.me, rf.log[rf.lastApplied].Command, msg.CommandIndex)
		}
		rf.mu.Unlock()
	}
}
