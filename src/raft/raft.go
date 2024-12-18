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
	"labs/src/labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// Log Entry
type LogEntry struct {
}

// Enum
type RaftState string

const (
	Leader    RaftState = "Leader"
	Candidate           = "Candidate"
	Follower            = "Follower"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 2A
	currentTerm int
	votedFor    int
	// logs            []LogEntry
	state           RaftState
	heartBeat       time.Duration
	electionTimeOut time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("RequestVote RPC called from %d to %d with term %d\n", args.LeaderId, rf.me, args.Term)
	term := args.Term
	reply.Term = rf.currentTerm
	reply.Success = true
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.state = Follower
		rf.votedFor = -1
		rf.resetElectionTimeOut()
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("RequestVote RPC called from %d to %d with term %d\n", args.CandidateId, rf.me, args.Term)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = args.CandidateId
		rf.resetElectionTimeOut()

		// reply
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}

	// Raft does not allow for voting more than once in one term
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		rf.resetElectionTimeOut()
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	}

	reply.VoteGranted = false
}

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

// !! Shouldn't hold lock before RPC call
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// 2A
	rf.state = Follower
	rf.heartBeat = 50 * time.Millisecond
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.resetElectionTimeOut()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.tick()

	return rf
}

func (rf *Raft) resetElectionTimeOut() {
	t := time.Now()
	timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	rf.electionTimeOut = t.Add(timeout)
}

// lock already acquired
func (rf *Raft) initElectionStates() {

	// increment current term
	rf.currentTerm += 1
	// transistion to Candidate State
	rf.state = Candidate
	// vote for itself
	rf.votedFor = rf.me
	// randomize election timeout time
	rf.resetElectionTimeOut()
	// persist
	// rf.persist()
}

func (rf *Raft) candidateRequestVote(serverId int, args *RequestVoteArgs, voterCounts *int, electedAsLeader *sync.Once) {

	// rf.initElectionStates()
	DPrintf("Leader %d sending RequestVote RPC to %d at Term%d\n", rf.me, serverId, args.Term)
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(serverId, args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > args.Term {
		DPrintf("Leader already elected in [%d], update %d current term status\n", serverId, rf.me)
		// Leader existed
		rf.votedFor = -1
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.resetElectionTimeOut()
		return
	}
	if reply.Term < args.Term {
		DPrintf("Vote reply from %d is out-of-date in %d\n", serverId, rf.me)
		return
	}
	if !reply.VoteGranted {
		DPrintf("Follower %d hasn't voted for %d\n", serverId, rf.me)
		return
	}
	*voterCounts++
	DPrintf("Follower %d voted for %d, it now has %d votes\n", serverId, rf.me, *voterCounts)

	if *voterCounts > len(rf.peers)/2 && rf.currentTerm == args.Term && rf.state == Candidate {
		// become new leader
		electedAsLeader.Do(func() {
			rf.state = Leader
			DPrintf("Candidate %d has won the election, election ended for Term %d\n", rf.me, args.Term)
		})
	}
}

func (rf *Raft) StartElection() {
	rf.initElectionStates()
	voterCounts := 1
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	var electedAsLeader sync.Once
	for idx := range rf.peers {
		if idx != rf.me {
			go rf.candidateRequestVote(idx, &args, &voterCounts, &electedAsLeader)
		}
	}
}

func (rf *Raft) sendHeartbeats() {
	for idx := range rf.peers {
		if idx != rf.me {
			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			go func(server int, args AppendEntriesArgs) {
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, &args, &reply)
				rf.mu.Lock()
				if ok && reply.Term > rf.currentTerm {

					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.resetElectionTimeOut()
				}
				rf.mu.Unlock()
			}(idx, args)
		}
	}
}

func (rf *Raft) tick() {
	for !rf.killed() {
		time.Sleep(rf.heartBeat)
		rf.mu.Lock()
		// if current state if Leader, send heartbeat signal
		if rf.state == Leader {
			rf.mu.Unlock()
			rf.sendHeartbeats()
		} else if time.Now().After(rf.electionTimeOut) {
			rf.mu.Unlock()
			rf.StartElection()
		} else {
			rf.mu.Unlock()
		}

	}
}
