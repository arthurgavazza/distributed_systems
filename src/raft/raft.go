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
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"
	//	"6.824/labgob"
	"6.824/labrpc"
)

/*

- Servers start as followers
- Randomized timeout makes one of them start election first ->
   -> increments term
   -> votes for itself
   -> request votes
- One server gets elected at first
- Some server may start a new election for the same term that just finished
-
- How to solve this?
- In the ticker function, iterate in millisecond intervals
  until the electionTimeout occurs, if some aapend entries
  is received, exit the loop and do not start an election
- If a server goes down as leader, some other one will start an
  election for a new term. The previous leader should step down
  to a follower again, but how to do this, if the server is isolated?



*/

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

const (
	min = 500
	max = 1000
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	Log                        []LogEntry
	CommitIndex                int
	LastApplied                int
	VotedFor                   int
	CurrentTerm                int
	CurrentTermElectionTimeout int

	NextIndex  []int
	MatchIndex []int

	CurrentElectionTimer      int
	LastAppendEntriesReceived time.Time
	IsLeader                  bool
	IsCandidate               bool
	IsFollower                bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isLeader bool
	// Your code here (2A).
	term = rf.CurrentTerm
	isLeader = rf.IsLeader

	return term, isLeader
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	Term int
	Peer int
}

type AppendEntriesReply struct {
	Term int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println("Received Append Entries from server", args.Peer, args.Term, rf.CurrentTerm, rf.me)
	if rf.CurrentTerm <= args.Term {
		//previousAppendEntriesReceived := rf.LastAppendEntriesReceived
		rf.LastAppendEntriesReceived = time.Now()
		//fmt.Println(rf.LastAppendEntriesReceived, "restart append entries")
		//fmt.Println(previousAppendEntriesReceived, rf.LastAppendEntriesReceived, "TIMES HERE")
		//rf.IsLeader = false
		rf.IsCandidate = false
		rf.IsFollower = true
		rf.IsLeader = false
		rf.VotedFor = -1
		reply.Term = args.Term
	} else {
		reply.Term = rf.CurrentTerm
	}

	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) execHeartBeat() {
	term, _, me, _, _, _ := rf.GetInfo()
	for i, _ := range rf.peers {
		//rf.mu.Lock()
		go func(i int, term int, me int) {
			args := &AppendEntriesArgs{Term: term, Peer: me}
			reply := &AppendEntriesReply{}
			if i != me {
				//fmt.Println("SENDING HEARTBEAT", i)
				rf.sendAppendEntries(i, args, reply)
				if reply.Term > term {
					rf.StepDownToFollower()
				}

			}

		}(i, term, me)
		//rf.mu.Unlock()
	}

}

func (rf *Raft) heartBeat() {
	for rf.killed() == false {

		time.Sleep(time.Duration(150) * time.Millisecond)
		_, _, _, isLeader, _, _ := rf.GetInfo()

		go func() {
			//rf.mu.Lock()
			//defer rf.mu.Unlock()
			if isLeader {
				//fmt.Println(rf.me, rf.IsLeader, rf.CurrentTerm, "I AM LEADER")
				rf.execHeartBeat()
			}
		}()

	}

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
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

func (rf *Raft) StepDownToFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.IsLeader = false
	rf.IsCandidate = false
	rf.IsFollower = true
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//fmt.Println(reply, rf.CurrentTerm, rf.me, "Request received")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	canVote := (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) && args.Term >= rf.CurrentTerm
	//fmt.Println(rf.CurrentTerm, rf.VotedFor, rf.me, "Request received", canVote, args.Term, args.CandidateId)
	if args.Term >= rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
	}

	if canVote {
		reply.VoteGranted = true
		reply.Term = rf.CurrentTerm
		rf.VotedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
	}

	return

	/*var lastLogTerm int
	var lastLogIndex int
	isLogIndexHigher := args.LastLogIndex == lastLogTerm && args.LastLogIndex > lastLogIndex
	isLogTermHigher := args.LastLogTerm > lastLogTerm
	canVote := rf.VotedFor == -1 || rf.VotedFor == args.CandidateId

	if (isLogTermHigher || isLogIndexHigher) && canVote {
		reply.VoteGranted = true
		reply.Term = rf.CurrentTerm
		rf.VotedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
	}
	*/

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
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) TransitionToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.LastAppendEntriesReceived = time.Now()
	rf.IsCandidate = true
	rf.IsFollower = false
	rf.CurrentTerm++    // Increase the current term
	rf.VotedFor = rf.me // Vote for itself
	//fmt.Println(rf.CurrentTerm, rf.VotedFor, "here", rf.CurrentTermElectionTimeout)
	return
}

func (rf *Raft) TransitionToFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.IsCandidate = false
	rf.IsFollower = true
	rf.VotedFor = -1
}

func (rf *Raft) TransitionToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.IsCandidate = false
	rf.IsFollower = false
	rf.IsLeader = true

}

func (rf *Raft) SetRandomTimeout() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.CurrentTermElectionTimeout = rand.Intn(max-min) + min
	return rf.CurrentTermElectionTimeout
}

func (rf *Raft) GetInfo() (term int, peers []*labrpc.ClientEnd, me int, isLeader bool, isFollower bool, isCandidate bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	peers = rf.peers
	me = rf.me
	isLeader = rf.IsLeader
	isCandidate = rf.IsCandidate
	isFollower = rf.IsFollower
	return term, peers, me, isLeader, isFollower, isCandidate
}

func (rf *Raft) StartElection() {
	//fmt.Println(rf.CurrentTerm, rf.VotedFor, "here")
	rf.TransitionToCandidate()
	term, peers, me, _, _, _ := rf.GetInfo()
	targetVotes := (len(rf.peers) + 1) / 2

	votes := 1
	notVotes := 0
	votedForChan := make(chan bool)

	for i, _ := range peers {

		if i != me {

			go func(i int, term int, me int, votedForChan chan bool) {
				args := &RequestVoteArgs{Term: term, CandidateId: me}
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(i, args, reply)
				//fmt.Println("REQUEST", term, i, me, ok, reply, *reply)

				if reply.Term <= term && reply.VoteGranted {
					votedForChan <- true
				}
				if !ok || !reply.VoteGranted {
					votedForChan <- false
				}

				return

			}(i, term, me, votedForChan)

		}

	}

	finished := false

	for !finished {
		for vote := range votedForChan {
			if vote {
				votes++
				//fmt.Println(votes, "HERE ARE MY VOTES")
			} else {
				//fmt.Println(notVotes, "HERE ARE MY NOT VOTES")
				notVotes++
			}

			if votes >= targetVotes {

				rf.TransitionToLeader()
				finished = true
				go rf.execHeartBeat()
				//fmt.Println("TRANSITIONING TO LEADER")

			}

			if notVotes >= targetVotes {
				rf.TransitionToFollower()
				//fmt.Println("TRANSITIONING TO FOLLOWER")
				finished = true
			}

		}

	}

	return

}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rf.CurrentTermElectionTimeout = rand.Intn(max-min) + min

	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//term, _, me, isLeader, isFollower, isCandidate := rf.GetInfo()
		waited := 0
		electionTimedout := true
		currentTimeout := rf.SetRandomTimeout()
		timeout := time.Duration(rf.CurrentTermElectionTimeout) * time.Millisecond
		for waited <= currentTimeout && electionTimedout {
			time.Sleep(time.Duration(1) * time.Millisecond)
			rf.mu.Lock()
			electionTimedout = time.Since(rf.LastAppendEntriesReceived).Milliseconds() > timeout.Milliseconds()
			rf.mu.Unlock()
			waited++
			if waited == currentTimeout && !rf.IsLeader {
				//fmt.Println(rf.LastAppendEntriesReceived, timeout.Milliseconds(), "last append entries received")
				rf.TransitionToFollower()
				go rf.StartElection()
				//fmt.Println("STARTING ELECTION FROM:", me, isCandidate, isFollower, isLeader, waited, term, timeout.Milliseconds())
			}

		}
		//rf.mu.Unlock()

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
	rf := &Raft{VotedFor: -1, IsFollower: true, CurrentTermElectionTimeout: rand.Intn(max-min) + min}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	go func() {
		rf.heartBeat()
	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go func() {
		rf.ticker()
	}()

	return rf
}
