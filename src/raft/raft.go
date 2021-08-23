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
	"6.824/consts"
	"6.824/utils"
	"fmt"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// ApplyMsg
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

// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        			sync.Mutex          // Lock to protect shared access to this peer's state
	peers     			[]*labrpc.ClientEnd // RPC end points of all peers
	persister 			*Persister          // Object to hold this peer's persisted state
	me        			int64               // this peer's index into peers[]
	dead      			int32               // set by Kill()

	serverType			int32
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	revAppendEntries 	chan int64
	recRequestVote	 	chan int64
	persistentState  	PersistentState
	volatileState 	 	VolatileState
	leaderState 	 	LeaderState
}

type Log struct {
	Term		int64
	Command		string
}

// PersistentState updated on stable storage before responding to RPCs
type PersistentState struct {
	CurrentTerm 	int64
	VotedFor		int64
	LogEntries		[]Log
}

type VolatileState struct {
	CommitIndex int64
	LastApplied int64
}

// LeaderState reinitialized after election
type LeaderState struct {
	NextIndex		map[int][]int
	MatchIndex      map[int][]int
}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term			int64
	CandidateID 	int64
	LastLogIndex 	int64
	LastLogTerm 	int64
	// Your data here (2A, 2B).
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term		int64
	VoteGranted bool
	// Your data here (2A).
}

type AppendEntriesReq struct {
	Term	 		int64 // leader's term
	LeaderID 		int64 // so follower can redirect clients
	PrevLogIndex	int // index of log entry immediately preceding new ones
	PreLogTerm		int64	// term of PrevLogIndex entry
	Entries 		[]Log // Log Entries to store(empty for heartbeat; may send more than one for efficiency)
	LeaderCommit	int64 // leader's commitIndex
}

type AppendEntriesResp struct {
	Term			int64
	Success			bool
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return int(rf.currentTerm()), rf.isLeader()
}

func (rf *Raft) isLeader() bool {
	return rf.getServerType() == consts.ServerTypeLeader
}

func (rf *Raft) updateTerm(term int64) {
	atomic.StoreInt64(&rf.persistentState.CurrentTerm, term)
}

func (rf *Raft) currentTerm() int64 {
	return atomic.LoadInt64(&rf.persistentState.CurrentTerm)
}

func (rf *Raft) lengthOfLog() int {
	return len(rf.persistentState.LogEntries)
}

func (rf *Raft) latestLog() Log {
	return rf.getNLog(rf.latestLogIndex())
}

func (rf *Raft) latestLogIndex() int {
	return rf.lengthOfLog() - 1
}

func (rf *Raft) getNLog(n int) Log {
	if n > rf.latestLogIndex() {
		panic(fmt.Sprintf("try to get %dth log, but log: %v", n, rf.persistentState.LogEntries))
	}

	return rf.persistentState.LogEntries[n]
}

func (rf *Raft) getMe() int64 {
	return atomic.LoadInt64(&rf.me)
}


func (rf *Raft) selfIncrementCurrentTerm() {
	atomic.AddInt64(&rf.persistentState.CurrentTerm, 1)
}

func (rf *Raft) voteForSelf() {
	rf.storeVotedFor(rf.getMe())
}

func (rf *Raft) changeServerType(serverType int32) {
	atomic.StoreInt32(&rf.serverType, serverType)
}

func (rf *Raft) getServerType() int32 {
	return atomic.LoadInt32(&rf.serverType)
}

func (rf *Raft) storeVotedFor(votedFor int64) {
	atomic.StoreInt64(&rf.persistentState.VotedFor, votedFor)
}

func (rf *Raft) loadVotedFor() int64 {
	return atomic.LoadInt64(&rf.persistentState.VotedFor)
}

func (rf *Raft) commitIndex() int64 {
	return atomic.LoadInt64(&rf.volatileState.CommitIndex)
}

func (rf *Raft) storeCommitIndex(index int64) {
	atomic.StoreInt64(&rf.volatileState.CommitIndex, index)
}

func (rf *Raft) isVoteForSelf() bool {
	return rf.loadVotedFor() == rf.getMe()
}

func (rf *Raft) noVoteFor() bool {
	return rf.loadVotedFor() == consts.DefaultNoCandidate
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

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


func (rf *Raft) recvRequestVote(candidateID int64) {
	rf.recRequestVote <- candidateID
}
// RequestVote
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("[Raft.RequestVote]Raft(%d) start to process requestVote from Raft(%d)", rf.getMe(), args.CandidateID)
	// Your code here (2A, 2B).

	rf.recvRequestVote(args.CandidateID)
	rf.applyServerRuleTwo(args.Term)
	reply.Term = rf.currentTerm()


	// rule 1
	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm() {
		reply.VoteGranted = false
		return
	}

	// rule 2
	//
	if (rf.noVoteFor() || rf.isVoteForSelf()) && rf.isAtLeastUpToDateAsMyLog(args) {
		reply.VoteGranted = true
		rf.storeVotedFor(args.CandidateID)
		DPrintf("[Raft.RequestVote]Raft(%d) votes for Raft(%d)", rf.getMe(), args.CandidateID)
	}
	return
}

// Todo: finished the function to judge whether the logs of candidate is at least up to date to receiver
func (rf *Raft) isAtLeastUpToDateAsMyLog(args *RequestVoteArgs) bool {
	return args.LastLogTerm > rf.latestLog().Term ||
		(args.LastLogTerm == rf.latestLog().Term && args.LastLogIndex >= int64(rf.latestLogIndex()))
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
	return ok && reply.VoteGranted
}

// applyServerRuleTwo
// if RPC request or response contains term T > currentTerm,
// set currentTerm to T, convert to follower
func (rf *Raft) applyServerRuleTwo(term int64) bool {
	if term > rf.currentTerm() {
		rf.updateTerm(term)
		rf.changeServerType(consts.ServerTypeFollower)
		rf.storeVotedFor(consts.DefaultNoCandidate)
		return true
	}
	return false
}

func (rf *Raft) recvAppendEntries(leaderID int64) {
	rf.revAppendEntries <- leaderID
}

func (rf *Raft) AppendEntries(req *AppendEntriesReq, resp *AppendEntriesResp) {

	rf.recvAppendEntries(req.LeaderID)
	rf.applyServerRuleTwo(req.Term)
	resp.Term = rf.currentTerm()

	// rule 1
	// Reply false if term < currentTerm
	if req.Term < rf.currentTerm() {
		resp.Success = false
		return
	}

	// rule 2
	// Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if !rf.isValidIndexAndTerm(req.PrevLogIndex, req.PreLogTerm) {
		resp.Success = false
		return
	}

	//Todo: finish rule 3 and rule 4: read the 5.3 part of the article

	// rule 5
	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if req.LeaderCommit > rf.commitIndex() {
		rf.storeCommitIndex(utils.Min(req.LeaderCommit, int64(rf.lengthOfLog())))
	}

	resp.Success = true
	return
}

func (rf *Raft) isValidIndexAndTerm(index int, term int64) bool {
	return index > rf.latestLogIndex() && rf.getNLog(index).Term == term
}

func (rf *Raft) sendAppendEntries(req *AppendEntriesReq, resp *AppendEntriesResp, server int) bool {
	return rf.peers[server].Call(consts.MethodAppendEntries, req, resp)
}



// Start
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

// Kill
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

func (rf *Raft) heartbeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i != int(rf.getMe()) {
			req := &AppendEntriesReq{
			Term:         rf.currentTerm(),
			LeaderID:     rf.getMe(),
			PrevLogIndex: rf.latestLogIndex(),
			PreLogTerm:   rf.latestLog().Term,
			Entries:      []Log{},
			LeaderCommit: rf.commitIndex(),
			}
			resp := &AppendEntriesResp{}
			result := rf.sendAppendEntries(req, resp, i)

			if rf.applyServerRuleTwo(resp.Term) {
				return
			}

			DPrintf("Heart beat message has sent from %d to %d, result = %v", rf.getMe(), i, result)
		}
	}
}

func (rf *Raft) requestVote() bool {
	vote := 1
	for i := 0; i < len(rf.peers); i++ {
		if i != int(rf.getMe()) {
			DPrintf("[Raft.requestVote]Raft(%d)[term:%d]]ready to send voteRequest to %v", rf.getMe(),rf.currentTerm(), i)
			req := &RequestVoteArgs{
				Term:         rf.currentTerm(),
				CandidateID:  rf.getMe(),
				LastLogIndex: int64(rf.latestLogIndex()),
				LastLogTerm:  rf.latestLog().Term,
			}
			resp := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, req, resp)

			if rf.applyServerRuleTwo(resp.Term) {
				return false
			}

			if ok {
				DPrintf("[Raft.requestVote]Raft(%d)[term:%d]receive vote from %d", rf.getMe(), rf.currentTerm(),i)
				vote += 1
			}
		}
	}

	DPrintf("vote for %d: %d", rf.getMe(), vote)
	return vote > (len(rf.peers) / 2)
}

// startElection Candidates rule 1
func (rf *Raft) startElection() {
	rf.selfIncrementCurrentTerm()
	rf.voteForSelf()
	rf.changeServerType(consts.ServerTypeCandidate)

	// Candidates rule 2
	if success := rf.requestVote(); success {
		rf.changeServerType(consts.ServerTypeLeader)
	}
}


// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	for rf.killed() == false {

		switch rf.getServerType() {

		case consts.ServerTypeLeader:
			time.Sleep(100 * time.Millisecond)
			rf.heartbeat()

		default:
			select {
			// followers rule 2
			case <-time.After(RandTimeMilliseconds(250, 400)):
				rf.startElection()
			case id := <- rf.revAppendEntries:
				DPrintf("[ticker]append entries from leader: %d, raft %d received request", id, rf.getMe())
			case id := <- rf.recRequestVote:
				DPrintf("[ticker]raft(%d) receive RequestVote from %d", rf.getMe(), id)
			}

		}

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
	}
}

// Make
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
	rf.me = int64(me)
	rf.revAppendEntries = make(chan int64)
	rf.recRequestVote = make(chan int64)
	rf.persistentState.VotedFor = consts.DefaultNoCandidate
	rf.persistentState.LogEntries = []Log{{
		Term:    0,
		Command: "",
	}}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
