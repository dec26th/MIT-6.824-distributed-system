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
	"6.824/labgob"
	"6.824/utils"
	"bytes"
	"context"
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

	commitChan          chan ApplyMsg
	revAppendEntries 	chan struct{}
	recRequestVote	 	chan struct{}
	persistentState  	PersistentState
	volatileState 	 	VolatileState
	leaderState 	 	LeaderState
}

func (rf *Raft) String() string {
	return fmt.Sprintf("Raft[%d]:{Term: %d, commitIndex: %d, voteFor: %d, latestLogIndex: %d, latestLogTerm: %d, serverType: %d}\n",
		rf.Me(), rf.currentTerm(), rf.commitIndex(), rf.votedFor(), rf.latestLogIndex(), rf.latestLog().Term, rf.serverType)
}

type Log struct {
	Term		int64
	Command		interface{}
}

func (l *Log) String() string {
	return fmt.Sprintf("{Term: %d, Command: %v}", l.Term, l.Command)
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
	NextIndex		map[int]int
	MatchIndex      map[int]int
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

func (r *RequestVoteArgs) String() string {
	return fmt.Sprintf("{Term: %d, CandidateID: %d, LastLogIndex: %d, LastLogTerm: %d}",
		r.Term, r.CandidateID, r.LastLogIndex, r.LastLogTerm)
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

func (a *AppendEntriesReq) String() string {
	return fmt.Sprintf("{Term: %d, LeaderID: %d, PrevLogIndex: %d, PreLogTerm: %d, Entries: %v, LeaderCommit: %d}",
		a.Term, a.LeaderID, a.PrevLogIndex, a.PreLogTerm, a.Entries, a.LeaderCommit)
}

type AppendEntriesResp struct {
	Term			int64
	Success			bool
	FastBackUp      FastBackUp
}

type FastBackUp struct {
	Term    int
	Index   int
	Len     int
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
	go rf.persist()
}

func (rf *Raft) currentTerm() int64 {
	return atomic.LoadInt64(&rf.persistentState.CurrentTerm)
}

func (rf *Raft) lengthOfLog() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.persistentState.LogEntries)
}

func (rf *Raft) latestLog() Log {
	return rf.getNthLog(rf.latestLogIndex())
}

func (rf *Raft) latestLogIndex() int {
	return rf.lengthOfLog() - 1
}

func (rf *Raft) getNLatestLog(n int) []Log {
	rf.mu.Lock()
	if n >= len(rf.persistentState.LogEntries) {
		panic(fmt.Sprintf("try to get %dth log, but log: %v", n, rf.Logs()))
	}
	if n == -1 {
		return []Log{}
	}

	logs := make([]Log, len(rf.persistentState.LogEntries) - n)
	copy(logs, rf.persistentState.LogEntries[n:])
	rf.mu.Unlock()
	return logs
}

func (rf *Raft) getNthLog(n int) Log {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if n >= len(rf.persistentState.LogEntries) {
		return Log{Term: consts.LogNotFound}
	}
	if n == -1 {
		return Log{}
	}

	return rf.persistentState.LogEntries[n]
}

func (rf *Raft) Me() int64 {
	return atomic.LoadInt64(&rf.me)
}


func (rf *Raft) selfIncrementCurrentTerm() {
	//DPrintf("[Raft.selfIncrementCurrentTerm] Raft[%d] term %d self increment", rf.Me(), rf.currentTerm())
	atomic.AddInt64(&rf.persistentState.CurrentTerm, 1)
	go rf.persist()
}

func (rf *Raft) voteForSelf() {
	rf.storeVotedFor(rf.Me())
}

func (rf *Raft) changeServerType(serverType int32) {
	atomic.StoreInt32(&rf.serverType, serverType)
}

func (rf *Raft) getServerType() int32 {
	return atomic.LoadInt32(&rf.serverType)
}

func (rf *Raft) storeVotedFor(votedFor int64) {
	atomic.StoreInt64(&rf.persistentState.VotedFor, votedFor)
	go rf.persist()
}

func (rf *Raft) votedFor() int64 {
	return atomic.LoadInt64(&rf.persistentState.VotedFor)
}

func (rf *Raft) commitIndex() int64 {
	return atomic.LoadInt64(&rf.volatileState.CommitIndex)
}

func (rf *Raft) storeCommitIndex(index int64) {
	atomic.StoreInt64(&rf.volatileState.CommitIndex, index)
}

func (rf *Raft) isVoteForSelf() bool {
	return rf.isVoteFor(rf.Me())
}

func (rf *Raft) noVoteFor() bool {
	return rf.isVoteFor(consts.DefaultNoCandidate)
}

func (rf *Raft) isVoteFor(id int64) bool {
	return rf.votedFor() == id
}

func (rf *Raft) getNthNextIndex(n int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.leaderState.NextIndex[n]
}

func (rf *Raft) storeNthNextIndex(n, nextIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.leaderState.NextIndex[n] = nextIndex
}

func (rf *Raft) storeNthMatchedIndex(n, matchedIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.leaderState.MatchIndex[n] = matchedIndex
}

func (rf *Raft) Logs() []Log {
	rf.mu.Lock()
	logs := make([]Log, len(rf.persistentState.LogEntries))
	copy(logs, rf.persistentState.LogEntries)
	rf.mu.Unlock()

	return logs
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	p := PersistentState{
		CurrentTerm: rf.currentTerm(),
		VotedFor:    rf.votedFor(),
		LogEntries:  rf.Logs(),
	}
	if err := encoder.Encode(p); err != nil {
		panic(fmt.Sprintf("Failed to encode persistentState, err = %s", err))
	}
	rf.persister.SaveRaftState(buffer.Bytes())

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
		rf.mu.Lock()
		rf.persistentState = PersistentState {
		VotedFor: consts.DefaultNoCandidate,
		LogEntries: []Log{{Term: 0, Command: nil}},
		}
		rf.mu.Unlock()
		return
	}
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)

	persitentStaet := new(PersistentState)
	if err := decoder.Decode(persitentStaet); err != nil {
		panic(fmt.Sprintf("Failed to read persist, err = %s", err))
	}
	DPrintf("read persist successfully, %+v", persitentStaet)
	rf.mu.Lock()
	rf.persistentState = *persitentStaet
	rf.mu.Unlock()

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


func (rf *Raft) recvRequestVote() {
	if rf.isServerType(consts.ServerTypeFollower) {
		DPrintf("Raft[%d] receive request vote",rf.Me())
		rf.recRequestVote <- struct{}{}
	}
}
// RequestVote
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("[Raft.RequestVote]%v requestVote from Raft[%d], req = %v", rf, args.CandidateID, args)
	// Your code here (2A, 2B).

	rf.recvRequestVote()
	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm()

	// rule 1
	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm() {
		DPrintf("[Raft.RequestVote]Raft[%d].Term = %d, Raft[%d].Term = %d, refused",
			args.CandidateID, args.Term, rf.Me(), rf.currentTerm())
		return
	}

	// rule 2
	//
	DPrintf("[Raft.RequestVote]Raft[%d] here", rf.Me())
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[Raft.RequestVote]Raft[%d] hello", rf.Me())
	if rf.noVoteFor() || rf.isVoteFor(args.CandidateID) {
		if  rf.isAtLeastUpToDateAsMyLog(args) {
			reply.VoteGranted = true
			rf.storeVotedFor(args.CandidateID)
			DPrintf("[Raft.RequestVote]Raft[%d] votes for Raft[%d]", rf.Me(), args.CandidateID)
			return
		}
	}
	DPrintf("[Raft.RequestVote]Raft[%d] refuse to vote for Raft[%d]", rf.Me(), args.CandidateID)
	return
}

func (rf *Raft) isAtLeastUpToDateAsMyLog(args *RequestVoteArgs) bool {
	lateLogIndex := len(rf.persistentState.LogEntries) - 1
	latestLogTerm := rf.persistentState.LogEntries[lateLogIndex].Term


	//DPrintf("[Raft.isAtLeastUpToDateAsMyLog] Raft[%d] send vote request to Raft[%d][latestLogTerm:%d], lastLogIndex:%d, req: %v,",
	//	args.CandidateID, rf.Me(),latestLogTerm, lateLogIndex, args)
	return args.LastLogTerm > latestLogTerm ||
		(args.LastLogTerm == latestLogTerm && args.LastLogIndex >= int64(lateLogIndex))
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
	//DPrintf("[Raft.sendRequestVote] Raft[%d] send to %d", rf.Me(), server)
	if rf.isServerType(consts.ServerTypeCandidate) {
		ok := rf.peers[server].Call(consts.MethodRequestVote, args, reply)
		DPrintf("[Raft.sendRequestVote]Raft[%d] receives resp from %d, resp = %+v", rf.Me(), server, reply)
		rf.checkTerm(reply.Term)
		return ok && reply.VoteGranted
	}
	return false
}

// checkTerm
// if RPC request or response contains term T > currentTerm,
// set currentTerm to T, convert to follower
func (rf *Raft) checkTerm(term int64) bool {
	if term > rf.currentTerm() {
		DPrintf("[raft.checkTerm]Raft[%d].term = %d Get term: %d, change to follower", rf.Me(), rf.currentTerm(), term)
		rf.updateTerm(term)
		rf.changeServerType(consts.ServerTypeFollower)
		rf.storeVotedFor(consts.DefaultNoCandidate)
		return true
	}
	return false
}

func (rf *Raft) recvAppendEntries() {
	if rf.isServerType(consts.ServerTypeFollower) {
		rf.revAppendEntries <- struct{}{}
	}
}

func (rf *Raft) recvFromLeader(req *AppendEntriesReq) {
	if rf.isServerType(consts.ServerTypeCandidate) {
		if req.Term <= rf.currentTerm() {
			DPrintf("Raft[%d] change from candidate to follower, req = %+v", rf.Me(), req)
			rf.changeServerType(consts.ServerTypeFollower)
		}
	}
}

func (rf *Raft) getFastBackUpInfo(prevLogIndex int) FastBackUp {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lenOfLog := len(rf.persistentState.LogEntries)
	result := FastBackUp{
		Len: lenOfLog,
		Term: consts.IndexOutOfRange,
	}

	if prevLogIndex >= lenOfLog {
		return result
	}

	result.Term = int(rf.persistentState.LogEntries[prevLogIndex].Term)
	for i := 0; i < lenOfLog; i++ {
		if rf.persistentState.LogEntries[i].Term == int64(result.Term) {
			DPrintf("[Raft.getFastBackUpInfo]%v first index of term: %d = %d", rf, result.Term, i)
			result.Index = i
		}
	}
	return result
}

func (rf *Raft) AppendEntries(req *AppendEntriesReq, resp *AppendEntriesResp) {
	DPrintf("[Raft.AppendEntries]%v AppendEntries from Raft[%d], req = %v", rf, req.LeaderID, req)
	rf.recvAppendEntries()

	rf.recvFromLeader(req)
	rf.checkTerm(req.Term)
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
	if !rf.checkConsistency(req.PrevLogIndex, int(req.PreLogTerm)) {
		resp.Success = false
		resp.FastBackUp = rf.getFastBackUpInfo(req.PrevLogIndex)

		DPrintf("[service.AppendEntries]Failed to check consistency, resp = %+v, req = %+v, %v", resp, req, rf)
		return
	}

	DPrintf("[service.AppendEntries] Raft[%d] finish check consistency", rf.Me())
	// rule 3, 4
	index := rf.tryBeAsConsistentAsLeader(req.PrevLogIndex, req.Entries)

	DPrintf("[service.AppendEntries] Raft[%d] finish check index", rf.Me())
	// rule 5
	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if req.LeaderCommit > rf.commitIndex() {
		min := utils.Min(req.LeaderCommit, int64(index))
		DPrintf("[service.AppendEntries] %v, leader commit = %d, change commit index to %d", rf, req.LeaderCommit, min)
		go rf.commit(int(min))
	}

	resp.Success = true
	return
}

func (rf *Raft) checkConsistency(index, term int) bool {
	DPrintf("[Raft.checkConsistency] %v, index = %d, term = %d", rf, index, term)
	return index <= rf.latestLogIndex() && int(rf.getNthLog(index).Term) == term
}

// tryBeAsConsistentAsLeader
// rule3: if an existing entry conflicts with a new one(same index but different terms),
// delete the existing entry and all that follow it
func (rf *Raft) tryBeAsConsistentAsLeader(index int, logs []Log) int {
	DPrintf("[Raft.tryBeAsConsistentAsLeader] %v, remove the part after index: %d, and append %v", rf, index, logs)
	lastIndex := 0
	rf.mu.Lock()
	rf.persistentState.LogEntries = rf.persistentState.LogEntries[:index+1]
	rf.persistentState.LogEntries = append(rf.persistentState.LogEntries, logs...)
	lastIndex = len(rf.persistentState.LogEntries) - 1
	rf.mu.Unlock()
	go rf.persist()
	return lastIndex
}

func (rf *Raft) sendAppendEntries(req *AppendEntriesReq, resp *AppendEntriesResp, server int) bool {
	//DPrintf("[Raft.sendAppendEntries] Send request to %d, %v", server, rf)
	if rf.isLeader() {
		DPrintf("[Raft.sendAppendEntries]Raft[%d] send request to %d",rf.Me(), server)
		ok := rf.peers[server].Call(consts.MethodAppendEntries, req, resp)
		DPrintf("[Raft.sendAppendEntries]Raft[%d] receives resp from %d, ok = %v ready to check term", rf.Me(), server, ok)
		rf.checkTerm(resp.Term)
		return ok
	}
	return false
}



// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise, start the
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

	// Your code here (2B).
	if rf.isLeader() {
		DPrintf("[Raft.Start]Raft[%d] start to replicate command: %v", rf.Me(), command)
		rf.mu.Lock()
		rf.persistentState.LogEntries = append(rf.persistentState.LogEntries, Log{
			Term:    rf.currentTerm(),
			Command: command,
		})
		index = len(rf.persistentState.LogEntries) - 1
		DPrintf("[Raft.Start]Raft[%d] Log:%v",rf.Me(), rf.persistentState.LogEntries)
		rf.mu.Unlock()

		go rf.persist()
		DPrintf("[Raft.Start]%v Receive command %v, index = %d", rf, command, index)
		go rf.processNewCommand(index)
	}

	return index, int(rf.currentTerm()), rf.isLeader()
}

func (rf *Raft) processNewCommand(index int) {
	replicated := make(chan bool)
	for i := 0; i < len(rf.peers); i++ {
		if i != int(rf.Me()) {
			go rf.sendAppendEntries2NServer(i, replicated, index)
		}
	}

	firstTime := true
	num := 1
	for i := 0; i < len(rf.peers) - 1; i++ {
		ok := <-replicated
		if ok {
			num ++
		}
		DPrintf("[Raft.processNewCommand] replicate num: %d, index = %d", num, index)
		// commit if a majority of peers replicate
		if rf.isLeader() && num > len(rf.peers) / 2 {
			if firstTime {
				go rf.commit(index)
				firstTime = false
			}
		}
	}
	close(replicated)
}

func (rf *Raft) commit(index int) {
	start := rf.commitIndex() + 1
	DPrintf("[Raft.commit] %v, from Log[%d]%v to Log[%d]%v",
		rf, start, rf.getNthLog(int(start)), index, rf.getNthLog(index))

	for i := start; i <= int64(index); i++ {
		log := rf.getNthLog(int(i))
		if log.Term == consts.LogNotFound {
			continue
		}

		DPrintf("[Raft.commit] Raft[%d] commit Log[%d]: %v", rf.Me(), i, log)
		if i > rf.commitIndex() {
			rf.storeCommitIndex(i)
			rf.commitChan <- ApplyMsg{
				CommandValid: true,
				Command:      log.Command,
				CommandIndex: int(i),
			}
		}
	}

}

func (rf *Raft) fastBackUp(info FastBackUp) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if info.Term == consts.IndexOutOfRange {
		DPrintf("[Raft.fastBackUp] Index out of range, set nextIndex to info.Len = %d", info.Len)
		return info.Len
	}

	if !rf.isTermExist(int64(info.Term)) {
		DPrintf("[Raft.fastBackUp] Term not in logs: %v, set nextIndex to info.Index = %d", rf.Logs(), info.Index)
		return info.Index
	} else {
		result := rf.lastIndexOfTerm(int64(info.Term)) + 1
		DPrintf("[Raft.fastBackUp] Get the last index of term: %d in logs:%v, set nextIndex to lastIndex of term + 1 = %d", info.Term, rf.Logs(), result)
		return result
	}
}

func (rf *Raft) lastIndexOfTerm(term int64) int {
	for i := len(rf.persistentState.LogEntries) - 1; i >= 0; i-- {
		if rf.persistentState.LogEntries[i].Term == term {
			return i
		}
	}
	return 0
}

func (rf *Raft) isTermExist(term int64) bool {
	for i := 0; i < len(rf.persistentState.LogEntries); i++ {
		if rf.persistentState.LogEntries[i].Term == term {
			return true
		}
		if rf.persistentState.LogEntries[i].Term > term {
			return false
		}
	}
	return false
}

func (rf *Raft) sendAppendEntries2NServer(n int, replicated chan<- bool, index int) {
	var lenAppend int

	nextIndex := rf.getNthNextIndex(n)
	DPrintf("[Raft.sendAppendEntries2NServer] NextIndex = %d latestLogIndex = %d, ready to replicate on Raft[%d]", nextIndex, rf.latestLogIndex(), n)
	// leaders rule3
	if  rf.latestLogIndex() >= rf.getNthNextIndex(n) {
		var finished bool

		// index of log entry immediately preceding new ones， 紧接着新append进来的Log的索引
		for !finished && nextIndex >= 0 && rf.isLeader() && rf.latestLogIndex() >= rf.getNthNextIndex(n) {
			ok := false

			for !ok && rf.isLeader() && rf.latestLogIndex() >= rf.getNthNextIndex(n) {
				time.Sleep(time.Millisecond * 10)

				entries := rf.getNLatestLog(nextIndex)
				DPrintf("[Raft.sendAppendEntries2NServer] Logs replicated on Raft[%d] from index = %d are %v", n, nextIndex, entries)
				lenAppend = len(entries)
				req := &AppendEntriesReq{
					Term:         rf.currentTerm(),
					LeaderID:     rf.Me(),
					PrevLogIndex: nextIndex - 1,
					PreLogTerm:   rf.getNthLog(nextIndex - 1).Term,
					Entries:      entries,
					LeaderCommit: rf.commitIndex(),
				}

				DPrintf("[Raft.sendAppendEntries2NServer] Replicate on Raft[%d], pre log index = %d, pre log term = %d", n, nextIndex, rf.getNthLog(nextIndex).Term)
				resp := new(AppendEntriesResp)
				ok = rf.sendAppendEntries(req, resp, n)
				DPrintf("[Raft.sendAppendEntries2NServer] Resp from Raft[%d], resp = %+v", n, resp)
				finished = resp.Success && ok

				if rf.isLeader() && ok && !resp.Success {
					DPrintf("[Raft.sendAppendEntries2NServer] Follower is inconsistent, get ready to fast backup: %v", resp.FastBackUp)
					nextIndex = rf.fastBackUp(resp.FastBackUp)
				}
			}
		}

		if rf.isLeader() && finished {
			rf.mu.Lock()
			next := nextIndex + lenAppend
			raw := rf.leaderState.NextIndex[n]
			if next > raw {
				raw = next
			}
			rf.leaderState.NextIndex[n] = raw
			rf.leaderState.MatchIndex[n] = raw - 1
			rf.mu.Unlock()
			replicated <- true
			DPrintf("[Raft.sendAppendEntries2NServer]Raft[%d] matchIndex now = %d successfully, and index = %d", n, raw - 1, index)
			return
		}
	}
	replicated <- false
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

func (rf *Raft) isServerType(serverType int32) bool {
	return rf.getServerType() == serverType
}

func (rf *Raft) heartbeat() {
	for i := 0; i < len(rf.peers); i++ {

		if i != int(rf.Me()) && rf.isLeader() {
			go rf.sendHeartBeat2NServer(i)
		}
	}
}

func (rf *Raft)sendHeartBeat2NServer(i int) {
	req := &AppendEntriesReq{
		Term:         rf.currentTerm(),
		LeaderID:     rf.Me(),
		PrevLogIndex: rf.latestLogIndex(),
		PreLogTerm:   rf.latestLog().Term,
		Entries:      nil,
		LeaderCommit: rf.commitIndex(),
	}
	resp := &AppendEntriesResp{}
	rf.sendAppendEntries(req, resp, i)
	DPrintf("Raft[%d] send heartbeat to %d", rf.Me(), i)
}

func (rf *Raft) requestVote(ctx context.Context, voteChan chan<- bool) {
	finish := make(chan bool)
	defer close(finish)

	vote := int64(1)
	no := int64(0)

	for i := 0; i < len(rf.peers); i++ {
		if i != int(rf.Me()) {
			go func(i int) {
				var getVote bool
				if !rf.isServerType(consts.ServerTypeCandidate) {
					finish <- getVote
					return
				}

				req := &RequestVoteArgs{
					Term:         rf.currentTerm(),
					CandidateID:  rf.Me(),
					LastLogIndex: int64(rf.latestLogIndex()),
					LastLogTerm:  rf.latestLog().Term,
				}
				resp := &RequestVoteReply{}
				ok := rf.sendRequestVote(i, req, resp)

				if ok {
					getVote = true
				}
				finish <- getVote
			}(i)
		}
	}

	for i := 0; i < len(rf.peers) - 1; i++ {
		select {
		case <- ctx.Done():
			DPrintf("[Raft.requestVote] Raft[%d] time out", rf.Me())

			for j := i; j < len(rf.peers) - 1; j++ {
				<- finish
			}
			return

		case v := <- finish:
			//DPrintf("[Raft.requestVote] finished, vote: %v", v)
			if v {
				atomic.AddInt64(&vote, 1)
			} else {
				atomic.AddInt64(&no, 1)
			}

			if atomic.LoadInt64(&vote) > int64(len(rf.peers) / 2) || atomic.LoadInt64(&no) > int64(len(rf.peers) / 2) {
				voteChan <- atomic.LoadInt64(&vote) > int64(len(rf.peers) / 2)

				// receive the left finish
				for j := i + 1; j < len(rf.peers) - 1; j++ {
					<- finish
				}
				return
			}
		}
	}
}

// startElection
// 1. increment currentTerm
// 2. vote for self
// 3. Reset election timer
// 4. SendRequestVote RPCs to all others servers
func (rf *Raft) startElection(ctx context.Context, electionResult chan<- bool, cancel context.CancelFunc) {
	defer cancel()
	if !rf.isServerType(consts.ServerTypeCandidate) {
		electionResult <- false
		return
	}

	rf.selfIncrementCurrentTerm()
	rf.voteForSelf()
	voteChan := make(chan bool)
	defer close(voteChan)

	go rf.requestVote(ctx, voteChan)

	select {
	case success := <-voteChan:
		if success && rf.isServerType(consts.ServerTypeCandidate) {
			rf.changeServerType(consts.ServerTypeLeader)
			rf.initLeaderState()
			DPrintf("[Raft.startElection] Raft[%d] has become leader", rf.Me())
		}
		electionResult <- success
		return

	case <-ctx.Done():
		DPrintf("[Raft.startElection] Raft[%d] time out", rf.Me())
		<- voteChan
		return
	}
}

func (rf *Raft) initLeaderState() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.leaderState = LeaderState{
		NextIndex: make(map[int]int, len(rf.peers) - 1),
		MatchIndex: make(map[int]int, len(rf.peers) - 1),
	}

	for i := 0; i < len(rf.peers); i++ {
		if int(rf.Me()) != i {
			rf.leaderState.NextIndex[i] = len(rf.persistentState.LogEntries)
			rf.leaderState.MatchIndex[i] = len(rf.persistentState.LogEntries) - 1
		}
	}
}

func (rf *Raft) randomTimeout() time.Duration {
	return RandTimeMilliseconds(300, 600)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	var (
		ctx         context.Context
		cancel      context.CancelFunc
	)
	electionResult := make(chan bool)

	for rf.killed() == false {
		timeout := rf.randomTimeout()
		switch rf.getServerType() {

		case consts.ServerTypeLeader:
			time.Sleep(10 * time.Millisecond)
			if !rf.isLeader() {
				continue
			}
			DPrintf("Ready to heartbeat")
			rf.heartbeat()
			time.Sleep(140 * time.Millisecond)

		case consts.ServerTypeCandidate:
			ctx, cancel = context.WithTimeout(context.Background(), timeout)
			now := time.Now()
			if !rf.isServerType(consts.ServerTypeCandidate) {
				cancel()
				continue
			}

			go rf.startElection(ctx, electionResult, cancel)
			select {
				case <- time.After(timeout):

				case result := <- electionResult:
					if !result {
						time.Sleep(timeout - time.Since(now))
					}
			}

		case consts.ServerTypeFollower:
			//DPrintf("[Raft.ticker] Raft[%d] timeout: %v, timeNow: %v",rf.Me(), timeout, time.Now())
			select {
			// followers rule 2
			case <-time.After(timeout):
				DPrintf("[Raft.ticker] Raft[%d] change to candidate.",  rf.Me())
				rf.changeServerType(consts.ServerTypeCandidate)
			case <- rf.revAppendEntries:

			case <- rf.recRequestVote:

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

	rf := &Raft{
		peers: peers,
		persister: persister,
		me: int64(me),
		revAppendEntries: make(chan struct{}),
		recRequestVote: make(chan struct{}),
		serverType: consts.ServerTypeFollower,
		commitChan: applyCh,
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
