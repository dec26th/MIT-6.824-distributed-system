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
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/consts"
	"6.824/labgob"
	"6.824/utils"

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
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int64               // this peer's index into peers[]
	dead      int32               // set by Kill()

	serverType       int32
	lastAppliedIndex int64
	lastAppliedTerm  int64
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	cmu sync.Mutex // commit lock

	commitChan       chan ApplyMsg
	revAppendEntries chan struct{}
	recRequestVote   chan struct{}
	persistentState  PersistentState
	volatileState    VolatileState
	leaderState      LeaderState
}

type Log struct {
	Term    int64
	Command interface{}
}

type StateToPersist struct {
	PersistentState
	LastAppliedIndex int64
	LastAppliedTerm  int64
}

// PersistentState updated on stable storage before responding to RPCs
type PersistentState struct {
	CurrentTerm int64
	VotedFor    int64
	LogEntries  []Log
}

type VolatileState struct {
	CommitIndex int64
	LastApplied int64
}

// LeaderState reinitialized after election
type LeaderState struct {
	NextIndex  map[int]int
	MatchIndex map[int]int
}

func (l *Log) String() string {
	return fmt.Sprintf("{Term: %d, Command: %+v}", l.Term, l.Command)
}

func (rf *Raft) String() string {
	return fmt.Sprintf("Raft[%d]:{Term: %d, commitIndex: %d, voteFor: %d, relativeLatestLogIndex: %d, serverType: %v, lastAppliedIndex: %d, lastAppliedTerm: %d}\n",
		rf.Me(), rf.CurrentTerm(), rf.commitIndex(), rf.votedFor(), rf.relativeLatestLogIndex(), rf.getServerType(), rf.LastAppliedIndex(), rf.LastAppliedTerm())
}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int64
	CandidateID  int64
	LastLogIndex int64
	LastLogTerm  int64
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
	Term        int64
	VoteGranted bool
	// Your data here (2A).
}

type AppendEntriesReq struct {
	Term         int64 // leader's term
	LeaderID     int64 // so follower can redirect clients
	PrevLogIndex int   // index of log entry immediately preceding new ones
	PreLogTerm   int64 // term of PrevLogIndex entry
	Entries      []Log // Log Entries to store(empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int64 // leader's commitIndex
}

func (a *AppendEntriesReq) String() string {
	return fmt.Sprintf("{Term: %d, LeaderID: %d, PrevLogIndex: %d, PreLogTerm: %d, Entries: %+v, LeaderCommit: %d}",
		a.Term, a.LeaderID, a.PrevLogIndex, a.PreLogTerm, a.Entries, a.LeaderCommit)
}

type AppendEntriesResp struct {
	Term       int64
	Success    bool
	FastBackUp FastBackUp
}

type FastBackUp struct {
	Term  int
	Index int
	Len   int
}

func (f *FastBackUp) String() string {
	return fmt.Sprintf("{Term: %d, Index: %d, Len: %d}", f.Term, f.Index, f.Len)
}

type Snapshot struct {
	LastAppliedIndex int
	LastAppliedTerm  int64
	Command          interface{}
}

// GetState return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return int(rf.CurrentTerm()), rf.isLeader()
}

func (rf *Raft) isLeader() bool {
	return rf.isServerType(consts.ServerTypeLeader)
}

func (rf *Raft) updateTerm(term int64) {
	atomic.StoreInt64(&rf.persistentState.CurrentTerm, term)
	go rf.persist()
}

func (rf *Raft) CurrentTerm() int64 {
	return atomic.LoadInt64(&rf.persistentState.CurrentTerm)
}

func (rf *Raft) lengthOfLog() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.persistentState.LogEntries)
}

func (rf *Raft) latestLog() Log {
	return rf.getNthLog(rf.absoluteLatestLogIndex())
}

func (rf *Raft) relativeLatestLogIndex() int {
	return rf.lengthOfLog() - 1
}

func (rf *Raft) absoluteLatestLogIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("[Raft.absoluteLatestLogIndex] Raft[%d], lastAppliedIndex: %d", rf.Me(), rf.LastAppliedIndex())
	return rf.absoluteIndex(int64(len(rf.persistentState.LogEntries) - 1))
}

func (rf *Raft) getLogsFromTo(from, to int) []Log {
	if from > to {
		panic(fmt.Sprintf("[Raft.getLogsFromTo] Failed to get logs from %d to %d", from, to))
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	to = rf.relativeIndex(int64(to))
	from = rf.relativeIndex(int64(from))
	if to >= len(rf.persistentState.LogEntries) {
		//DPrintf("[Raft.getLogsFromTo] Failed to get logs to %d, len of logs: %d", to, len(rf.persistentState.LogEntries))
		to = len(rf.persistentState.LogEntries) - 1
	}
	if from < 0 || to < from {
		DPrintf("[Raft.getLogsFromTo] Failed to get logs from %d to %d", from, to)
		return []Log{}
	}

	logs := make([]Log, to-from+1)
	copy(logs, rf.persistentState.LogEntries[from:to+1])
	return logs
}

func (rf *Raft) getNthLog(n int) Log {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	n = rf.relativeIndex(int64(n))
	if n >= len(rf.persistentState.LogEntries) || n < 0 {
		// DPrintf("[Raft.getNthLog]Raft[%d] ready to get relative index %d of Log, Logs: %+v", rf.Me(), n, rf.persistentState.LogEntries)
		return Log{Term: consts.LogNotFound}
	}

	return rf.persistentState.LogEntries[n]
}

func (rf *Raft) Me() int64 {
	return atomic.LoadInt64(&rf.me)
}

func (rf *Raft) selfIncrementCurrentTerm() {
	////DPrintf("[Raft.selfIncrementCurrentTerm] Raft[%d] term %d self increment", rf.Me(), rf.CurrentTerm())
	atomic.AddInt64(&rf.persistentState.CurrentTerm, 1)
	go rf.persist()
}

func (rf *Raft) voteForSelf() {
	rf.storeVotedFor(rf.Me())
}

func (rf *Raft) changeServerType(serverType consts.ServerType) {
	atomic.StoreInt32(&rf.serverType, int32(serverType))
}

func (rf *Raft) getServerType() consts.ServerType {
	return consts.ServerType(atomic.LoadInt32(&rf.serverType))
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
	if index > rf.commitIndex() {
		atomic.StoreInt64(&rf.volatileState.CommitIndex, index)
	}
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

func (rf *Raft) getNthMatchIndex(n int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.leaderState.MatchIndex[n]
}

func (rf *Raft) storeNthNextIndex(n, nextIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.leaderState.NextIndex[n] = nextIndex
	DPrintf("[Raft.storeNthNextIndex]LeaderRaft[%d] Change Raft[%d]'s next index change to %d", rf.Me(), n, nextIndex)
}

func (rf *Raft) storeNthMatchedIndex(n, matchedIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.leaderState.MatchIndex[n] = matchedIndex
}

func (rf *Raft) Logs() []Log {
	logs := make([]Log, len(rf.persistentState.LogEntries))
	copy(logs, rf.persistentState.LogEntries)

	return logs
}

func (rf *Raft) relativeIndex(absoluteIndex int64) int {
	relativeIndex := absoluteIndex - rf.LastAppliedIndex()
	// DPrintf("[Raft.relativeIndex]Raft[%d] AbsoluteIndex: %d, lastAppliedIndex: %d, relativeIndex: %d", rf.Me(), absoluteIndex, rf.lastAppliedIndex, relativeIndex)
	return int(relativeIndex)
}

func (rf *Raft) absoluteIndex(relativeIndex int64) int {
	absoluteIndex := relativeIndex + rf.LastAppliedIndex()
	// DPrintf("[Raft.absoluteIndex]Raft[%d] RelativeIndex: %d, lastAppliedIndex: %d, AbsoluteIndex: %d", rf.Me(), relativeIndex, rf.LastAppliedIndex(), absoluteIndex)
	return int(absoluteIndex)
}

func (rf *Raft) LastAppliedIndex() int64 {
	return atomic.LoadInt64(&rf.lastAppliedIndex)
}

func (rf *Raft) SetLastAppliedIndex(set int64) {
	atomic.StoreInt64(&rf.lastAppliedIndex, set)
}

func (rf *Raft) LastAppliedTerm() int64 {
	return atomic.LoadInt64(&rf.lastAppliedTerm)
}

func (rf *Raft) SetLastAppliedTerm(set int64) {
	atomic.StoreInt64(&rf.lastAppliedTerm, set)
}

func (rf *Raft) absoluteLen() int {
	return len(rf.persistentState.LogEntries) + int(rf.lastAppliedIndex)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.mu.Lock()
	b := rf.getPersistenceStatusBytes()
	rf.mu.Unlock()
	rf.persister.SaveRaftState(b)

	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

func (rf *Raft) getPersistenceStatusBytes() []byte {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	p := StateToPersist{
		PersistentState: PersistentState{
			CurrentTerm: rf.CurrentTerm(),
			VotedFor:    rf.votedFor(),
			LogEntries:  rf.Logs(),
		},
		LastAppliedIndex: rf.LastAppliedIndex(),
		LastAppliedTerm:  rf.LastAppliedTerm(),
	}
	if err := encoder.Encode(p); err != nil {
		panic(fmt.Sprintf("Failed to encode persistentState, err = %s", err))
	}
	return buffer.Bytes()
}

func (rf *Raft) storePersistentState(data []byte) {
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)

	persistentState := new(StateToPersist)
	if err := decoder.Decode(persistentState); err != nil {
		panic(fmt.Sprintf("Failed to read persist, err = %s", err))
	}

	DPrintf("[Raft.storePersistentState] Raft[%d] read persistentState: %+v", rf.Me(), persistentState)
	rf.persistentState = persistentState.PersistentState
	rf.SetLastAppliedTerm(persistentState.LastAppliedTerm)
	rf.SetLastAppliedIndex(persistentState.LastAppliedIndex)
	rf.volatileState.CommitIndex = rf.lastAppliedIndex
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	//DPrintf("[Raft.readPersist]Raft[%d] reads %s", rf.Me(), string(data))
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.persistentState = PersistentState{
			VotedFor:   consts.DefaultNoCandidate,
			LogEntries: []Log{{Term: 0, Command: nil}},
		}
		return
	}

	rf.storePersistentState(data)
	DPrintf("[Raft.readPersist]Raft[%d] read persist successfully, %+v", rf.me, rf.persistentState)

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
	if rf == nil {
		return false
	}

	return rf.doubleLockCheckIfDo(func() bool {
		return int64(lastIncludedIndex) > rf.LastAppliedIndex()
	}, func() bool {
		DPrintf("[Raft.CondInstallSnapshot] Raft[%d] ready to cond install snapshot, lastIncludedIndex = %d, lastAppliedTerm = %d", rf.Me(), lastIncludedIndex, lastIncludedTerm)

		relativeIndex := rf.relativeIndex(int64(lastIncludedIndex))

		if lastIncludedIndex >= rf.absoluteLen() {
			rf.SetLastAppliedTerm(int64(lastIncludedTerm))
			rf.SetLastAppliedIndex(int64(lastIncludedIndex))
			rf.storeCommitIndex(int64(lastIncludedIndex))
			rf.persistentState.LogEntries = []Log{{Term: rf.lastAppliedTerm}}
		} else {
			DPrintf("[Raft.CondInstallSnapshot]Raft[%d] ready to snapshot, snap index = %d, relative index = %d, logs = %+v", rf.Me(), lastIncludedIndex, relativeIndex, rf.Logs())

			rf.SetLastAppliedTerm(int64(lastIncludedTerm))
			rf.SetLastAppliedIndex(int64(lastIncludedIndex))
			rf.storeCommitIndex(int64(lastIncludedIndex))
			temp := make([]Log, len(rf.persistentState.LogEntries)-relativeIndex)
			temp[0] = Log{Term: rf.lastAppliedTerm}
			copy(temp[1:], rf.persistentState.LogEntries[relativeIndex+1:])
			rf.persistentState.LogEntries = temp

			DPrintf("[Raft.CondInstallSnapshot]Raft[%d] condInstallSnapshot finished, logs: %+v", rf.Me(), rf.Logs())
		}
		rf.persister.SaveStateAndSnapshot(rf.getPersistenceStatusBytes(), snapshot)

		return true
	})
	// Your code here (2D).
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	DPrintf("[Raft.Snapshot]Raft[%d] ready to snapshot, snap index = %d, relative index = %d", rf.Me(), index, rf.LastAppliedIndex())

	rf.doubleLockCheckIfDo(func() bool {
		relativeIndex := rf.relativeIndex(int64(index))
		return relativeIndex >= 0
	}, func() bool {
		relativeIndex := rf.relativeIndex(int64(index))
		rf.SetLastAppliedIndex(int64(index))
		rf.SetLastAppliedTerm(rf.persistentState.LogEntries[relativeIndex].Term)
		temp := make([]Log, len(rf.persistentState.LogEntries)-relativeIndex)
		temp[0] = Log{Term: rf.lastAppliedTerm}
		copy(temp[1:], rf.persistentState.LogEntries[relativeIndex+1:])
		rf.persistentState.LogEntries = temp

		rf.persister.SaveStateAndSnapshot(rf.getPersistenceStatusBytes(), snapshot)
		DPrintf("[Raft.Snapshot] Raft[%d] snapshot finished, lastAppliedIndex: %d", rf.Me(), rf.LastAppliedIndex())
		return true
		// Your code here (2D).
	})
}

type InstallSnapshotReq struct {
	Term              int64
	LeaderID          int64
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotResp struct {
	Term int64
}

func (rf *Raft) sendInstallSnapshot(req *InstallSnapshotReq, resp *InstallSnapshotResp, n int) bool {
	if rf.isLeader() {
		DPrintf("[Raft.sendInstallSnapshot] Raft[%d] send installSnapshot to Raft[%d], req = %+v", rf.Me(), n, *req)
		ok := rf.peers[n].Call(consts.MethodInstallSnapshot, req, resp)
		return ok
	}
	return false
}

func (rf *Raft) InstallSnapShot(req *InstallSnapshotReq, resp *InstallSnapshotResp) {
	DPrintf("[Raft.InstallSnapshot]Raft[%d] receives InstallSnapshot, req = %+v", rf.Me(), req)
	rf.checkTerm(req.Term)
	resp.Term = rf.CurrentTerm()
	if req.Term < rf.CurrentTerm() {
		DPrintf("[Raft.InstallSnapshot]Raft[%d].Term = %d while receives term: %d, return", rf.Me(), rf.CurrentTerm(), req.LastIncludedTerm)
		return
	}

	defer rf.recvInstallSnapShot()

	rf.commitChan <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      req.Data,
		SnapshotTerm:  req.LastIncludedTerm,
		SnapshotIndex: req.LastIncludedIndex,
	}
}

func (rf *Raft) recvRequestVote() {
	if rf.isServerType(consts.ServerTypeFollower) {
		DPrintf("[Raft.recvRequestVote] Raft[%d], serverType: %v received requestVote, term: %d", rf.Me(), rf.getServerType(), rf.CurrentTerm())
		rf.recRequestVote <- struct{}{}
		rf.changeServerType(consts.ServerTypeFollower)
		DPrintf("[Raft.recvRequestVote] Raft[%d], serverType: %v sent requestVote, term: %d", rf.Me(), rf.getServerType(), rf.CurrentTerm())
	}
}

// RequestVote
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("[Raft.RequestVote]%+v requestVote from Raft[%d], req = %+v", rf, args.CandidateID, args)
	// Your code here (2A, 2B).

	rf.checkTerm(args.Term)
	reply.Term = rf.CurrentTerm()

	// rule 1
	// Reply false if term < CurrentTerm
	if args.Term < rf.CurrentTerm() {
		DPrintf("[Raft.RequestVote]Raft[%d].Term = %d, Raft[%d].Term = %d, refused", args.CandidateID, args.Term, rf.Me(), rf.CurrentTerm())
		return
	}
	// rule 2
	//

	if rf.doubleLockCheckIfDo(func() bool {
		return (rf.noVoteFor() || rf.isVoteFor(args.CandidateID)) && rf.isServerType(consts.ServerTypeFollower)
	}, func() bool {
		if rf.isAtLeastUpToDateAsMyLog(args) {
			reply.VoteGranted = true
			rf.storeVotedFor(args.CandidateID)
			rf.changeServerType(consts.ServerTypeFollower)
			go rf.recvRequestVote()
			DPrintf("[Raft.RequestVote]Raft[%d] votes for Raft[%d], reply: %+v", rf.Me(), args.CandidateID, reply)
			return true
		}
		return false
	}) {
		return
	}

	DPrintf("[Raft.RequestVote]Raft[%d] refuse to vote for Raft[%d]", rf.Me(), args.CandidateID)
	return
}

func (rf *Raft) isAtLeastUpToDateAsMyLog(args *RequestVoteArgs) bool {
	lateLogIndex := len(rf.persistentState.LogEntries) - 1
	latestLogTerm := rf.persistentState.LogEntries[lateLogIndex].Term
	absoluteIndex := rf.absoluteIndex(int64(lateLogIndex))

	DPrintf("[Raft.isAtLeastUpToDateAsMyLog] Raft[%d] send vote request to Raft[%d][latestLogTerm:%d], absoluteIndex:%d, req: %+v,", args.CandidateID, rf.Me(), latestLogTerm, absoluteIndex, args)
	return args.LastLogTerm > latestLogTerm ||
		(args.LastLogTerm == latestLogTerm && args.LastLogIndex >= int64(absoluteIndex))
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
	DPrintf("[Raft.sendRequestVote] Raft[%d].Term:%d send to %d", rf.Me(), rf.CurrentTerm(), server)
	if rf.isServerType(consts.ServerTypeCandidate) {
		ok := rf.peers[server].Call(consts.MethodRequestVote, args, reply)
		DPrintf("[Raft.sendRequestVote]Raft[%d] receives resp from %d, resp = %+v", rf.Me(), server, reply)
		rf.checkTerm(reply.Term)
		return ok && reply.VoteGranted
	}
	return false
}

// checkTerm
// if RPC request or response contains term T > CurrentTerm,
// set CurrentTerm to T, convert to follower
func (rf *Raft) checkTerm(term int64) bool {
	return rf.doubleLockCheckIfDo(func() bool {
		return term > rf.CurrentTerm()
	}, func() bool {
		rf.changeServerType(consts.ServerTypeFollower)
		rf.storeVotedFor(consts.DefaultNoCandidate)
		DPrintf("[raft.checkTerm]Raft[%d].term = %d Get term: %d, change to follower", rf.Me(), rf.CurrentTerm(), term)
		rf.updateTerm(term)
		return true
	})
}

func (rf *Raft) recvAppendEntries() {
	rf.doubleLockCheckIfDo(func() bool {
		return rf.isServerType(consts.ServerTypeFollower)
	}, func() bool {
		go func() {
			rf.revAppendEntries <- struct{}{}
			DPrintf("[Raft.recvRequestVote] Raft[%d], serverType: %v received appendEntries, term: %d", rf.Me(), rf.getServerType(), rf.CurrentTerm())
		}()
		rf.changeServerType(consts.ServerTypeFollower)
		return true
	})
}

func (rf *Raft) recvInstallSnapShot() {
	rf.doubleLockCheckIfDo(func() bool {
		return rf.isServerType(consts.ServerTypeFollower)
	}, func() bool {
		go func() {
			rf.revAppendEntries <- struct{}{}
			DPrintf("[Raft.recvRequestVote] Raft[%d], serverType: %v received snapshot, term: %d", rf.Me(), rf.getServerType(), rf.CurrentTerm())
		}()
		rf.changeServerType(consts.ServerTypeFollower)
		return true
	})
}

func (rf *Raft) recvFromLeader(req *AppendEntriesReq) {
	rf.doubleLockCheckIfDo(func() bool {
		return rf.isServerType(consts.ServerTypeCandidate)
	}, func() bool {
		if req.Term >= rf.CurrentTerm() {
			rf.changeServerType(consts.ServerTypeFollower)
			DPrintf("[Raft.recvFromLeader] Raft[%d] receives AppendEntries from leader[%d]", rf.Me(), req.LeaderID)
		}
		return true
	})
}

func (rf *Raft) getFastBackUpInfo(absolutePreLogIndex int) FastBackUp {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	relativePreLogIndex := rf.relativeIndex(int64(absolutePreLogIndex))

	lenOfLog := rf.absoluteLen()
	DPrintf("[Raft.getFastBackUpInfo]Raft[%d] absolute preLogIndex = %d, relative preLogIndex = %d, absoluteLenOfLog = %d, lastAppliedIndex: %d, logs: %+v", rf.Me(), absolutePreLogIndex, relativePreLogIndex, lenOfLog, rf.LastAppliedIndex(), rf.Logs())
	result := FastBackUp{
		Len:  lenOfLog,
		Term: consts.IndexOutOfRange,
	}

	if absolutePreLogIndex >= lenOfLog {
		return result
	}

	if relativePreLogIndex < 0 {
		result.Index = int(rf.lastAppliedIndex + 1)
		return result
	}

	result.Term = int(rf.persistentState.LogEntries[relativePreLogIndex].Term)
	for i := 0; i < lenOfLog; i++ {
		if rf.persistentState.LogEntries[i].Term == int64(result.Term) {
			DPrintf("[Raft.getFastBackUpInfo]Raft[%d] first index of term(%d) = %d", rf.Me(), result.Term, i)
			result.Index = rf.absoluteIndex(int64(i))
			break
		}
	}
	return result
}

func (rf *Raft) AppendEntries(req *AppendEntriesReq, resp *AppendEntriesResp) {
	DPrintf("[Raft.AppendEntries] %+v AppendEntries from Raft[%d], req = %+v", rf, req.LeaderID, req)

	rf.recvFromLeader(req)
	rf.checkTerm(req.Term)
	resp.Term = rf.CurrentTerm()

	// rule 1
	// Reply false if term < CurrentTerm
	if req.Term < rf.CurrentTerm() {
		resp.Success = false
		return
	}
	rf.recvAppendEntries()

	// rule 2
	// Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if !rf.checkConsistency(req.PrevLogIndex, int(req.PreLogTerm)) {
		resp.Success = false
		resp.FastBackUp = rf.getFastBackUpInfo(req.PrevLogIndex)

		DPrintf("[Raft.AppendEntries] Raft[%d] failed to check consistency with leader[%d], resp = %+v", rf.Me(), req.LeaderID, resp)
		return
	}

	// rule 3, 4
	rf.doubleLockCheckIfDo(func() bool {
		return rf.CurrentTerm() == req.Term
	}, func() bool {
		index := rf.tryBeAsConsistentAsLeader(req.PrevLogIndex, req.Entries)
		DPrintf("[Raft.AppendEntries] Raft[%d] finish check index. absolute last index: %d", rf.Me(), index)
		// rule 5
		// If leaderCommit > commitIndex, set commitIndex =
		// min(leaderCommit, index of last new entry)
		// maybe the problem length of req.Entries
		if req.LeaderCommit > rf.commitIndex() && index > int(rf.commitIndex()) {
			min := utils.Min(req.LeaderCommit, int64(index))
			DPrintf("[Raft.AppendEntries] Raft[%d] leader commit = %d, change commit index to %d", rf.Me(), req.LeaderCommit, min)
			go rf.commit(int(min))
		}

		rf.changeServerType(consts.ServerTypeFollower)
		resp.Success = true
		return true
	})
	return
}

func (rf *Raft) checkConsistency(index, term int) bool {
	absoluteLatestLogIndex := rf.absoluteLatestLogIndex()
	if index > absoluteLatestLogIndex {
		DPrintf("[Raft.checkConsistency] Raft[%d] absoluteLatestLogIndex: %d, index: %d", rf.Me(), absoluteLatestLogIndex, index)
		return false
	}

	nthLogTerm := rf.getNthLog(index).Term
	DPrintf("[Raft.checkConsistency] Raft[%d], index = %d, term = %d, absoluteLatestLogIndex = %d, Log[%d].Term = %d", rf.Me(), index, term, absoluteLatestLogIndex, index, nthLogTerm)
	return int(nthLogTerm) == term
}

// tryBeAsConsistentAsLeader
// rule3: if an existing entry conflicts with a new one(same index but different terms),
// delete the existing entry and all that follow it
func (rf *Raft) tryBeAsConsistentAsLeader(index int, logs []Log) int {
	if len(logs) == 0 {
		DPrintf("[Raft.tryBeAsConsistentAsLeader] Raft[%d] got logs: %+v", rf.Me(), len(logs))
		return rf.absoluteIndex(int64(len(rf.persistentState.LogEntries) - 1))
	}

	lastIndex := 0
	lastIndex = rf.consistLogs(index, logs)
	go rf.persist()
	return rf.absoluteIndex(int64(lastIndex))
}

func (rf *Raft) consistLogs(index int, logs []Log) int {
	relativeIndex := rf.relativeIndex(int64(index))
	if relativeIndex < 0 {
		DPrintf("[Raft.consistLogs] Raft[%d] got index: %d, logs: %+v, but relativeIndex: %d", rf.Me(), index, logs, relativeIndex)
		return 0
	}

	DPrintf("[Raft.consistLog] Raft[%d] logs start at %d is %v", rf.Me(), relativeIndex+1, rf.persistentState.LogEntries[relativeIndex+1:])
	for i := relativeIndex + 1; i < len(rf.persistentState.LogEntries) && (i-relativeIndex-1) < len(logs); i++ {
		if rf.persistentState.LogEntries[i].Term != logs[i-relativeIndex-1].Term {
			DPrintf("[Raft.consistLogs] Raft[%d]Remove logs after index: %d and append logs: %+v", rf.Me(), i-1, logs[i-relativeIndex-1:])
			rf.persistentState.LogEntries = append(rf.persistentState.LogEntries[:i], logs[i-relativeIndex-1:]...)
			return len(rf.persistentState.LogEntries) - 1
		}
	}

	if relativeIndex+len(logs) >= len(rf.persistentState.LogEntries) {
		DPrintf("[Raft.consistLogs] Raft[%d]Remove logs after %d and append logs: %+v", rf.Me(), len(rf.persistentState.LogEntries)-1, logs[len(rf.persistentState.LogEntries)-relativeIndex-1:])
		rf.persistentState.LogEntries = append(rf.persistentState.LogEntries, logs[len(rf.persistentState.LogEntries)-relativeIndex-1:]...)
	}
	return len(rf.persistentState.LogEntries) - 1
}

func (rf *Raft) sendAppendEntries(req *AppendEntriesReq, resp *AppendEntriesResp, server int) bool {
	if rf.isLeader() && req.Term == rf.CurrentTerm() {
		//DPrintf("[Raft.sendAppendEntries] Leader[%d] send %+v to Raft[%d]", rf.Me(), req, server)
		ok := rf.peers[server].Call(consts.MethodAppendEntries, req, resp)
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
	rf.doWithDoubleLockCheckIfIsLeader(func() bool {
		DPrintf("[Raft.Start]Raft[%d] start to replicate command: %+v, term: %d", rf.Me(), command, rf.CurrentTerm())
		rf.persistentState.LogEntries = append(rf.persistentState.LogEntries, Log{
			Term:    rf.CurrentTerm(),
			Command: command,
		})
		index = rf.absoluteIndex(int64(len(rf.persistentState.LogEntries) - 1))
		// DPrintf("[Raft.Start]Raft[%d] Log:%+v", rf.Me(), rf.persistentState.LogEntries)
		go rf.persist()
		DPrintf("[Raft.Start]Raft[%d] Receive command %+v, term = %d, index = %d", rf.Me(), command, rf.CurrentTerm(), index)
		go rf.processNewCommand(index)
		return true
	})

	return index, int(rf.CurrentTerm()), rf.isLeader() && !rf.killed()
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
	for i := 0; i < len(rf.peers)-1; i++ {
		if <-replicated {
			num++
		}

		//DPrintf("[Raft.processNewCommand] i = %d, replicate num: %d, index = %d", i, num, index)
		// commit if a majority of peers replicate
		if num > len(rf.peers)/2 {
			if firstTime {
				DPrintf("[Raft.processNewCommand] Leader[%d] ready to commit log to index: %d", rf.Me(), index)
				go rf.doWithDoubleLockCheckIfIsLeader(func() bool {
					DPrintf("[Raft.processNewCommand] Leader[%d] ready to commit log to index: %d", rf.Me(), index)
					go rf.commit(index)
					return true
				})

				firstTime = false
			}
		}
	}
	close(replicated)
}

func (rf *Raft) commit(index int) {
	if index < int(rf.commitIndex()) {
		DPrintf("[Raft.commit] Raft[%d] ready to commit index to %d, but commitIndex: %d, return", rf.Me(), index, rf.commitIndex())
		return
	}

	rf.cmu.Lock()
	defer rf.cmu.Unlock()
	if index < int(rf.commitIndex()) {
		DPrintf("[Raft.commit] Raft[%d] ready to commit index to %d, but commitIndex: %d, return", rf.Me(), index, rf.commitIndex())
		return
	}

	DPrintf("[Raft.commit] %+v ready to commit logs to index: %d", rf, index)
	start := rf.commitIndex() + 1

	relativeStartIndex := rf.relativeIndex(start)
	relativeIIndex := rf.relativeIndex(int64(index))
	if relativeStartIndex <= 0 || relativeIIndex <= 0 {
		DPrintf("[Raft.commit] Relative index of start:%d is %d, relative of index:%d is %d", start, relativeStartIndex, index, relativeIIndex)
		return
	}
	DPrintf("[Raft.commit] %+v, from Log[%d]%+v to Log[%d]%+v", rf, start, rf.getNthLog(int(start)), index, rf.getNthLog(index))

	for i := start; i <= int64(index); i++ {
		log := rf.getNthLog(int(i))
		if log.Term == consts.LogNotFound {
			DPrintf("[Raft.commit]Raft[%d] Failed to get Log[%d]", rf.Me(), i)
			continue
		}

		commitIndex := rf.commitIndex()
		if i == commitIndex+1 {
			DPrintf("[Raft.commit] Raft[%d] commit Log[%d]: %+v", rf.Me(), i, log)
			rf.commitChan <- ApplyMsg{
				CommandValid: true,
				Command:      log.Command,
				CommandIndex: int(i),
			}
			rf.storeCommitIndex(i)
		}

	}

	DPrintf("[Raft.commit] %+v commit logs to index: %d finished", rf, index)
}

func (rf *Raft) fastBackUp(info FastBackUp) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if info.Term == consts.IndexOutOfRange {
		DPrintf("[Raft.fastBackUp] Index out of range, set nextIndex to info.Len = %d", info.Len)
		return info.Len
	}

	if !rf.isTermExist(int64(info.Term)) {
		DPrintf("[Raft.fastBackUp] Term: %d do no exist, nextIndex = %d", info.Term, info.Index)
		return info.Index
	} else {
		result := rf.absoluteIndex(int64(rf.lastIndexOfTerm(int64(info.Term))))
		DPrintf("[Raft.fastBackUp] Term: %d exist, last index of %d is %d", info.Term, info.Term, result)
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
	term := rf.CurrentTerm()
	var lenAppend int
	var entries []Log

	nextIndex := rf.getNthNextIndex(n)
	nNextIndex := nextIndex
	//DPrintf("[Raft.sendAppendEntries2NServer] NextIndex = %d relativeLatestLogIndex = %d, ready to replicate on Raft[%d]， index = %d, absoluteLatestIndex = %d, lastAppliedIndex: %d", nextIndex, rf.relativeLatestLogIndex(), n, index, absoluteLatestIndex, rf.LastAppliedIndex())
	// leaders rule3
	if rf.absoluteLatestLogIndex() >= nextIndex && rf.isLeader() {
		var finished bool

		for !finished && rf.isLeader() && nextIndex <= index && index <= rf.absoluteLatestLogIndex() && index > rf.getNthMatchIndex(n) && term == rf.CurrentTerm() {
			ok := false

			if rf.isFollowerCatchUp(nNextIndex) {
				for !ok && rf.isLeader() && nextIndex >= int(rf.LastAppliedIndex()) && nextIndex <= index && index <= rf.absoluteLatestLogIndex() && index > rf.getNthMatchIndex(n) && term == rf.CurrentTerm() {
					nNextIndex = rf.getNthNextIndex(n)
					if nNextIndex < int(rf.LastAppliedIndex()) {
						break
					}

					time.Sleep(time.Millisecond * 10)
					if nextIndex <= int(rf.LastAppliedIndex()) {
						DPrintf("[Raft.sendAppendEntries2NServer]Raft[%d]: lastAppliedIndex: %d, nextIndex: %d, nNextIndex: %d", rf.Me(), rf.LastAppliedIndex(), nextIndex, nNextIndex)
						nNextIndex = nextIndex
						break
					}

					entries = rf.getLogsFromTo(nextIndex, index)
					//DPrintf("[Raft.sendAppendEntries2NServer]Leader[%d] index = %d Logs replicated on Raft[%d] from (nextIndex) = %d to %d", rf.Me(), index, n, nextIndex, index)
					lenAppend = len(entries)
					if lenAppend == 0 {
						DPrintf("[Raft.sendAppendEntries2NServer]Leader[%d] Ready to replicates on Raft[%d].Try to get index from %d to %d, but lastAppliedIndex = %d, set nNextIndex to NextIndex: %d, len of logs: %v", rf.Me(), n, nextIndex, index, rf.LastAppliedIndex(), nextIndex, rf.lengthOfLog())
						nNextIndex = nextIndex
						break
					}

					if term != rf.CurrentTerm() {
						replicated <- false
						return
					}
					req := &AppendEntriesReq{
						Term:         term,
						LeaderID:     rf.Me(),
						PrevLogIndex: nextIndex - 1,
						PreLogTerm:   rf.getNthLog(nextIndex - 1).Term,
						Entries:      entries,          // [1,2,3]
						LeaderCommit: rf.commitIndex(), //100
					}
					resp := new(AppendEntriesResp)
					ok = rf.sendAppendEntries(req, resp, n)

					finished = resp.Success && ok
					if rf.isLeader() && ok && !resp.Success {
						DPrintf("[Raft.sendAppendEntries2NServer] Follower[%d] is inconsistent, get ready to fast backup: %+v", n, resp.FastBackUp)
						nextIndex = rf.fastBackUp(resp.FastBackUp)
						DPrintf("[Raft.sendAppendEntries2NServer] Fast backup finished, next index = %d", nextIndex)

						if nextIndex < int(rf.LastAppliedIndex()) {
							DPrintf("[Raft.sendAppendEntries2NServer] fast backup nextIndex found: %d, lastAppliedIndex: %d", nextIndex, rf.LastAppliedIndex())
							nNextIndex = nextIndex
							break
						}
					}
				}
			} else {
				for !ok && rf.isLeader() {
					time.Sleep(time.Millisecond * 10)
					DPrintf("[Raft.sendAppendEntries2NServer]Ready to send InstallSnapshot RPC to Raft[%d], nextIndex of Raft[%d] is %d, but lastAppliedIndex = %d, index = %d", n, n, rf.getNthNextIndex(n), rf.LastAppliedIndex(), index)
					req := &InstallSnapshotReq{
						Term:              rf.CurrentTerm(),
						LeaderID:          rf.Me(),
						LastIncludedIndex: int(rf.LastAppliedIndex()),
						LastIncludedTerm:  int(rf.LastAppliedTerm()),
						Data:              rf.persister.ReadSnapshot(),
					}
					resp := new(InstallSnapshotResp)
					ok = rf.sendInstallSnapshot(req, resp, n)
					rf.checkTerm(resp.Term)

					if !rf.isLeader() {
						replicated <- false
						return
					}
				}
				if ok && rf.isLeader() {
					rf.storeNthNextIndex(n, int(rf.LastAppliedIndex()+1))
					rf.storeNthMatchedIndex(n, int(rf.LastAppliedIndex()))
					nextIndex = int(rf.lastAppliedIndex + 1)
					nNextIndex = nextIndex
				}
			}

		}

		if rf.doubleLockCheckIfDo(func() bool {
			return rf.isLeader() && finished
		}, func() bool {
			DPrintf("[Raft.sendAppendEntries2NServer] Leader[%d] finished commit log to index: %d to Raft[%d]. nextIndex: %d, lenAppend: %d", rf.Me(), index, n, nextIndex, lenAppend)
			next := nextIndex + lenAppend
			raw := rf.leaderState.NextIndex[n]
			if next > raw {
				raw = next
			}
			rf.leaderState.NextIndex[n] = raw
			rf.leaderState.MatchIndex[n] = raw - 1
			DPrintf("[Raft.sendAppendEntries2NServer]Leader[%d]: Raft[%d] matchIndex now = %d successfully, and index = %d, entries = %v", rf.Me(), n, raw-1, index, entries)
			replicated <- true
			return true
		}) {
			return
		}
	}
	//DPrintf("[Raft.sendAppendEntries2NServer]Raft[%d] replicate logs to index: %d on Raft[%d] failed", rf.Me(), index, n)
	replicated <- false
}

func (rf *Raft) doWithDoubleLockCheckIfIsLeader(fc func() bool) bool {
	return rf.doubleLockCheckIfDo(rf.isLeader, fc)
}

func (rf *Raft) doubleLockCheckIfDo(ifFunc func() bool, do func() bool) bool {
	// atomic operation
	if ifFunc() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if ifFunc() {
			return do()
		}
	}
	return false
}

func (rf *Raft) isFollowerCatchUp(nNextIndex int) bool {
	result := nNextIndex > int(rf.LastAppliedIndex())
	// DPrintf("[Raft.isFollowerCatchUp] Leader[%d] lastAppliedIndex: %d, Follower[%d] nextIndex: %d, catch up: %+v", rf.Me(), rf.LastAppliedIndex(), nth, rf.getNthNextIndex(nth), result)
	return result
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
	DPrintf("[Raft.Kill] Raft[%d] has been killed", rf.me)
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) isServerType(serverType consts.ServerType) bool {
	return rf.getServerType() == serverType
}

func (rf *Raft) heartbeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i != int(rf.Me()) && rf.isLeader() {
			go rf.sendHeartBeat2NServer(i)
		}
	}
}

func (rf *Raft) sendHeartBeat2NServer(i int) {
	req := &AppendEntriesReq{
		Term:         rf.CurrentTerm(),
		LeaderID:     rf.Me(),
		PrevLogIndex: int(rf.commitIndex()),
		PreLogTerm:   rf.getNthLog(int(rf.commitIndex())).Term,
		Entries:      nil,
		LeaderCommit: rf.commitIndex(),
	}
	resp := &AppendEntriesResp{}
	if rf.isLeader() {
		//DPrintf("Raft[%d] send heartbeat to %d, req = %+v", rf.Me(), i, *req)
		rf.sendAppendEntries(req, resp, i)
	}
}

func (rf *Raft) requestVote(ctx context.Context, voteChan chan<- bool) {
	term := rf.CurrentTerm()
	finish := make(chan bool)
	defer close(finish)

	vote := int64(1)
	no := int64(0)

	for i := 0; i < len(rf.peers); i++ {
		if i != int(rf.Me()) {
			go func(i int) {
				if !rf.isServerType(consts.ServerTypeCandidate) {
					finish <- false
					return
				}

				req := &RequestVoteArgs{
					Term:         rf.CurrentTerm(),
					CandidateID:  rf.Me(),
					LastLogIndex: int64(rf.absoluteLatestLogIndex()),
					LastLogTerm:  rf.latestLog().Term,
				}
				resp := &RequestVoteReply{}
				finish <- rf.sendRequestVote(i, req, resp)
			}(i)
		}
	}

	for i := 0; i < len(rf.peers)-1; i++ {
		select {
		case <-ctx.Done():
			if rf.isServerType(consts.ServerTypeFollower) {
				DPrintf("[Raft.requestVote] Raft[%d] term: %d time out", rf.Me(), term)
			}

			for j := i; j < len(rf.peers)-1; j++ {
				<-finish
			}
			return

		case v := <-finish:
			if v {
				DPrintf("[Raft.requestVote] Candidate[%d] received vote", rf.Me())
				vote++
			} else {
				no++
			}

			if vote > int64(len(rf.peers)/2) || no > int64(len(rf.peers)/2) {
				voteChan <- vote > int64(len(rf.peers)/2) && rf.isServerType(consts.ServerTypeCandidate)

				// receive the left finish
				for j := i + 1; j < len(rf.peers)-1; j++ {
					<-finish
				}
				return
			}
		}
	}
}

// startElection
// 1. increment CurrentTerm
// 2. vote for self
// 3. Reset election timer
// 4. SendRequestVote RPCs to all others servers
func (rf *Raft) startElection(ctx context.Context, electionResult chan<- bool, cancel context.CancelFunc) {
	defer cancel()

	term := rf.CurrentTerm()
	voteChan := make(chan bool)
	defer close(voteChan)
	go rf.requestVote(ctx, voteChan)

	select {
	case success := <-voteChan:
		DPrintf("[Raft.startElection] Raft[%d] Election result: %+v, serverType: %+v", rf.Me(), success, rf.getServerType())
		electionResult <- rf.doubleLockCheckIfDo(func() bool {
			return success && rf.isServerType(consts.ServerTypeCandidate)
		}, func() bool {
			rf.changeServerType(consts.ServerTypeLeader)
			rf.initLeaderState()
			DPrintf("[Raft.startElection] Raft[%d] has become the leader of term: %d", rf.Me(), rf.CurrentTerm())
			return true
		})
		return

	case <-ctx.Done():
		if rf.isServerType(consts.ServerTypeFollower) {
			DPrintf("[Raft.startElection] Raft[%d] term: %d time out", rf.Me(), term)
		}
		<-voteChan
		return
	}
}

func (rf *Raft) initLeaderState() {
	rf.leaderState = LeaderState{
		NextIndex:  make(map[int]int, len(rf.peers)-1),
		MatchIndex: make(map[int]int, len(rf.peers)-1),
	}

	for i := 0; i < len(rf.peers); i++ {
		if int(rf.Me()) != i {
			rf.leaderState.NextIndex[i] = int(rf.commitIndex() + 1)
			rf.leaderState.MatchIndex[i] = rf.leaderState.NextIndex[i] - 1
		}
	}
}

func (rf *Raft) randomTimeout() time.Duration {
	return RandTimeMilliseconds(450, 750)
}

func (rf *Raft) Wait(ifThenExist func() bool, ms int) {
	for i := consts.Interval; i < ms; i += consts.Interval {
		time.Sleep(consts.Interval * time.Millisecond)
		if ifThenExist() {
			break
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
	)
	electionResult := make(chan bool)

	for rf.killed() == false {
		timeout := rf.randomTimeout()
		switch rf.getServerType() {

		case consts.ServerTypeLeader:
			time.Sleep(consts.Interval * time.Millisecond)

			if !rf.doWithDoubleLockCheckIfIsLeader(
				func() bool {
					go rf.heartbeat()
					return true
				}) {
				continue
			}

			rf.Wait(func() bool {
				return !rf.isLeader()
			}, 140)

		case consts.ServerTypeCandidate:

			if !rf.doubleLockCheckIfDo(func() bool {
				return rf.isServerType(consts.ServerTypeCandidate)
			}, func() bool {
				rf.selfIncrementCurrentTerm()
				rf.voteForSelf()
				return true
			}) {
				continue
			}

			ctx, cancel = context.WithTimeout(context.Background(), timeout)
			now := time.Now()
			if !rf.isServerType(consts.ServerTypeCandidate) {
				cancel()
				continue
			}

			go rf.startElection(ctx, electionResult, cancel)
			select {
			case <-ctx.Done():

			case result := <-electionResult:
				if !result && rf.isServerType(consts.ServerTypeCandidate) {

					timeToWait := (timeout - time.Since(now)).Milliseconds()

					rf.Wait(func() bool {
						return !rf.isServerType(consts.ServerTypeCandidate)
					}, int(timeToWait))
				}
			}

		case consts.ServerTypeFollower:
			select {
			// followers rule 2
			case <-time.After(timeout):
				DPrintf("[Raft.ticker] Raft[%d] term: %d change to candidate.", rf.Me(), rf.CurrentTerm())
				rf.changeServerType(consts.ServerTypeCandidate)
			case <-rf.revAppendEntries:

			case <-rf.recRequestVote:

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
		peers:            peers,
		persister:        persister,
		me:               int64(me),
		revAppendEntries: make(chan struct{}),
		recRequestVote:   make(chan struct{}),
		serverType:       int32(consts.ServerTypeFollower),
		commitChan:       applyCh,
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
