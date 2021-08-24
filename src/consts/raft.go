package consts

const (
	MethodAppendEntries = "Raft.AppendEntries"
	MethodRequestVote = "Raft.RequestVote"


	ServerTypeLeader    = 1
	ServerTypeCandidate = 2
	ServerTypeFollower  = 3
)


var DefaultNoCandidate = int64(-1)
