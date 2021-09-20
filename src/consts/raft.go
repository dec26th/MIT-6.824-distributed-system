package consts

const (
	MethodAppendEntries   = "Raft.AppendEntries"
	MethodRequestVote     = "Raft.RequestVote"
	MethodInstallSnapshot = "Raft.InstallSnapShot"

	ServerTypeLeader    = 1
	ServerTypeCandidate = 2
	ServerTypeFollower  = 3

	Interval = 10

	IndexOutOfRange = -1
)

var DefaultNoCandidate = int64(-1)
