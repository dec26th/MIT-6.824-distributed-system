package consts

const (
	MethodAppendEntries   = "Raft.AppendEntries"
	MethodRequestVote     = "Raft.RequestVote"
	MethodInstallSnapshot = "Raft.InstallSnapShot"

	ServerTypeLeader    = ServerType(1)
	ServerTypeCandidate = ServerType(2)
	ServerTypeFollower  = ServerType(3)

	Interval = 10

	IndexOutOfRange = -1
)

type ServerType int32
func (s ServerType) String() string {
	switch s {
	case 1:
		return "Leader"
	case 2:
		return "Candidate"
	default:
		return "Follower"
	}
}

var DefaultNoCandidate = int64(-1)
