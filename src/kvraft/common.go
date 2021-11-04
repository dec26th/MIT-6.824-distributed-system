package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	OpPut            = "Put"
	OpAppend         = "Append"
	OpGet            = "Get"

	MethodPutAppend = "KVServer.PutAppend"
	MethodGet       = "KVServer.Get"
)

type Err string

func (e Err) WrongLeader() bool {
	return e == ErrWrongLeader
}

func (e Err) OK() bool {
	return e == OK
}

func (e Err) NoKey() bool {
	return e == ErrNoKey
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string
	RequestID string
	// "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
