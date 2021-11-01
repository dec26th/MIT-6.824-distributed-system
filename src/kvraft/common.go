package kvraft

import "fmt"

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

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

func (p *PutAppendArgs) String() string {
	return fmt.Sprintf(`{Key: %s, Value: %s, Op: %s}`, p.Key, p.Value, p.Op)
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
