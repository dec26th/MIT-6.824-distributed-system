package shardkv

import (
	"6.824/shardctrler"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"

	OpPut    = "Put"
	OpAppend = "Append"
	OpGet    = "Get"

	MethodShardKVMigrate = "ShardKV.Migrate"
	Interval             = 200 * time.Millisecond

	Sharding = 1
	Idle     = 0
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	RequestID int64
	ClientID  int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key      string
	ClientID int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type MigrateArgs struct {
	ShardIDList []int
	Config      shardctrler.Config
}

type MigrateReply struct {
	Err  Err
	Info Info
}

type Info struct {
	Store  map[int]Store
	Record map[int]Record
}
