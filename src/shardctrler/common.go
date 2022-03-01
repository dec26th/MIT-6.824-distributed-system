package shardctrler

import "time"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (c Config) NewerThan(num int) bool {
	return c.Num > num
}

type Modifier func(config *Config)

const (
	OK       = "OK"
	Interval = 200 * time.Millisecond
)

type Err string

type Args interface {
	GetRequestID() int64
	GetClientID() int64
}

type Reply interface {
	SetErr(err Err)
	SetWrongLeader(wrongLeader bool)
}

type JoinArgs struct {
	Servers   map[int][]string // new GID -> servers mappings
	RequestID int64
	ClientID  int64
}

func (j *JoinArgs) GetRequestID() int64 {
	return j.RequestID
}

func (j *JoinArgs) GetClientID() int64 {
	return j.ClientID
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

func (j *JoinReply) SetErr(err Err) {
	j.Err = err
}

func (j *JoinReply) SetWrongLeader(wrongLeader bool) {
	j.WrongLeader = wrongLeader
}

type LeaveArgs struct {
	GIDs      []int
	RequestID int64
	ClientID  int64
}

func (l *LeaveArgs) GetRequestID() int64 {
	return l.RequestID
}

func (l *LeaveArgs) GetClientID() int64 {
	return l.ClientID
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

func (l *LeaveReply) SetErr(err Err) {
	l.Err = err
}

func (l *LeaveReply) SetWrongLeader(wrongLeader bool) {
	l.WrongLeader = wrongLeader
}

type MoveArgs struct {
	Shard     int
	GID       int
	RequestID int64
	ClientID  int64
}

func (m *MoveArgs) GetRequestID() int64 {
	return m.RequestID
}

func (m *MoveArgs) GetClientID() int64 {
	return m.ClientID
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

func (m *MoveReply) SetErr(err Err) {
	m.Err = err
}

func (m *MoveReply) SetWrongLeader(wrongLeader bool) {
	m.WrongLeader = wrongLeader
}

type QueryArgs struct {
	Num       int // desired config number
	RequestID int64
	ClientID  int64
}

func (q QueryArgs) GetRequestID() int64 {
	return q.RequestID
}

func (q QueryArgs) GetClientID() int64 {
	return q.ClientID
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

func (q *QueryReply) SetErr(err Err) {
	q.Err = err
}

func (q *QueryReply) SetWrongLeader(wrongLeader bool) {
	q.WrongLeader = wrongLeader
}
