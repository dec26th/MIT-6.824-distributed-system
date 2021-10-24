package kvraft

import (
	"6.824/labrpc"
	"crypto/rand"
)
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

// Get
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	req := &GetArgs{Key: key}
	resp := &GetReply{}

	// You will have to modify this function.
	return ck.sendGet(req, resp)
}

func (ck *Clerk) sendGet(req *GetArgs, resp *GetReply) string {
	var i int

	for {
		ok := ck.servers[i].Call(MethodGet, req, resp)
		if ok && (resp.Err == OK || resp.Err == ErrNoKey) {
			return resp.Value
		}

		i = (i+1) % len(ck.servers)
	}
}

// PutAppend
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	req := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}
	resp := &PutAppendReply{}

	ck.sendPutAppend(req, resp)
	// You will have to modify this function.
}

func (ck *Clerk) sendPutAppend(req *PutAppendArgs, resp *PutAppendReply) {
	var i int
	for {
		ok := ck.servers[i].Call(MethodPutAppend, req, resp)
		if ok && resp.Err == OK {
			return
		}

		i = (i+1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, Put)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, Append)
}
