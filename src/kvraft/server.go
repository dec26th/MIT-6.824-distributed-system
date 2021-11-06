package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

//const Debug = false
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op    string
	Key   string
	Value string
}

type KVServer struct {
	mu       sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	leaderCh chan raft.ApplyMsg
	dead     int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	store  map[string]string
	record map[string]struct{}
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("[KVServer.Get] KV[%d] tries to get key: %v", kv.me, args.Key)
	reply.Err = OK

	index, _, isLeader := kv.rf.Start(Op{
		Op:  OpGet,
		Key: args.Key,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	result := <-kv.leaderCh
	DPrintf("[KVServer.Get] KV[%d] index = %d args = %+v, applyMsg = %+v", kv.me, index, args, result)
	if result.CommandIndex != index {
		reply.Err = ErrWrongLeader
		return
	}

	if kv.isLeader() {
		value, ok := kv.store[args.Key]
		DPrintf("[KVServer.Get] KV[%d] get key: %s, value: %s", kv.me, args.Key, value)
		if !ok {
			reply.Err = ErrNoKey
			return
		}
		reply.Value = value
		return
	} else {
		reply.Err = ErrWrongLeader
		return
	}
	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("[KVServer.PutAppend] KV[%d] received %+v", kv.me, args)
	reply.Err = OK

	if kv.Recorded(args.RequestID) {
		DPrintf("[KVServer.PutAppend] KV[%d] %s request has already executed.", kv.me, args.RequestID)
		return
	}

	DPrintf("[KVServer.PutAppend] KV[%d] ready to send to raft", kv.me)
	index, _, isLeader := kv.rf.Start(Op{
		Op:    args.Op,
		Key:   args.Key,
		Value: args.Value,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("[KVServer.PutAppend] KV[%d] failed to start because server is not a leader", kv.me)
		return
	}
	DPrintf("[KVServer.PutAppend] KV[%d] start to replicate command.", kv.me)

	for {
		result := <-kv.leaderCh
		DPrintf("[KVServer.PutAppend] KV[%d] index = %d args = %+v, applyMsg = %+v", kv.me, index, args, result)
		if result.CommandIndex != index {
			DPrintf("[KVServer.PutAppend] KV[%d] start index = %d, but applyMsg from leaderCh is %+v", kv.me, index, result)
			continue
		}

		if kv.isLeader() {
			kv.mu.Lock()
			DPrintf("[KVServer.PutAppend] KV[%d] index = %d, try to modify the store, args = %+v, before modify: [%s:%s]", kv.me, index, result, args.Key, kv.store[args.Key])
			kv.doPutAppend(args)
			DPrintf("[KVServer.PutAppend] KV[%d] index = %d, after modify: [%s:%s]", kv.me, index, args.Key, kv.store[args.Key])
			kv.mu.Unlock()
		} else {
			DPrintf("[KVServer.PutAppend] KV[%d] now is no longer leader.", kv.me)
			reply.Err = ErrWrongLeader
		}
		return
	}
	// Your code here.
}

func (kv *KVServer) doPutAppend(args *PutAppendArgs) {
	kv.Record(args.RequestID)
	switch args.Op {
	case OpPut:
		kv.store[args.Key] = args.Value
	case OpAppend:
		kv.store[args.Key] = fmt.Sprintf("%s%s", kv.store[args.Key], args.Value)
	}
}

func (kv *KVServer) Record(requestID string) {
	kv.record[requestID] = struct{}{}
}

func (kv *KVServer) Recorded(requestID string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, ok := kv.record[requestID]
	return ok
}

// Kill
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) listen() {
	for !kv.killed() {
		result := <-kv.applyCh
		op := kv.getOP(result)
		if kv.isLeader() {
			DPrintf("[KVServer.listen] Leader[%d] has commit %+v", kv.me, result)
			kv.leaderCh <- result
			continue
		}
		kv.mu.Lock()
		DPrintf("[KVServer.listen] KV[%d] received result: %+v", kv.me, result)
		switch op.Op {
		case OpPut:
			kv.store[op.Key] = op.Value
		case OpAppend:
			kv.store[op.Key] = fmt.Sprintf("%s%s", kv.store[op.Key], op.Value)
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) getOP(applyMsg raft.ApplyMsg) Op {
	command := applyMsg.Command
	if result, ok := command.(Op); !ok {
	} else {
		return result
	}

	return Op{}
}

func (kv *KVServer) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

// StartKVServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	ch := make(chan raft.ApplyMsg)
	kv := &KVServer{
		mu:           sync.Mutex{},
		me:           me,
		rf:           raft.Make(servers, me, persister, ch),
		applyCh:      ch,
		leaderCh:     make(chan raft.ApplyMsg, 0),
		maxraftstate: maxraftstate,
		store:        make(map[string]string, 0),
		record:       make(map[string]struct{}, 0),
	}

	go kv.listen()

	// You may need initialization code here.

	return kv
}
