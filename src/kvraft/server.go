package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
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
	Op      string
	Key     string
	Value   string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	store map[string]string
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	reply.Err = OK

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	if _, _, isLeader = kv.rf.Start(Op{
		Op:    OpGet,
		Key:   args.Key,
	}); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	<-kv.applyCh
	value, ok := kv.store[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		return
	}
	reply.Value = value
	return
	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//DPrintf("[KVServer.PubAppend] KV[%d] received ")
	reply.Err = OK
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	if _, _, isLeader = kv.rf.Start(Op{
		Op:    args.Op,
		Key:   args.Key,
		Value: args.Value,
	}); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	<-kv.applyCh
	switch args.Op {
	case OpPut:
		kv.store[args.Key] = args.Value
	case OpAppend:
		kv.store[args.Key] = fmt.Sprintf("%s%s", kv.store[args.Key], args.Value)
	}
	kv.mu.Unlock()
	// Your code here.
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
		result := <- kv.applyCh

		op := kv.getOP(result)
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.applyCh <- result
			continue
		}

		kv.mu.Lock()
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
		DPrintf("[KV.getOp]KV[%d] getOP: %v", kv.me, result)
		return result
	}

	return Op{}
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
		maxraftstate: maxraftstate,
		store:        make(map[string]string, 0),
	}

	go kv.listen()

	// You may need initialization code here.

	return kv
}
