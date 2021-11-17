package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

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
	ClientID     int64
	CommandIndex int
	RequestID    int64
}

type KVServer struct {
	mu       sync.Mutex
	mmu      sync.RWMutex
	me       int
	rf       *raft.Raft
	applyCh     chan raft.ApplyMsg
	commandCh  chan Op
	dead        int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister   *raft.Persister

	store  map[string]string
	record map[int64]int64
	commandIndex     *int64 // record the index which is expected by the leader
	executeIndex     *int64 // record the latest index of command which has been executed by server
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[KVServer.Get] KV[%d] tries to get key: %v", kv.me, args.Key)
	reply.Err = OK

	index, term, isLeader := kv.rf.Start(Op{
		Op:  OpGet,
		Key: args.Key,
		ClientID: args.ClientID,
	})
	if !isLeader {
		DPrintf("[KVServer.Get] KV[%d] is not a leader", kv.me)
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("[KVServer.Get] KV[%d] start to replicate command %+v. index = %d, term = %d", kv.me, args, index, term)
	kv.SetCommandIndex(index)

	for {
		var result Op
		if kv.isLostLeadership(int64(term)) {
			DPrintf("[KVServer.Get] After start %+v, KV[%d] is no longer a leader", args, kv.me)
			reply.Err = ErrWrongLeader
			return
		}

		select {
			case result = <-kv.commandCh:
				DPrintf("[KVServer.Get] KV[%d] index = %d args = %+v, applyMsg = %+v", kv.me, index, args, result)

			case <- time.After(Interval):
				DPrintf("[KVServer.Get] KV[%d] wait 200 msec", kv.me)
				continue
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
	}
	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[KVServer.PutAppend] KV[%d] received %+v", kv.me, args)
	reply.Err = OK

	if kv.Recorded(args.ClientID, args.RequestID) {
		DPrintf("[KVServer.PutAppend] KV[%d] %d request has already executed.", kv.me, args.RequestID)
		return
	}

	DPrintf("[KVServer.PutAppend] KV[%d] ready to send %+v to raft", kv.me, args)
	index, term, isLeader := kv.rf.Start(Op{
		Op:    args.Op,
		Key:   args.Key,
		Value: args.Value,
		ClientID: args.ClientID,
		RequestID: args.RequestID,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("[KVServer.PutAppend] KV[%d] failed to start because server is not a leader", kv.me)
		return
	}
	DPrintf("[KVServer.PutAppend] KV[%d] start to replicate command %+v. index = %d, term = %d", kv.me, args, index, term)
	kv.SetCommandIndex(index)

	for {
		var result Op
		if kv.isLostLeadership(int64(term)) {
			reply.Err = ErrWrongLeader
			return
		}

		select {
		case result = <-kv.commandCh:
			DPrintf("[KVServer.PutAppend] KV[%d] index = %d args = %+v, applyMsg = %+v", kv.me, index, args, result)
		case <- time.After(Interval):
			DPrintf("[KVServer.PutAppend] KV[%d] wait 200 msec", kv.me)
			continue
		}

		if kv.isLeader() {
			DPrintf("[KVServer.PutAppend] KV[%d] index = %d, try to modify the store, args = %+v, before modify: [%s:%s]", kv.me, index, result, args.Key, kv.GetValueWithRLock(args.Key))
			kv.doPutAppend(args)
			DPrintf("[KVServer.PutAppend] KV[%d] index = %d, after modify: [%s:%s]", kv.me, index, args.Key, kv.GetValueWithRLock(args.Key))

		} else {
			DPrintf("[KVServer.PutAppend] KV[%d] now is no longer leader.", kv.me)
			reply.Err = ErrWrongLeader
		}
		return
	}
	// Your code here.
}

func (kv *KVServer) isLostLeadership(term int64) bool {
	if !kv.isLeader() {
		DPrintf("[KVServer.isLostLeadership] KV[%d] is not a leader", kv.me)
		return true
	}

	if term != kv.rf.CurrentTerm() {
		DPrintf("[KVServer.isLostLeadership] KV[%d]'s term changed from %d to %d", kv.me, term, kv.rf.CurrentTerm())
		return true
	}

	return false
}

func (kv *KVServer) doPutAppend(args *PutAppendArgs) {
	kv.mmu.Lock()
	defer kv.mmu.Unlock()
	if kv.Recorded(args.ClientID, args.RequestID) {
		return
	}
	switch args.Op {
	case OpPut:
		kv.SetValue(args.Key, args.Value)
	case OpAppend:
		kv.SetValue(args.Key, fmt.Sprintf("%s%s", kv.store[args.Key], args.Value))
	}
	kv.Record(args.ClientID, args.RequestID)
}

func (kv *KVServer) tryDoPutAppend(op Op) {
	kv.doPutAppend(&PutAppendArgs{
		Key:       op.Key,
		Value:     op.Value,
		Op:        op.Op,
		RequestID: op.RequestID,
		ClientID:  op.ClientID,
	})
}

func (kv *KVServer) Record(clientID, requestID int64) {
	kv.record[clientID] = requestID
}

func (kv *KVServer) Recorded(clientID, requestID int64) bool {
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	result, ok := kv.record[clientID]
	recorded := ok && result >= requestID
	DPrintf("[KVServer.Recorded] KV[%d] check whether client: %d has send request: %d, result: %v", kv.me, clientID, requestID, recorded)
	return recorded
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
		DPrintf("[KVServer.listen] KV[%d] received applyMsg: %+v", kv.me, result)

		if result.SnapshotValid {
			kv.tryInstallSnapshot(result)
			continue
		}
		op := kv.getOP(result)
		op.CommandIndex = result.CommandIndex
		if kv.isLeader() {
			DPrintf("[KVServer.listen] Leader[%d] has commit %+v, commend index = %d", kv.me, result, kv.CommandIndex())
			kv.TrySetExecuteIndex(op.CommandIndex)

			if op.CommandIndex == kv.CommandIndex() {
				DPrintf("[KVServer.listen] Leader[%d] send command:%+v to chan", kv.me, op)
				kv.commandCh <- op
				go kv.trySnapshot(op.CommandIndex)
			}

			kv.tryExecute(op)
			continue
		}
		kv.slaveConsist(op)
		go kv.trySnapshot(op.CommandIndex)
	}
}

func (kv *KVServer) tryExecute(op Op) {
	if op.CommandIndex == kv.ExecuteIndex() {
		if op.Op != OpGet {
			kv.tryDoPutAppend(op)
		}
	}
}

func (kv *KVServer) slaveConsist(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("[KVServer.slaveConsist] KV[%d] execute index: %d, op: %+v", kv.me, kv.ExecuteIndex(), op)
	kv.TrySetExecuteIndex(op.CommandIndex)

	DPrintf("[KVServer.slaveConsist] KV[%d] received op: %+v, before modify: [%s:%v]", kv.me, op, op.Key, kv.GetValueWithRLock(op.Key))
	kv.tryExecute(op)
	DPrintf("[KVServer.slaveConsist] KV[%d] after modify: [%s:%v]", kv.me, op.Key, kv.GetValueWithRLock(op.Key))

}

func (kv *KVServer) GetValueWithRLock(key string) string {
	kv.mmu.RLock()
	defer kv.mmu.RUnlock()

	result, ok := kv.store[key]
	if !ok {
		return ""
	}
	return result
}

func (kv *KVServer) SetValue(key, value string) {
	kv.store[key] = value
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

func (kv *KVServer) CommandIndex() int {
	return int(atomic.LoadInt64(kv.commandIndex))
}

func (kv *KVServer) SetCommandIndex(set int) {
	DPrintf("[KVServer.SetCommandIndex] KV[%d] changes command index from %d to %d", kv.me, kv.CommandIndex(), set)
	atomic.StoreInt64(kv.commandIndex, int64(set))
}

func (kv *KVServer) ExecuteIndex() int {
	return int(atomic.LoadInt64(kv.executeIndex))
}

func (kv *KVServer) TrySetExecuteIndex(set int) {
	DPrintf("[KVServer.TrySetExecuteIndex] KV[%d] tries to change execute index from %d to %d", kv.me, kv.ExecuteIndex(), set)
	atomic.CompareAndSwapInt64(kv.executeIndex, int64(set-1), int64(set))
	DPrintf("[KVServer.TrySetExecuteIndex] KV[%d] after changed: executeIndex: %d", kv.me, kv.ExecuteIndex())
}

func (kv *KVServer) tryRecoverFromSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.installSnapshot(kv.persister.ReadSnapshot())
}

type Snapshot struct {
	Store           map[string]string
	Record          map[int64]int64
	CommandIndex    int64
	ExecuteIndex    int64
}

func (kv *KVServer) trySnapshot(commandIndex int) {
	if kv.maxraftstate == -1 {
		return
	}

	if kv.shouldSnapshot() {
		kv.mu.Lock()
		kv.rf.Snapshot(commandIndex, kv.snapshotBytes())
		kv.mu.Unlock()
	}
}

func (kv *KVServer) shouldSnapshot() bool {
	size := kv.persister.RaftStateSize()
	result := kv.maxraftstate <= size
	DPrintf("[KVServer.shouldSnapshot] KV[%d] check whether it's the time to snapshot, size: %d, maxraftstate: %d, result: %v", kv.me, size, kv.maxraftstate, result)
	return result
}

func (kv *KVServer) snapshotBytes() []byte {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	p := Snapshot{
		Store: kv.store,
		Record: kv.record,
		CommandIndex: *kv.commandIndex,
		ExecuteIndex: *kv.executeIndex,
	}
	if err := encoder.Encode(p); err != nil {
		panic(fmt.Sprintf("Failed to encode persistentState, err = %s", err))
	}
	return buffer.Bytes()
}

func (kv *KVServer) tryInstallSnapshot(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("[KVServer.tryInstallSnapshot] KV[%d] tries to install snapshot: %+v", kv.me, msg)
	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		kv.installSnapshot(msg.Snapshot)
	}
}

func (kv *KVServer) installSnapshot(data []byte) {
	DPrintf("[KVServer.installSnapshot] KV[%d] tries to install snapshot: %v", kv.me, data)
	if data == nil || len(data) == 0 {
		return
	}

	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)

	snapshot := new(Snapshot)
	if err := decoder.Decode(snapshot); err != nil {
		panic(fmt.Sprintf("Failed to read persist, err = %s", err))
	}

	DPrintf("[KVServer.installSnapshot] KV[%d].executeIndex: %d received snapshot: %+v", kv.me, kv.ExecuteIndex(), snapshot)
	if *kv.executeIndex <= snapshot.ExecuteIndex {
		kv.store = snapshot.Store
		kv.record = snapshot.Record
		kv.commandIndex = &snapshot.CommandIndex
		kv.executeIndex = &snapshot.ExecuteIndex
	}
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
		mmu:          sync.RWMutex{},
		me:           me,
		rf:           raft.Make(servers, me, persister, ch),
		applyCh:      ch,
		commandCh:    make(chan Op, 0),
		maxraftstate: maxraftstate,
		store:        make(map[string]string, 0),
		record:       make(map[int64]int64, 0),
		commandIndex: new(int64),
		executeIndex: new(int64),
		persister: persister,
	}

	kv.tryRecoverFromSnapshot()
	go kv.listen()

	// You may need initialization code here.

	return kv
}
