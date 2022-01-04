package shardkv

import (
	"6.824/shardctrler"
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

type Op struct { // Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op           string
	Key          string
	Value        string
	ClientID     int64
	CommandIndex int
	RequestID    int64
	Config       shardctrler.Config
	Store        map[int]Store
}

//var Debug = false
var Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Store map[string]string

func (s Store) Copy() map[string]string {
	result := make(map[string]string, len(s))
	for k, v := range s {
		result[k] = v
	}

	return result
}

type ShardKV struct {
	mu           sync.Mutex
	mmu          sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	commandCh    chan Op
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	shardCtrler  *shardctrler.Clerk
	maxraftstate int // snapshot if log grows this big

	config    shardctrler.Config
	persister *raft.Persister

	store        map[int]Store
	record       map[int64]int64
	commandIndex *int64 // record the index which is expected by the leader
	executeIndex *int64 // record the latest index of command which has been executed by server
	// Your definitions here.
}

func (kv *ShardKV) isKeyAvailable(key string) bool {
	return kv.config.Shards[key2shard(key)] == kv.gid
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[ShardKV.Get] KV[%d] tries to get key: %v", kv.me, args.Key)
	reply.Err = OK

	if !kv.isKeyAvailable(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}

	shard := key2shard(args.Key)
	index, term, isLeader := kv.rf.Start(Op{
		Op:       OpGet,
		Key:      args.Key,
		ClientID: args.ClientID,
	})
	if !isLeader {
		DPrintf("[ShardKV.Get] KV[%d] is not a leader", kv.me)
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("[ShardKV.Get] KV[%d] start to replicate command %+v. index = %d, term = %d", kv.me, args, index, term)
	kv.SetCommandIndex(index)

	for {
		var result Op
		if kv.isLostLeadership(int64(term)) {
			DPrintf("[ShardKV.Get] After start %+v, KV[%d] is no longer a leader", args, kv.me)
			reply.Err = ErrWrongLeader
			return
		}

		select {
		case result = <-kv.commandCh:
			DPrintf("[ShardKV.Get] KV[%d] index = %d args = %+v, applyMsg = %+v", kv.me, index, args, result)

		case <-time.After(Interval):
			DPrintf("[ShardKV.Get] KV[%d] wait 200 msec", kv.me)
			continue
		}

		if kv.isLeader() {
			value, ok := kv.store[shard][args.Key]
			DPrintf("[ShardKV.Get] KV[%d] get key: %s, value: %s", kv.me, args.Key, value)
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[ShardKV.PutAppend] KV[%d] received %+v", kv.me, args)
	reply.Err = OK

	if kv.Recorded(args.ClientID, args.RequestID) {
		DPrintf("[ShardKV.PutAppend] KV[%d] %d request has already executed.", kv.me, args.RequestID)
		return
	}

	DPrintf("[ShardKV.PutAppend] KV[%d] ready to send %+v to raft", kv.me, args)
	index, term, isLeader := kv.rf.Start(Op{
		Op:        args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("[ShardKV.PutAppend] KV[%d] failed to start because server is not a leader", kv.me)
		return
	}
	DPrintf("[ShardKV.PutAppend] KV[%d] start to replicate command %+v. index = %d, term = %d", kv.me, args, index, term)
	kv.SetCommandIndex(index)

	for {
		var result Op
		if kv.isLostLeadership(int64(term)) {
			reply.Err = ErrWrongLeader
			return
		}

		select {
		case result = <-kv.commandCh:
			DPrintf("[ShardKV.PutAppend] KV[%d] index = %d args = %+v, applyMsg = %+v", kv.me, index, args, result)
		case <-time.After(Interval):
			DPrintf("[ShardKV.PutAppend] KV[%d] wait 200 msec", kv.me)
			continue
		}

		if kv.isLeader() {
			DPrintf("[ShardKV.PutAppend] KV[%d] index = %d, try to modify the store, args = %+v, before modify: [%s:%s]", kv.me, index, result, args.Key, kv.GetValueWithRLock(args.Key))
			kv.doPutAppend(args)
			DPrintf("[ShardKV.PutAppend] KV[%d] index = %d, after modify: [%s:%s]", kv.me, index, args.Key, kv.GetValueWithRLock(args.Key))

		} else {
			DPrintf("[ShardKV.PutAppend] KV[%d] now is no longer leader.", kv.me)
			reply.Err = ErrWrongLeader
		}
		return
	}
	// Your code here.
}

func (kv *ShardKV) isLostLeadership(term int64) bool {
	if !kv.isLeader() {
		DPrintf("[ShardKV.isLostLeadership] KV[%d] is not a leader", kv.me)
		return true
	}

	if term != kv.rf.CurrentTerm() {
		DPrintf("[ShardKV.isLostLeadership] KV[%d]'s term changed from %d to %d", kv.me, term, kv.rf.CurrentTerm())
		return true
	}

	return false
}

func (kv *ShardKV) doPutAppend(args *PutAppendArgs) {
	kv.mmu.Lock()
	defer kv.mmu.Unlock()
	if kv.Recorded(args.ClientID, args.RequestID) {
		return
	}

	switch args.Op {
	case OpPut:
		kv.SetValue(args.Key, args.Value)
	case OpAppend:
		kv.SetValue(args.Key, fmt.Sprintf("%s%s", kv.store[key2shard(args.Key)][args.Key], args.Value))
	}
	kv.Record(args.ClientID, args.RequestID)
}

func (kv *ShardKV) tryDoPutAppend(op Op) {
	kv.doPutAppend(&PutAppendArgs{
		Key:       op.Key,
		Value:     op.Value,
		Op:        op.Op,
		RequestID: op.RequestID,
		ClientID:  op.ClientID,
	})
}

func (kv *ShardKV) Record(clientID, requestID int64) {
	kv.record[clientID] = requestID
}

func (kv *ShardKV) Recorded(clientID, requestID int64) bool {
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	result, ok := kv.record[clientID]
	recorded := ok && result >= requestID
	DPrintf("[ShardKV.Recorded] KV[%d] check whether client: %d has send request: %d, result: %v", kv.me, clientID, requestID, recorded)
	return recorded
}

// Kill
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	Debug = false
	// Your code here, if desired.
}

func (kv *ShardKV) listen() {
	for result := range kv.applyCh {

		DPrintf("[ShardKV.listen] KV[%d] received applyMsg: %+v", kv.me, result)

		if result.SnapshotValid {
			kv.tryInstallSnapshot(result)
			continue
		}
		op := kv.getOP(result)
		op.CommandIndex = result.CommandIndex
		if kv.isLeader() {
			DPrintf("[ShardKV.listen] Leader[%d] has commit %+v, commend index = %d", kv.me, result, kv.CommandIndex())
			kv.TrySetExecuteIndex(op.CommandIndex)

			if op.CommandIndex == kv.CommandIndex() {
				DPrintf("[ShardKV.listen] Leader[%d] send command:%+v to chan", kv.me, op)
				if op.Op != "" {
					kv.commandCh <- op
					go kv.trySnapshot(op.CommandIndex)
				}
			}

			kv.tryExecute(op)
			continue
		}
		kv.slaveConsist(op)
		go kv.trySnapshot(op.CommandIndex)
	}
}

func (kv *ShardKV) tryExecute(op Op) {
	if op.CommandIndex == kv.ExecuteIndex() {
		if op.Op == "" {
			kv.tryReShard(op)
		} else if op.Op != OpGet {
			kv.tryDoPutAppend(op)
		}
	}
}

func (kv *ShardKV) tryReShard(op Op) {
	if op.Config.NewerThan(kv.config) {
		kv.config = op.Config
		for shardID, store := range op.Store {
			kv.store[shardID] = store.Copy()
		}
	}
}

func (kv *ShardKV) slaveConsist(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("[ShardKV.slaveConsist] KV[%d] execute index: %d, op: %+v", kv.me, kv.ExecuteIndex(), op)
	kv.TrySetExecuteIndex(op.CommandIndex)

	DPrintf("[ShardKV.slaveConsist] KV[%d] received op: %+v, before modify: [%s:%v]", kv.me, op, op.Key, kv.GetValueWithRLock(op.Key))
	kv.tryExecute(op)
	DPrintf("[ShardKV.slaveConsist] KV[%d] after modify: [%s:%v]", kv.me, op.Key, kv.GetValueWithRLock(op.Key))

}

func (kv *ShardKV) GetValueWithRLock(key string) string {
	kv.mmu.RLock()
	defer kv.mmu.RUnlock()

	shard := key2shard(key)
	result, ok := kv.store[shard][key]
	if !ok {
		return ""
	}
	return result
}

func (kv *ShardKV) SetValue(key, value string) {
	shard := key2shard(key)
	if _, ok := kv.store[shard]; !ok {
		kv.store[shard] = make(map[string]string)
	}
	kv.store[shard][key] = value
}

func (kv *ShardKV) getOP(applyMsg raft.ApplyMsg) Op {
	command := applyMsg.Command
	if result, ok := command.(Op); !ok {
	} else {
		return result
	}

	return Op{}
}

func (kv *ShardKV) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv *ShardKV) CommandIndex() int {
	return int(atomic.LoadInt64(kv.commandIndex))
}

func (kv *ShardKV) SetCommandIndex(set int) {
	DPrintf("[ShardKV.SetCommandIndex] KV[%d] changes command index from %d to %d", kv.me, kv.CommandIndex(), set)
	atomic.StoreInt64(kv.commandIndex, int64(set))
}

func (kv *ShardKV) ExecuteIndex() int {
	return int(atomic.LoadInt64(kv.executeIndex))
}

func (kv *ShardKV) TrySetExecuteIndex(set int) {
	DPrintf("[ShardKV.TrySetExecuteIndex] KV[%d] tries to change execute index from %d to %d", kv.me, kv.ExecuteIndex(), set)
	atomic.CompareAndSwapInt64(kv.executeIndex, int64(set-1), int64(set))
	DPrintf("[ShardKV.TrySetExecuteIndex] KV[%d] after changed: executeIndex: %d", kv.me, kv.ExecuteIndex())
}

func (kv *ShardKV) tryRecoverFromSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("[ShardKV.tryRecoverFromSnapshot] KV[%d] tries to recover from snapshot", kv.me)
	kv.installSnapshot(kv.persister.ReadSnapshot())
}

type Snapshot struct {
	Store        map[int]Store
	Record       map[int64]int64
	CommandIndex int64
	ExecuteIndex int64
}

func (kv *ShardKV) trySnapshot(commandIndex int) {
	if kv.maxraftstate == -1 {
		return
	}

	if kv.shouldSnapshot() {
		kv.mu.Lock()
		kv.rf.Snapshot(commandIndex, kv.snapshotBytes())
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) shouldSnapshot() bool {
	size := kv.persister.RaftStateSize()
	result := kv.maxraftstate <= size
	DPrintf("[ShardKV.shouldSnapshot] KV[%d] check whether it's the time to snapshot, size: %d, maxraftstate: %d, result: %v", kv.me, size, kv.maxraftstate, result)
	return result
}

func (kv *ShardKV) snapshotBytes() []byte {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	p := Snapshot{
		Store:        kv.store,
		Record:       kv.record,
		CommandIndex: *kv.commandIndex,
		ExecuteIndex: *kv.executeIndex,
	}
	if err := encoder.Encode(p); err != nil {
		panic(fmt.Sprintf("Failed to encode persistentState, err = %s", err))
	}
	return buffer.Bytes()
}

func (kv *ShardKV) tryInstallSnapshot(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("[ShardKV.tryInstallSnapshot] KV[%d] tries to install snapshot: %+v", kv.me, msg)
	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		kv.installSnapshot(msg.Snapshot)
	}
}

func (kv *ShardKV) installSnapshot(data []byte) {
	DPrintf("[ShardKV.installSnapshot] KV[%d] tries to install snapshot: %s", kv.me, string(data))
	if data == nil || len(data) == 0 {
		return
	}

	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)

	snapshot := new(Snapshot)
	if err := decoder.Decode(snapshot); err != nil {
		panic(fmt.Sprintf("Failed to read persist, err = %s", err))
	}

	DPrintf("[ShardKV.installSnapshot] KV[%d].executeIndex: %d received snapshot: %+v", kv.me, kv.ExecuteIndex(), snapshot)
	if kv.ExecuteIndex() <= int(snapshot.ExecuteIndex) {
		kv.store = snapshot.Store
		kv.record = snapshot.Record
		kv.commandIndex = &snapshot.CommandIndex
		kv.executeIndex = &snapshot.ExecuteIndex
	}
}

func (kv *ShardKV) syncConfiguration() {
	for {
		if kv.isLeader() {
			newConfig := kv.shardCtrler.Query(-1)

			DPrintf("[ShardKV.syncConfiguration] KV[%d] gets config: %+v", kv.me, newConfig)
			kv.mu.Lock()
			if kv.isLeader() && newConfig.NewerThan(kv.config) {
				DPrintf("[ShardKV.syncConfiguration] KV[%d] receives a newer config: %+v, old config: %+v", kv.me, newConfig, kv.config)
				old := kv.config
				kv.config = newConfig
				if kv.isLeader() {
					kv.updateShard(old)
				}
			}
			kv.mu.Unlock()

			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (kv *ShardKV) updateShard(oldConfig shardctrler.Config) {
	DPrintf("[ShardKV]KV[%d] ready to update shard, new config: %+v, old config: %+v", kv.me, kv.config, oldConfig)
	shards := kv.shardObtained(oldConfig, kv.config)
	if len(shards) == 0 {
		return
	}

	for i := 0; i < len(shards); i++ {
		if !kv.applyShardForReplica(shards[i].ShardID, oldConfig.Groups[shards[i].OriginGID]) {
			return
		}
	}
	return
}

func (kv *ShardKV) applyShardForReplica(shardID int, replicas []string) bool {
	args := MigrateArgs{
		Shard:  shardID,
		Config: kv.config,
	}

	for _, replica := range replicas {
		var reply MigrateReply
		ok := kv.make_end(replica).Call(MethodShardKVMigrate, &args, &reply)
		if ok && reply.Err == OK {
			kv.store[shardID] = reply.Store.Copy()
			index, _, isLeader := kv.rf.Start(Op{
				Config: kv.config,
				Store: map[int]Store{
					shardID: reply.Store.Copy(),
				},
			})

			if !isLeader {
				DPrintf("[ShardKV.updateShard] KV[%d] is no longer a leader", kv.me)
				return false
			}

			kv.SetCommandIndex(index)
			return true
		}
	}

	return false
}

type Shard struct {
	OriginGID int
	ShardID   int
}

func (kv *ShardKV) shardObtained(oldConfig, newConfig shardctrler.Config) []Shard {
	result := make([]Shard, 0)
	for i := 0; i < len(oldConfig.Shards); i++ {
		if newConfig.Shards[i] == kv.gid && oldConfig.Shards[i] != 0 && newConfig.Shards[i] != oldConfig.Shards[i] {
			result = append(result, Shard{
				OriginGID: oldConfig.Shards[i],
				ShardID:   i,
			})
		}
	}

	return result
}

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("[ShardKV.Migrate] KV[%d] received %+v", kv.me, args)
	reply.Err = OK

	if kv.config.Num > args.Config.Num {
		reply.Err = ErrInvalidConfig
		return
	}

	kv.config = args.Config
	if _, ok := kv.store[args.Shard]; ok {
		reply.Store = kv.store[args.Shard].Copy()
	}

	index, _, isLeader := kv.rf.Start(Op{
		Config: kv.config,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.SetCommandIndex(index)
	return
}

// StartServer
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	applyCh := make(chan raft.ApplyMsg)
	kv := &ShardKV{
		me:           me,
		rf:           raft.Make(servers, me, persister, applyCh),
		applyCh:      applyCh,
		make_end:     make_end,
		gid:          gid,
		shardCtrler:  shardctrler.MakeClerk(ctrlers),
		maxraftstate: maxraftstate,
		commandCh:    make(chan Op),
		store:        make(map[int]Store, 0),
		record:       make(map[int64]int64, 0),
		commandIndex: new(int64),
		executeIndex: new(int64),
		persister:    persister,
		config:       shardctrler.Config{},
	}

	kv.tryRecoverFromSnapshot()
	go kv.listen()
	go kv.syncConfiguration()
	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	return kv
}
