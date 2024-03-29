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
	Info         Info
}

func (o Op) isUpdateConfigOp() bool {
	return o.Config.Num != 0
}

func (o Op) isValidStore() bool {
	return o.Info.Store != nil && o.Info.Record != nil
}

//var Debug = false

var Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Record map[int64]int64

func (r Record) Copy() map[int64]int64 {
	result := make(map[int64]int64, len(r))
	for k, v := range r {
		result[k] = v
	}

	return result
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
	smu          sync.RWMutex
	cmu          sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	commandCh    chan Op
	makeEnd      func(string) *labrpc.ClientEnd
	gid          int
	shardCtrler  *shardctrler.Clerk
	maxraftstate int // snapshot if log grows this big

	config    shardctrler.Config
	persister *raft.Persister

	configChan  chan shardctrler.Config
	resharding  *int64
	reshardList []int

	store        map[int]Store
	record       map[int]Record
	commandIndex *int64 // record the index which is expected by the leader
	executeIndex *int64 // record the latest index of command which has been executed by server
	// Your definitions here.
}

func (kv *ShardKV) isKeyAvailable(key string) bool {
	kv.cmu.RLock()
	gid := kv.config.Shards[key2shard(key)]
	kv.cmu.RUnlock()
	DPrintf("[ShardKV.isKeyAvailable] KV[gid:%d, %d] config: %+v, key: %s belongs to shard %d", kv.gid, kv.me, kv.config, key, key2shard(key))
	return gid == kv.gid && kv.isLatestConfig()
}

func (kv *ShardKV) isLatestConfig() bool {
	return kv.configNum() == kv.shardCtrler.Query(-1).Num
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[ShardKV.Get] KV[gid:%d, %d] tries to get key: %v", kv.gid, kv.me, args.Key)
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
		DPrintf("[ShardKV.Get] KV[gid:%d, %d] is not a leader", kv.gid, kv.me)
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("[ShardKV.Get] KV[gid:%d, %d] start to replicate command %+v. index = %d, term = %d", kv.gid, kv.me, args, index, term)
	kv.SetCommandIndex(index)

	for {
		var result Op
		if kv.isLostLeadership(int64(term)) {
			DPrintf("[ShardKV.Get] After start %+v, KV[gid:%d, %d] is no longer a leader", args, kv.gid, kv.me)
			reply.Err = ErrWrongLeader
			return
		}

		select {
		case result = <-kv.commandCh:
			DPrintf("[ShardKV.Get] KV[gid:%d, %d] index = %d args = %+v, applyMsg = %+v", kv.gid, kv.me, index, args, result)

		case <-time.After(Interval):
			DPrintf("[ShardKV.Get] KV[gid:%d, %d] wait 200 msec", kv.gid, kv.me)
			continue
		}

		if kv.isLeader() {
			value, ok := kv.store[shard][args.Key]
			DPrintf("[ShardKV.Get] KV[gid:%d, %d] get key: %s, value: %s", kv.gid, kv.me, args.Key, value)
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
	DPrintf("[ShardKV.PutAppend] KV[gid:%d, %d] received %+v", kv.gid, kv.me, args)
	reply.Err = OK

	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	if !kv.isKeyAvailable(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}

	if kv.Recorded(args.ClientID, args.RequestID, args.Key) {
		DPrintf("[ShardKV.PutAppend] KV[gid:%d, %d] %d request has already executed.", kv.gid, kv.me, args.RequestID)
		return
	}

	DPrintf("[ShardKV.PutAppend] KV[gid:%d, %d] ready to send %+v to raft", kv.gid, kv.me, args)
	index, term, isLeader := kv.rf.Start(Op{
		Op:        args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	})
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("[ShardKV.PutAppend] KV[gid:%d, %d] failed to start because server is not a leader", kv.gid, kv.me)
		return
	}
	DPrintf("[ShardKV.PutAppend] KV[gid:%d, %d] start to replicate command %+v. index = %d, term = %d", kv.gid, kv.me, args, index, term)
	kv.SetCommandIndex(index)

	for {
		var result Op
		if kv.isLostLeadership(int64(term)) {
			reply.Err = ErrWrongLeader
			return
		}

		select {
		case result = <-kv.commandCh:
			DPrintf("[ShardKV.PutAppend] KV[gid:%d, %d] index = %d args = %+v, applyMsg = %+v", kv.gid, kv.me, index, args, result)
		case <-time.After(Interval):
			DPrintf("[ShardKV.PutAppend] KV[gid:%d, %d] wait 200 msec", kv.gid, kv.me)
			continue
		}

		if kv.isLeader() {
			//DPrintf("[ShardKV.PutAppend] KV[gid:%d, %d] index = %d, try to modify the store, args = %+v, before modify: [%s:%s]", kv.gid, kv.me, index, result, args.Key, kv.GetValueWithRLock(args.Key))
			//kv.doPutAppend(args)
			DPrintf("[ShardKV.PutAppend] KV[gid:%d, %d] index = %d, after modify: [%s:%s]", kv.gid, kv.me, index, args.Key, kv.GetValueWithRLock(args.Key))

		} else {
			DPrintf("[ShardKV.PutAppend] KV[gid:%d, %d] now is no longer leader.", kv.gid, kv.me)
			reply.Err = ErrWrongLeader
		}
		return
	}
	// Your code here.
}

func (kv *ShardKV) isLostLeadership(term int64) bool {
	if !kv.isLeader() {
		DPrintf("[ShardKV.isLostLeadership] KV[gid:%d, %d] is not a leader", kv.gid, kv.me)
		return true
	}

	if term != kv.rf.CurrentTerm() {
		DPrintf("[ShardKV.isLostLeadership] KV[gid:%d, %d]'s term changed from %d to %d", kv.gid, kv.me, term, kv.rf.CurrentTerm())
		return true
	}

	return false
}

func (kv *ShardKV) doPutAppend(args *PutAppendArgs) {
	kv.mmu.Lock()
	defer kv.mmu.Unlock()
	DPrintf("[ShardKV.doPutAppend] KV[gid:%d, %d] tries to do put append: %+v", kv.gid, kv.me, args)
	if kv.Recorded(args.ClientID, args.RequestID, args.Key) {
		return
	}

	DPrintf("[ShardKV.doPutAppend] KV[gid:%d, %d] before modify: [%s:%s]", kv.gid, kv.me, args.Key, kv.store[key2shard(args.Key)][args.Key])
	switch args.Op {
	case OpPut:
		kv.SetValue(args.Key, args.Value)
	case OpAppend:
		kv.SetValue(args.Key, fmt.Sprintf("%s%s", kv.store[key2shard(args.Key)][args.Key], args.Value))
	}

	DPrintf("[ShardKV.doPutAppend] KV[gid:%d, %d] after modify {Op: %v, Value: %v}: [%s:%s]", kv.gid, kv.me, args.Op, args.Value, args.Key, kv.store[key2shard(args.Key)][args.Key])
	kv.Record(args.ClientID, args.RequestID, args.Key)
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

func (kv *ShardKV) Record(clientID, requestID int64, key string) {
	if _, ok := kv.record[key2shard(key)]; !ok {
		kv.record[key2shard(key)] = make(Record, 0)
	}
	kv.record[key2shard(key)][clientID] = requestID
}

func (kv *ShardKV) Recorded(clientID, requestID int64, key string) bool {
	//kv.mu.Lock()
	//defer kv.mu.Unlock()
	result, ok := kv.record[key2shard(key)][clientID]
	recorded := ok && result >= requestID
	DPrintf("[ShardKV.Recorded] KV[gid:%d, %d] check whether client: %d has send request: %d, result: %v", kv.gid, kv.me, clientID, requestID, recorded)
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
	// Your code here, if desired.
}

func (kv *ShardKV) listen() {
	for result := range kv.applyCh {

		DPrintf("[ShardKV.listen] KV[gid:%d, %d] received applyMsg: %+v", kv.gid, kv.me, result)

		if result.SnapshotValid {
			kv.tryInstallSnapshot(result)
			continue
		}

		op := kv.getOP(result)
		op.CommandIndex = result.CommandIndex
		if kv.isLeader() {
			DPrintf("[ShardKV.listen] Leader[gid:%d, %d] has commit %+v, commend index = %d", kv.gid, kv.me, result, kv.CommandIndex())
			kv.TrySetExecuteIndex(op.CommandIndex)

			kv.tryExecute(op)
			if op.CommandIndex == kv.CommandIndex() {
				DPrintf("[ShardKV.listen] Leader[gid:%d, %d] send command:%+v to chan", kv.gid, kv.me, op)
				kv.commandCh <- op
				go kv.trySnapshot(op.CommandIndex)
			}
			continue
		}
		kv.slaveConsist(op)
		go kv.trySnapshot(op.CommandIndex)
	}
}

func (kv *ShardKV) tryExecute(op Op) {
	DPrintf("[ShardKV.tryExecute] KV[gid:%d, %d] received op: %+v, executeIndex: %d", kv.gid, kv.me, op, kv.ExecuteIndex())
	if op.CommandIndex == kv.ExecuteIndex() {
		DPrintf("[ShardKV.tryExecute] KV[gid:%d, %d] tries to execute op: %+v", kv.gid, kv.me, op)
		if op.isUpdateConfigOp() {
			kv.tryReShard(op)
		}

		if op.isValidStore() {
			kv.tryUpdateStore(op)
		}

		if op.Op == OpPut || op.Op == OpAppend {
			kv.tryDoPutAppend(op)
		}
	}
}

func (kv *ShardKV) tryReShard(op Op) {
	DPrintf("[ShardKV.tryReShard] KV[gid:%d, %d] tries to re shard, op: %+v", kv.gid, kv.me, op)
	if op.Config.NewerThan(kv.configNum()) {
		kv.cmu.Lock()
		DPrintf("[ShardKV.tryReShard] KV[gid:%d, %d] updates config: %+v", kv.gid, kv.me, op.Config)
		kv.config = op.Config
		kv.cmu.Unlock()
	}
}

func (kv *ShardKV) configNum() int {
	kv.cmu.RLock()
	defer kv.cmu.RUnlock()
	return kv.config.Num
}

func (kv *ShardKV) tryUpdateStore(op Op) {
	DPrintf("[ShardKV.tryReShard] KV[gid:%d, %d] tries to update store: %v", kv.gid, kv.me, op.Info)
	for shardID, store := range op.Info.Store {
		if _, ok := kv.store[shardID]; !ok {
			kv.store[shardID] = make(Store, len(store))
		}

		for k, v := range store {
			kv.store[shardID][k] = v
		}
	}

	for shardID, record := range op.Info.Record {
		if _, ok := kv.record[shardID]; !ok {
			kv.record[shardID] = make(Record, len(record))
		}

		for k, v := range record {
			kv.record[shardID][k] = v
		}
	}
}

func (kv *ShardKV) slaveConsist(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("[ShardKV.slaveConsist] KV[gid:%d, %d] execute index: %d, op: %+v", kv.gid, kv.me, kv.ExecuteIndex(), op)
	kv.TrySetExecuteIndex(op.CommandIndex)

	DPrintf("[ShardKV.slaveConsist] KV[gid:%d, %d] received op: %+v, before modify: [%s:%v]", kv.gid, kv.me, op, op.Key, kv.GetValueWithRLock(op.Key))
	kv.tryExecute(op)
	DPrintf("[ShardKV.slaveConsist] KV[gid:%d, %d] after modify: [%s:%v]", kv.gid, kv.me, op.Key, kv.GetValueWithRLock(op.Key))

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
	DPrintf("[ShardKV.SetCommandIndex] KV[gid:%d, %d] changes command index from %d to %d", kv.gid, kv.me, kv.CommandIndex(), set)
	atomic.StoreInt64(kv.commandIndex, int64(set))
}

func (kv *ShardKV) ExecuteIndex() int {
	return int(atomic.LoadInt64(kv.executeIndex))
}

func (kv *ShardKV) TrySetExecuteIndex(set int) {
	DPrintf("[ShardKV.TrySetExecuteIndex] KV[gid:%d, %d] tries to change execute index from %d to %d", kv.gid, kv.me, kv.ExecuteIndex(), set)
	atomic.CompareAndSwapInt64(kv.executeIndex, int64(set-1), int64(set))
	DPrintf("[ShardKV.TrySetExecuteIndex] KV[gid:%d, %d] after changed: executeIndex: %d", kv.gid, kv.me, kv.ExecuteIndex())
}

func (kv *ShardKV) tryRecoverFromSnapshot() {
	DPrintf("[ShardKV.tryRecoverFromSnapshot] KV[gid:%d, %d] tries to recover from snapshot", kv.gid, kv.me)
	kv.installSnapshot(kv.persister.ReadSnapshot())
}

type Snapshot struct {
	Store        map[int]Store
	Record       map[int]Record
	CommandIndex int64
	ExecuteIndex int64
	Config       shardctrler.Config
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
	//DPrintf("[ShardKV.shouldSnapshot] KV[gid:%d, %d] check whether it's the time to snapshot, size: %d, maxraftstate: %d, result: %v", kv.gid, kv.me, size, kv.maxraftstate, result)
	return result
}

func (kv *ShardKV) snapshotBytes() []byte {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	p := Snapshot{
		Store:        kv.store,
		Record:       kv.record,
		CommandIndex: int64(kv.CommandIndex()),
		ExecuteIndex: int64(kv.ExecuteIndex()),
		Config:       kv.config,
	}
	if err := encoder.Encode(p); err != nil {
		panic(fmt.Sprintf("Failed to encode persistentState, err = %s", err))
	}
	return buffer.Bytes()
}

func (kv *ShardKV) tryInstallSnapshot(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("[ShardKV.tryInstallSnapshot] KV[gid:%d, %d] tries to install snapshot: %+v", kv.gid, kv.me, msg)
	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		kv.installSnapshot(msg.Snapshot)
	}
}

func (kv *ShardKV) installSnapshot(data []byte) {
	DPrintf("[ShardKV.installSnapshot] KV[gid:%d, %d] tries to install snapshot: %s", kv.gid, kv.me, string(data))
	if data == nil || len(data) == 0 {
		return
	}

	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)

	snapshot := new(Snapshot)
	if err := decoder.Decode(snapshot); err != nil {
		panic(fmt.Sprintf("Failed to read persist, err = %s", err))
	}

	DPrintf("[ShardKV.installSnapshot] KV[gid:%d, %d].executeIndex: %d received snapshot: %+v", kv.gid, kv.me, kv.ExecuteIndex(), snapshot)
	if kv.ExecuteIndex() <= int(snapshot.ExecuteIndex) {
		kv.store = snapshot.Store
		kv.record = snapshot.Record
		kv.commandIndex = &snapshot.CommandIndex
		kv.executeIndex = &snapshot.ExecuteIndex
		kv.config = snapshot.Config
	}
}

func (kv *ShardKV) applyConfigTimer() {
	for {
		time.Sleep(100 * time.Millisecond)

		if kv.isLeader() {
			kv.configChan <- kv.shardCtrler.Query(kv.configNum() + 1)
		}
	}
}

func (kv *ShardKV) syncConfiguration() {
	for newConfig := range kv.configChan {
		//DPrintf("[ShardKV.syncConfiguration] KV[gid:%d, %d] gets config: %+v, originConfig: %+v", kv.gid, kv.me, newConfig, kv.config)
		if kv.isLeader() {
			if kv.isLeader() && newConfig.NewerThan(kv.configNum()) {
				if kv.isLeader() {
					DPrintf("[ShardKV.syncConfiguration] KV[gid:%d, %d] receives a newer config: %+v, old config: %+v", kv.gid, kv.me, newConfig, kv.config)
					if kv.updateShard(kv.config, newConfig) {
						kv.syncConfigToFollowers(newConfig)
					}

				}
			}
		}
	}
}

func (kv *ShardKV) updateShard(oldConfig, newConfig shardctrler.Config) bool {
	kv.setResharding(Sharding)
	defer kv.setResharding(Idle)
	defer kv.clearShardList()

	DPrintf("[ShardKV.updateShard]KV[gid:%d, %d] ready to update shard, new config: %+v, old config: %+v", kv.gid, kv.me, newConfig, oldConfig)

	shardMap := kv.shardObtained(oldConfig, newConfig)
	kv.setShardList(shardMap.shardList())
	for gid, shardIDList := range shardMap {
		if !kv.applyShardForReplica(shardIDList, oldConfig.Groups[gid], newConfig) {
			DPrintf("[ShardKV.updateShard] KV[gid:%d, %d] failed to apply shard: %v from group: %d", kv.gid, kv.me, shardIDList, gid)
			return false
		}
	}

	return true
}

func (kv *ShardKV) clearShardList() {
	kv.smu.Lock()
	kv.reshardList = nil
	kv.smu.Unlock()
}

func (kv *ShardKV) setShardList(shardList []int) {
	kv.smu.Lock()
	DPrintf("[ShardKV.setShardList] KV[gid: %d, %d] set shard list: %v", kv.gid, kv.me, shardList)
	kv.reshardList = shardList
	kv.smu.Unlock()
}

func (kv *ShardKV) syncConfigToFollowers(newConfig shardctrler.Config) bool {
	DPrintf("[ShardKV.syncConfigToFollowers] KV[gid:%d, %d] Ready to sync newConfig: %+v", kv.gid, kv.me, newConfig)
	index, term, isLeader := kv.rf.Start(Op{
		Config: newConfig,
	})

	if !isLeader {
		DPrintf("[ShardKV.syncConfigToFollowers] KV[gid:%d, %d] Failed to sync config: %+v to followers", kv.gid, kv.me, kv.config)
		return false
	}

	kv.SetCommandIndex(index)
	for {
		var result Op
		if kv.isLostLeadership(int64(term)) {
			return false
		}

		select {
		case result = <-kv.commandCh:
			DPrintf("[ShardKV.syncConfigToFollowers] KV[gid:%d, %d] index = %d applyMsg = %+v", kv.gid, kv.me, index, result)
		case <-time.After(Interval):
			DPrintf("[ShardKV.syncConfigToFollowers] KV[gid:%d, %d] wait 200 msec", kv.gid, kv.me)
			continue
		}

		if !kv.isLeader() {
			DPrintf("[ShardKV.syncConfigToFollowers] KV[gid:%d, %d] is no longer a leader.", kv.gid, kv.me)
			return false
		}
		return true
	}
}

func (kv *ShardKV) applyShardForReplica(shardIDList []int, replicas []string, newConfig shardctrler.Config) bool {
	DPrintf("[ShardKV.applyShardForReplica] KV[gid:%d, %d] applies shard: %v from %v", kv.gid, kv.me, shardIDList, replicas)
	args := MigrateArgs{
		ShardIDList: shardIDList,
		Config:      newConfig,
	}

	for {
		for _, replica := range replicas {
			time.Sleep(50 * time.Millisecond)
			if !kv.isLeader() {
				DPrintf("[ShardKV.applyShardForReplica] KV[gid:%d, %d] is no longer a leader", kv.gid, kv.me)
				return false
			}

			var reply MigrateReply
			DPrintf("[ShardKV.applyShardForReplica] KV[gid:%d, %d] send %+v to replica: %s", kv.gid, kv.me, args, replica)
			ok := kv.makeEnd(replica).Call(MethodShardKVMigrate, &args, &reply)
			DPrintf("[ShardKV.applyShardForReplica] KV[gid:%d, %d] received reply: %+v from replica: %s", kv.gid, kv.me, reply, replica)

			if ok && reply.Err == OK {
				DPrintf("[ShardKV.applyShardForReplica] KV[gid:%d, %d] ready to send %+v to raft", kv.gid, kv.me, reply)
				index, term, isLeader := kv.rf.Start(Op{
					Info: reply.Info,
				})

				if !isLeader {
					DPrintf("[ShardKV.applyShardForReplica] KV[gid:%d, %d] is no longer a leader", kv.gid, kv.me)
					reply.Err = ErrWrongLeader
					return false
				}

				kv.SetCommandIndex(index)
				for {
					var result Op
					if kv.isLostLeadership(int64(term)) {
						reply.Err = ErrWrongLeader
						return false
					}

					select {
					case result = <-kv.commandCh:
						DPrintf("[ShardKV.applyShardForReplica] KV[gid:%d, %d] index = %d applyMsg = %+v", kv.gid, kv.me, index, result)
					case <-time.After(Interval):
						DPrintf("[ShardKV.applyShardForReplica] KV[gid:%d, %d] wait 200 msec", kv.gid, kv.me)
						continue
					}

					if !kv.isLeader() {
						DPrintf("[ShardKV.applyShardForReplica] KV[gid:%d, %d] is no longer a leader.", kv.gid, kv.me)
						reply.Err = ErrWrongLeader
					}
					return true
				}
			}
		}
	}

}

type Shard struct {
	OriginGID   int
	ShardIDList []int
}

type ShardMap map[int][]int

func (s ShardMap) shardList() []int {
	result := make([]int, 0)
	for _, v := range s {
		result = append(result, v...)
	}

	return result
}

func (kv *ShardKV) shardObtained(oldConfig, newConfig shardctrler.Config) ShardMap {
	result := make(ShardMap, 0)
	for i := 0; i < len(oldConfig.Shards); i++ {
		if newConfig.Shards[i] == kv.gid && oldConfig.Shards[i] != 0 && newConfig.Shards[i] != oldConfig.Shards[i] {
			if _, ok := result[oldConfig.Shards[i]]; !ok {
				result[oldConfig.Shards[i]] = make([]int, 0)
			}
			result[oldConfig.Shards[i]] = append(result[oldConfig.Shards[i]], i)
		}
	}

	DPrintf("[ShardKV.shardObtained] KV[gid:%d, %d] shards to obtain: %v", kv.gid, kv.me, result)
	return result
}

func (kv *ShardKV) isResharding() bool {
	return atomic.LoadInt64(kv.resharding) == Sharding
}

func (kv *ShardKV) setResharding(x int64) {
	kv.mu.Lock()
	atomic.StoreInt64(kv.resharding, x)
	kv.mu.Unlock()
}

func (kv *ShardKV) shouldWaitForResharding(shardList []int) bool {
	kv.smu.RLock()
	defer kv.smu.RUnlock()
	for _, shard := range shardList {
		for _, sharding := range kv.reshardList {
			if shard == sharding {
				return true
			}
		}
	}
	return false
}

func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.isLeader() {
		DPrintf("[ShardKV.Migrate] KV[gid:%d, %d] is no longer a leader", kv.gid, kv.me)
		reply.Err = ErrWrongLeader
		return
	}

	if kv.isResharding() && kv.shouldWaitForResharding(args.ShardIDList) && args.Config.Num > kv.configNum() {
		DPrintf("[ShardKV.Migrate] KV[gid:%d, %d] is resharding", kv.gid, kv.me)
		reply.Err = ErrWrongGroup
		return
	}

	DPrintf("[ShardKV.Migrate] KV[gid:%d, %d] received %+v", kv.gid, kv.me, args)
	reply.Err = OK

	reply.Info.Store = make(map[int]Store, len(args.ShardIDList))
	reply.Info.Record = make(map[int]Record, len(args.ShardIDList))
	for _, shardID := range args.ShardIDList {
		if _, ok := kv.store[shardID]; ok {
			reply.Info.Store[shardID] = kv.store[shardID].Copy()
		}
		if _, ok := kv.record[shardID]; ok {
			reply.Info.Record[shardID] = kv.record[shardID].Copy()
		}
	}
	DPrintf("[ShardKV.Migrate] KV[gid:%d, %d] store now: %+v, reply: %+v", kv.gid, kv.me, kv.store, reply)
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
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	var resharding int64

	applyCh := make(chan raft.ApplyMsg)
	kv := &ShardKV{
		me:           me,
		rf:           raft.Make(servers, me, persister, applyCh),
		applyCh:      applyCh,
		makeEnd:      makeEnd,
		gid:          gid,
		shardCtrler:  shardctrler.MakeClerk(ctrlers),
		maxraftstate: maxraftstate,
		commandCh:    make(chan Op),
		store:        make(map[int]Store, 0),
		record:       make(map[int]Record, 0),
		commandIndex: new(int64),
		executeIndex: new(int64),
		persister:    persister,
		config:       shardctrler.Config{},
		configChan:   make(chan shardctrler.Config, 10),
		resharding:   &resharding,
	}

	kv.tryRecoverFromSnapshot()
	go kv.listen()
	go kv.applyConfigTimer()
	go kv.syncConfiguration()
	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	return kv
}
