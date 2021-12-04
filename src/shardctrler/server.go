package shardctrler

import (
	"6.824/utils"
	"log"
	"math"
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

type ShardCtrler struct {
	mu      sync.Mutex
	cmu     sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	commandCh chan Op


	record map[int64]int64
	commandIndex    *int64
	executeIndex    *int64
	// Your data here.

	configs []Config // indexed by config num
}

func (sc *ShardCtrler) newConfig() Config {
	latestConfig := sc.latestConfig()
	c := Config{
		Num:    latestConfig.Num + 1,
	}

	for i := 0; i < len(latestConfig.Shards); i++ {
		c.Shards[i] = latestConfig.Shards[i]
	}

	c.Groups = make(map[int][]string, len(latestConfig.Groups))
	for k, v := range latestConfig.Groups {
		c.Groups[k] = make([]string, len(v))
		copy(c.Groups[k], v)
	}
	return c
}

func (sc *ShardCtrler) latestConfig() Config {
	return sc.configs[len(sc.configs) - 1]
}

func (sc *ShardCtrler) Recorded(clientID, requestID int64) bool {
	result, ok := sc.record[clientID]
	recorded := ok && result >= requestID
	DPrintf("[ShardCtrler.Recorded] sc[%d] check whether client: %d has send request: %d, result: %v", sc.me, clientID, requestID, recorded)
	return recorded
}

func (sc *ShardCtrler) Record(clientID, requestID int64) {
	sc.record[clientID] = requestID
}

func (sc *ShardCtrler) isLostLeadership(term int64) bool {
	if !sc.isLeader() {
		DPrintf("[ShardCtrler.isLostLeadership] sc[%d] is not a leader", sc.me)
		return true
	}

	if term != sc.rf.CurrentTerm() {
		DPrintf("[ShardCtrler.isLostLeadership] sc[%d]'s term changed from %d to %d", sc.me, term, sc.rf.CurrentTerm())
		return true
	}

	return false
}

func (sc *ShardCtrler) isLeader() bool {
	_, isLeader := sc.rf.GetState()
	return isLeader
}

func (sc *ShardCtrler) CommandIndex() int {
	return int(atomic.LoadInt64(sc.commandIndex))
}

func (sc *ShardCtrler) SetCommandIndex(set int) {
	DPrintf("[ShardCtrler.SetCommandIndex] sc[%d] changes command index from %d to %d", sc.me, sc.CommandIndex(), set)
	atomic.StoreInt64(sc.commandIndex, int64(set))
}

type Op struct { // Your data here.
	Config  Config
	CommandIndex    int
}



func (sc *ShardCtrler) apply(args Args, reply Reply, modifier Modifier) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	DPrintf("[ShardCtrler.apply] sc[%d] received %+v", sc.me, args)
	reply.SetErr(OK)
	reply.SetWrongLeader(false)

	if !sc.isLeader() {
		return
	}

	if sc.Recorded(args.GetClientID(), args.GetRequestID()) {
		DPrintf("[ShardCtrler.apply] sc[%d] %d request has already executed.", sc.me, args.GetRequestID())
		return
	}

	c := sc.newConfig()
	modifier(&c)
	DPrintf("[ShardCtrler.apply] After modify and re shard, config: %+v", c)

	DPrintf("[ShardCtrler.apply] sc[%d] ready to send %+v to raft", sc.me, args)
	index, term, isLeader := sc.rf.Start(Op{
		Config: c,
	})
	if !isLeader {
		reply.SetWrongLeader(true)
		DPrintf("[ShardCtrler.apply] sc[%d] failed to start because server is not a leader", sc.me)
		return
	}
	DPrintf("[ShardCtrler.apply] sc[%d] start to replicate command %+v. index = %d, term = %d", sc.me, args, index, term)
	sc.SetCommandIndex(index)

	for {
		var result Op
		if sc.isLostLeadership(int64(term)) {
			reply.SetWrongLeader(true)
			return
		}

		select {
		case result = <-sc.commandCh:
			DPrintf("[ShardCtrler.apply] sc[%d] index = %d args = %+v, applyMsg = %+v", sc.me, index, args, result)
		case <- time.After(Interval):
			DPrintf("[ShardCtrler.apply] sc[%d] wait 200 msec", sc.me)
			continue
		}

		if sc.isLeader() {
			sc.updateConfig(result.Config)
		} else {
			DPrintf("[ShardCtrler.apply] sc[%d] now is no longer leader.", sc.me)
			reply.SetWrongLeader(true)
		}
		return
	}
}

func (sc *ShardCtrler) updateConfig(config Config) {
	sc.cmu.Lock()
	DPrintf("[ShardCtrler.updateConfig] SC[%d] length of configs: %d, config to update: %+v", sc.me, len(sc.configs), config)
	if config.Num == len(sc.configs) {
		sc.configs = append(sc.configs, config)
	}
	sc.cmu.Unlock()
}

func JoinModifier(args *JoinArgs) Modifier {
	return func(config *Config) {
		for k, v := range args.Servers {
			config.Groups[k] = make([]string, len(v))
			copy(config.Groups[k], v)
		}

		config.Shards = evenShard(config.Shards, 10 / len(config.Groups), utils.MapKeysToSlice(args.Servers))
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	sc.apply(args, reply, JoinModifier(args))
}

func LeaveModifier(args *LeaveArgs) Modifier {
	return func(config *Config) {
		for i := 0; i < len(config.Shards); i++ {
			if utils.ContainsInt(args.GIDs, config.Shards[i]) {
				config.Shards[i] = 0
				continue
			}
		}

		for i := 0; i < len(args.GIDs); i++ {
			delete(config.Groups, args.GIDs[i])
		}

		config.Shards = evenShard(config.Shards, 10 / len(config.Groups), nil)
	}
}

func evenShard(shards [10]int, expected int, join []int) [10]int {
	record := make(map[int][]int)
	for i := 0; i < len(join); i++ {
		record[join[i]] = []int{}
	}

	for i := 0; i < len(shards); i++ {
		if _, ok := record[shards[i]]; !ok {
			record[shards[i]] = []int{i}
		} else {
			record[shards[i]] = append(record[shards[i]], i)
		}
	}

	larger := make([]int, 0)
	for k := range record {
		if k == 0 {
			larger = append(larger, record[k]...)
			delete(record, 0)
		}
		if len(record[k]) > expected {
			larger = append(larger, record[k][expected:]...)
			record[k] = record[k][:expected]
		}
	}

	for k, v := range record {
		lengthToAppend := expected - len(v)
		var toAppend []int
		if len(v) < expected {
			if len(larger) <= lengthToAppend {
				toAppend = larger[:]
				larger = []int{}
			} else {
				toAppend = larger[:lengthToAppend]
				larger = larger[lengthToAppend:]
			}

			for _, shard := range toAppend {
				shards[shard] = k
			}
		}
	}

	for i := 0; i < len(larger); i++ {
		record := make(map[int]int)
		for _, replica := range shards{
			if replica != 0 {
				record[replica] ++
			}
		}

		min := math.MaxInt
		index := 0
		for key, value := range record {
			if value < min {
				index = key
			}
		}

		shards[larger[i]] = index
	}
	return shards
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	sc.apply(args, reply, LeaveModifier(args))
	// Your code here.
}

func MoveModifier(args *MoveArgs) Modifier {
	return func(config *Config) {
		config.Shards[args.Shard] = args.GID
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	sc.apply(args, reply, MoveModifier(args))
	// Your code here.
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	DPrintf("[ShardCtrler.Get] KV[%d] tries to get config: %d", sc.me, args.Num)
	reply.Err = OK

	if !sc.isLeader() {
		reply.WrongLeader = true
		return
	}
	reply.Config = sc.latestConfig()
	if args.Num != -1 && args.Num < len(sc.configs) {
		reply.Config = sc.configs[args.Num]
	}
	// Your code here.
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) listen() {
	for applyMsg := range sc.applyCh {
		op := getOP(applyMsg)
		op.CommandIndex = applyMsg.CommandIndex
		if sc.isLeader() {
			DPrintf("[ShardCtrler.listen] Leader[%d] has commit %+v, commend index = %d", sc.me, applyMsg, sc.CommandIndex())
			sc.TrySetExecuteIndex(applyMsg.CommandIndex)

			if applyMsg.CommandIndex == sc.CommandIndex() {
				DPrintf("[ShardCtrler.listen] Leader[%d] send command:%+v to chan", sc.me, op)
				sc.commandCh <- op
			}
			sc.tryExecute(op)
			continue
		}

		sc.slaveConsist(op)
	}
}

func (sc *ShardCtrler) slaveConsist(op Op) {
	sc.mu.Lock()
	sc.TrySetExecuteIndex(op.CommandIndex)
	sc.tryExecute(op)
	sc.mu.Unlock()
}

func (sc *ShardCtrler) tryExecute(op Op) {
	DPrintf("[ShardCtrler.tryExecute] SC[%d] received %+v, executeIndex: %d", sc.me, op, sc.ExecuteIndex())
	if op.CommandIndex == sc.ExecuteIndex() {
		sc.updateConfig(op.Config)
	}
}

func (sc *ShardCtrler) TrySetExecuteIndex(set int) {
	DPrintf("[ShardCtrler.TrySetExecuteIndex] KV[%d] tries to change execute index from %d to %d", sc.me, sc.ExecuteIndex(), set)
	atomic.CompareAndSwapInt64(sc.executeIndex, int64(set-1), int64(set))
	DPrintf("[ShardCtrler.TrySetExecuteIndex] KV[%d] after changed: executeIndex: %d", sc.me, sc.ExecuteIndex())
}

func (sc *ShardCtrler) ExecuteIndex() int {
	return int(atomic.LoadInt64(sc.executeIndex))
}

func getOP(applyMsg raft.ApplyMsg) Op {
	command := applyMsg.Command
	if result, ok := command.(Op); !ok {
	} else {
		return result
	}

	return Op{}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	configs := make([]Config, 1)
	configs[0].Groups = map[int][]string{}
	applyCh := make(chan raft.ApplyMsg)

	sc := &ShardCtrler{
		mu:           sync.Mutex{},
		me:           me,
		rf:           raft.Make(servers, me, persister, applyCh),
		applyCh:      applyCh,
		record:       make(map[int64]int64),
		commandIndex: new(int64),
		executeIndex: new(int64),
		configs:      configs,
	}
	labgob.Register(Op{})

	// Your code here.

	return sc
}
