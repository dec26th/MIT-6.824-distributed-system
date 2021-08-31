# MIT-6.824-Distributed-System
Never lose the desire for learning.

## MapReduce
done

## Raft
![Raft接口详情](./pics/figure2.png)
- 2A   finished ✅
  - use context with timeout to control the election flow 
  - when connection between candidate and follower is closed, how does candidate count the votes?
- 2B
  - This also commits all preceding entries in the leader's log, including entries created by previous leaders? 需要把所有的都commit一遍么
  - Once a follower learns that a log entry is committed, it applies the entry to its local state machine (in log order).
  - When send- ing an AppendEntries RPC, the leader includes the index and term of the entry in its log that immediately precedes the new entries.
- 2C
- 2D

Course Site: https://pdos.csail.mit.edu/6.824/schedule.html
