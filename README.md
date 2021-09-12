# MIT-6.824-Distributed-System
Never lose the desire for learning.

Course Site: https://pdos.csail.mit.edu/6.824/schedule.html

## MapReduce
done

## Raft
![Raft接口详情](./pics/figure2.png)
- 2A finished ✅
  - use context with timeout to control the election flow 
  - when connection between candidate and follower is closed, how does candidate count the votes?
- 2B finished ✅
  - This also commits all preceding entries in the leader's log, including entries created by previous leaders? 需要把所有的都commit一遍么
  - Once a follower learns that a log entry is committed, it applies the entry to its local state machine (in log order).
  - When send- ing an AppendEntries RPC, the leader includes the index and term of the entry in its log that immediately precedes the new entries.
  - Once a follower learns that a log entry is committed, it applies the entry to its local state machine (in log order).
  - To bring a follower’s log into consistency with its own, the leader must find the latest log entry where the two logs agree, delete any entries in the follower’s log after that point, and send the follower all of the leader’s entries after that point.
  - All of these actions happen in response to the **consistency check** performed by AppendEntries RPCs.
  - When a leader first comes to power, it initializes all nextIndex values to the index just after the last one in its log (11 in Figure 7). If a follower’s log is inconsistent with the leader’s, the AppendEntries consis- tency check will fail in the next AppendEntries RPC. Af- ter a rejection, the leader decrements nextIndex and retries the AppendEntries RPC.
  - Raft uses a simpler approach where it guarantees that all the committed entries from previous terms are present on each new leader from the moment of its election, without the need to transfer those entries to the leader.
  - Raft uses the voting process to prevent a candidate from winning an election unless its log contains all committed entries.
  - A candidate must contact a majority of the cluster in order to be elected, which means that every committed entry must be present in at least one of those servers.
- 2C
  - AppendEntries request in the preview will 
- 2D