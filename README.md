# MIT-6.824-Distributed-System
Never lose the desire for learning.

Course Site: https://pdos.csail.mit.edu/6.824/schedule.html

## MapReduce
done

## Raft
https://pdos.csail.mit.edu/6.824/labs/lab-raft.html
![Raft接口详情](./pics/figure2.png)
- 2A finished ✅
- 2B finished ✅
- 2C finished ✅
- 2D finished ✅
done

## Fault-tolerant Key/Value Service
[课程链接
](https://pdos.csail.mit.edu/6.824/labs/lab-kvraft.html
)
- 3A ✅
  - Clerks send `Put()`, `Append()`, and `Get()` RPCs to the kvserver whose associated Raft is the leader
  - If the Clerk sends an RPC to the wrong kvserver, or if it cannot reach the kvserver, the Clerk should re-try by sending to a different kvserver.
  - All the kvservers execute operations from the Raft log in order, applying the operations to their key/value databases;
  - Your kvservers should not directly communicate; they should only interact with each other through Raft.
  - Each server should execute Op commands as Raft **commits** them
  - If a leader fails just after committing an entry to the Raft log, the Clerk may not receive a reply, and thus may re-send the request to another leader. 
  - Each call to `Clerk.Put()` or `Clerk.Append()` should result in just a single execution, so you will have to ensure that the re-send doesn't result in the servers executing the request twice.
    - how to ensure the command will be only executed once ?
    - Generate a client id and request id to identify whether the command has been executed
  - It's OK to assume that a client will make only one call into a Clerk at a time.

- 3B ✅

## Sharded Key/Value Service
[课程链接](https://pdos.csail.mit.edu/6.824/labs/lab-shard.html)
- 4A
  - The shardctrler manages **a sequence of numbered configurations**. Each **configuration** describes **a set of replica groups** and **an assignment of shards to replica groups**.
  - Whenever this assignment needs to change, the shard controller creates a new configuration with the new assignment.
  - Key/value clients and servers contact the shardctrler when they want to know the current (or a past) configuration.
  - The `Join` RPC is used by an administrator to add new replica groups.
    - Its **argument** is a **set of mappings** from unique, non-zero **replica group identifiers (GIDs)** to **lists of server names**.
    - The shardctrler should **react by creating a new configuration** that includes the new replica groups.
    - The new configuration should **divide the shards as evenly as possible** among the full set of groups, and should **move as few shards as possible** to achieve that goal.
    - The shardctrler **should allow re-use of a GID** if it's **not part of the current configuration** (i.e. a GID should be allowed to Join, then Leave, then Join again).
  - The `Leave` RPC's argument is a list of GIDs of previously joined groups.
    - The shardctrler should **create a new configuration** that **does not include those groups**, and that **assigns those groups' shards** to the **remaining groups**.
  - The `Move` RPC's arguments are a shard number and a GID.
    - 
  - The `Query` RPC's argument is a configuration number.
- 4B