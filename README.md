# MIT-6.824-Distributed-System
Never lose the desire for learning.

Course Site: https://pdos.csail.mit.edu/6.824/schedule.html

---
## MapReduce
done
---

## Raft
[课程链接](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html)
![Raft接口详情](./pics/figure2.png)
- 2A finished ✅
- 2B finished ✅
- 2C finished ✅
- 2D finished ✅
done
---
## Fault-tolerant Key/Value Service
[课程链接](https://pdos.csail.mit.edu/6.824/labs/lab-kvraft.html)
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
---
- 3B ✅
---
## Sharded Key/Value Service
[课程链接](https://pdos.csail.mit.edu/6.824/labs/lab-shard.html)
- 4A ✅

  - For example, a Put may arrive at about the same time as a reconfiguration that causes the replica group to stop being responsible for the shard holding the Put's key. 
  - All replicas in the group must agree on whether the Put occurred before or after the reconfiguration. 
    - If before, the Put should take effect and the new owner of the shard will see its effect; 
    - if after, the Put won't take effect and client must re-try at the new owner. 
  - The recommended approach is to have each replica group use Raft to log not just the sequence of Puts, Appends, and Gets **but also the sequence of reconfigurations**. You will need to **ensure that at most one replica group** is serving requests for each shard at any one time.
  - 需要确定put以及get操作是在reconfiguration前面还是后面。
  - Reconfiguration also requires interaction among the replica groups. 
  - For example, in configuration 10 group G1 may be responsible for shard S1. In configuration 11, group G2 may be responsible for shard S1. During the reconfiguration from 10 to 11, G1 and G2 must use RPC to move the contents of shard S1 (the key/value pairs) from G1 to G2.
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
    - The shardctrler replies with the configuration that has that number. 
  - The `Query` RPC's argument is a configuration number.
    - If the **number is -1 or bigger** than **the biggest known configuration number**, the shardctrler should **reply with the latest configuration**. 
    - The result of Query(-1) should reflect every Join, Leave, or Move RPC that the shardctrler finished handling before it received the Query(-1) RPC.
  - The very first configuration should be numbered zero. It should contain no groups, and all shards should be assigned to GID zero (an invalid GID).
  - There will usually be significantly more shards than groups (i.e., each group will serve more than one shard), in order that load can be shifted at a fairly fine granularity.

--- 

- 4B （push shard instead of pulling shard）

  - Each shardkv server operates as part of a replica group. 
    - Each replica group serves `Get`, `Put`, and `Append` operations for some of the key-space shards. 
    - Use `key2shard()` in client.go to find which shard a key belongs to.
  - A single instance of the **shardctrler service** assigns shards to replica groups; 
    - when this **assignment changes**, replica groups **have to hand off shards to each other**, while ensuring that clients do not see inconsistent responses.
  - Your storage system must provide a linearizable interface to applications that use its client interface. 
    - That is, completed application calls to the `Clerk.Get()`, `Clerk.Put()`, and `Clerk.Append()` methods in `shardkv/client.go` must appear to have affected all replicas in the same order.
  - Each of your shards is only required to make progress when a majority of servers in the shard's Raft replica group is **alive** and **can talk to each other**, and can **talk to a majority of the shardctrler servers**. 
    - Your implementation must operate (serve requests and be able to re-configure as needed) **even if a minority of servers in some replica group(s) are dead**, temporarily unavailable, or slow.
  - We supply you with `client.go` code that sends each RPC to the replica group responsible for the RPC's key. 
    - It **re-tries** if the replica group says it is **not responsible for the key**; in that case, the **client code asks the shard controller for the latest configuration** and tries again. 
    - You'll have to modify client.go as part of your support for dealing with duplicate client RPCs, much as in the kvraft lab.
---
  - Your first task is to pass the very first shardkv test. 
    - In this test, there is only a single assignment of shards, so your code should be **very similar to that of your Lab 3 server**. 
    - The biggest modification will be to **have your server detect when a configuration happens** and start accepting requests whose keys match shards that it now owns.
  - You will need to **make your servers watch for configuration changes**, and when one is detected, to start the **shard migration process**
    - If a replica group loses a shard, it must **stop serving requests to keys in that shard immediately**, and **start migrating the data for that shard to the replica group** that is taking over ownership.
    - If a replica group gains a shard, it **needs to wait for the previous owner to send over the old shard data** before accepting requests for that shard.
  - Implement **shard migration during configuration changes**. Make sure that **all servers in a replica group do the migration at the same point** in the sequence of operations they execute, so that they all either accept or reject concurrent client requests. 
  - Your server will need to **periodically poll the shardctrler to learn about new configurations**. The tests expect that your code polls roughly **every 100 milliseconds**
  - Servers will **need to send RPCs to each other in order to transfer shards** during configuration changes. 
    - The shardctrler's **Config struct contains server names**, but you need a `labrpc.ClientEnd` in order to send an RPC. 
    - You should use the `make_end()` function passed to `StartServer()` to **turn a server name into a ClientEnd**. `shardkv/client.go` contains code that does this.
  - Add code to `server.go` to periodically fetch the latest configuration from the shardctrler
    - add code to **reject client requests** if the receiving group **isn't responsible for the client's key's shard**.
  - Your server should respond with an **ErrWrongGroup error** to a client RPC with **a key that the server isn't responsible for** (i.e. for a key whose shard is not assigned to the server's group).
    - Make sure your `Get`, `Put`, and `Append` handlers make this decision correctly in the face of a concurrent re-configuration.
  - Think about **how the shardkv client and server** should **deal with ErrWrongGroup**. 
    - Should the **client change the sequence number** if it receives ErrWrongGroup? 
    - Should the server **update the client state** if it returns ErrWrongGroup when executing a Get/Put request?
  - After a server has moved to a new configuration, **it is acceptable for it to continue to store shards that it no longer owns** (though this would be regrettable in a real system). This may help simplify your server implementation.