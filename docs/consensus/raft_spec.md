# Raft State Machine Specification (L2)

## Raft Invariants Enforced
1. **Election Safety**: At most one leader can be elected in a given term.
2. **Leader Append-Only**: A leader never overwrites or truncates its entries; it only appends new entries.
3. **Log Matching**: If two logs contain an entry with the same index and term, then the logs are identical in all entries up through the given index.
4. **Leader Completeness**: If a log entry is committed in a given term, then that entry will be present in the logs of the leaders for all higher-numbered terms.
5. **State Machine Safety**: If a server has applied a log entry at a given index to its state machine, no other server will ever apply a different log entry for the same index.

## Election Mechanics
- Ticks increment the `election_elapsed` counter.
- When `election_elapsed >= randomized_election_timeout`, the node transitions from Follower to Candidate:
  - Increments `current_term`.
  - Votes for itself.
  - Resets election timer.
  - Emits `RequestVote` RPCs to all peers.
- Upon receiving a majority of affirmative votes, Candidate transitions to Leader.

## Replication & Commit Logic
- Leader tracks `next_index[peer]` and `match_index[peer]` for each follower.
- When `match_index` for a quorum of nodes exceeds `commit_index`, and the entry at that index was created in the current term, `commit_index` advances.
