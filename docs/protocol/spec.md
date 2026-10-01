# Wire Protocol Specification (L2)

## Constants
- `MAGIC`: `0x464C5431` ("FLT1" in ASCII)
- `PROTOCOL_VERSION`: `1`
- `MAX_DATAGRAM_SIZE`: `1472` bytes (Standard Ethernet MTU 1500 - 20 IP - 8 UDP)
- `HEADER_SIZE`: `40` bytes

## Binary Layout of `PacketHeader`

Defined in [`src/codec/packet_header.rs`](file:///home/chad/source/rust/flotilla/src/codec/packet_header.rs). All fields are Little-Endian.

| Offset | Type | Field Name | Description |
| :--- | :--- | :--- | :--- |
| `0..4` | `u32` | `magic` | Protocol magic constant (`0x464C5431`) |
| `4..6` | `u16` | `version` | Protocol version (`1`) |
| `6..8` | `u16` | `msg_type` | RPC message type discriminant |
| `8..16` | `u64` | `sender_id` | Originating Node ID (`NodeId`) |
| `16..24` | `u64` | `receiver_id` | Destination Node ID (`NodeId`) |
| `24..32` | `u64` | `term` | Current term of sender (`Term`) |
| `32..36` | `u32` | `checksum` | CRC32 of payload |
| `36..40` | `u32` | `payload_len` | Byte length of payload |

Total Header Size: 40 bytes.

## Message Type Codes

Defined in [`src/message/msg_type.rs`](file:///home/chad/source/rust/flotilla/src/message/msg_type.rs):
- `0x0001`: `RequestVoteArgs` ([`src/message/request_vote_args.rs`](file:///home/chad/source/rust/flotilla/src/message/request_vote_args.rs))
- `0x0002`: `RequestVoteReply` ([`src/message/request_vote_reply.rs`](file:///home/chad/source/rust/flotilla/src/message/request_vote_reply.rs))
- `0x0003`: `AppendEntriesArgs` ([`src/message/append_entries_header.rs`](file:///home/chad/source/rust/flotilla/src/message/append_entries_header.rs))
- `0x0004`: `AppendEntriesReply` ([`src/message/append_entries_reply.rs`](file:///home/chad/source/rust/flotilla/src/message/append_entries_reply.rs))
- `0x0005`: `HeartbeatArgs`
- `0x0006`: `HeartbeatReply`
- `0x0007`: `ClientProposal` (raw bytes payload for client state proposals)
- `0x0008`: `ClientProposalReply` ([`src/message/client_proposal_reply.rs`](file:///home/chad/source/rust/flotilla/src/message/client_proposal_reply.rs))

## Payload Binary Layouts

### 1. `RequestVoteArgs` (32 bytes)
Defined in [`src/message/request_vote_args.rs`](file:///home/chad/source/rust/flotilla/src/message/request_vote_args.rs):
| Offset | Type | Field Name | Description |
| :--- | :--- | :--- | :--- |
| `0..8` | `u64` | `term` | Candidate's term (`Term`) |
| `8..16` | `u64` | `candidate_id` | Candidate requesting vote (`NodeId`) |
| `16..24` | `u64` | `last_log_index` | Index of candidate's last log entry (`LogIndex`) |
| `24..32` | `u64` | `last_log_term` | Term of candidate's last log entry (`Term`) |

### 2. `RequestVoteReply` (16 bytes)
Defined in [`src/message/request_vote_reply.rs`](file:///home/chad/source/rust/flotilla/src/message/request_vote_reply.rs):
| Offset | Type | Field Name | Description |
| :--- | :--- | :--- | :--- |
| `0..8` | `u64` | `term` | Current term of voter (`Term`) |
| `8..9` | `u8` | `vote_granted` | `1` if vote is granted, `0` if rejected |
| `9..16` | `[u8; 7]` | `_pad` | Alignment padding |

### 3. `AppendEntriesHeader` (48 bytes + variable entry payload)
Defined in [`src/message/append_entries_header.rs`](file:///home/chad/source/rust/flotilla/src/message/append_entries_header.rs):
| Offset | Type | Field Name | Description |
| :--- | :--- | :--- | :--- |
| `0..8` | `u64` | `term` | Leader's term (`Term`) |
| `8..16` | `u64` | `leader_id` | Leader's node identifier (`NodeId`) |
| `16..24` | `u64` | `prev_log_index` | Index of log entry immediately preceding new ones (`LogIndex`) |
| `24..32` | `u64` | `prev_log_term` | Term of `prev_log_index` entry (`Term`) |
| `32..40` | `u64` | `leader_commit` | Leader's current `commit_index` (`LogIndex`) |
| `40..44` | `u32` | `entries_count` | Number of replicated log entries contained in payload |
| `44..48` | `[u8; 4]` | `_pad` | Alignment padding |

### 4. `AppendEntriesReply` (32 bytes)
Defined in [`src/message/append_entries_reply.rs`](file:///home/chad/source/rust/flotilla/src/message/append_entries_reply.rs):
| Offset | Type | Field Name | Description |
| :--- | :--- | :--- | :--- |
| `0..8` | `u64` | `term` | Follower's current term (`Term`) |
| `8..16` | `u64` | `follower_id` | Responding follower identifier (`NodeId`) |
| `16..17` | `u8` | `success` | `1` if follower contained entry matching `prev_log_index`/`term` |
| `17..24` | `[u8; 7]` | `_pad` | Alignment padding |
| `24..32` | `u64` | `match_index` | Follower's highest replicated index (`LogIndex`) |

### 5. `ClientProposal` (Variable payload)
Direct payload containing the uninterpreted application bytes submitted for replicated state machine execution.

### 6. `ClientProposalReply` (32 bytes)
Defined in [`src/message/client_proposal_reply.rs`](file:///home/chad/source/rust/flotilla/src/message/client_proposal_reply.rs):
| Offset | Type | Field Name | Description |
| :--- | :--- | :--- | :--- |
| `0..1` | `u8` | `success` | `1` if proposal accepted by leader, `0` if rejected |
| `1..8` | `[u8; 7]` | `_pad` | Alignment padding |
| `8..16` | `u64` | `index` | Appended or accepted log index (`LogIndex`) |
| `16..24` | `u64` | `term` | Current term of the responder (`Term`) |
| `24..32` | `u64` | `leader_id` | Current leader identifier (`NodeId`) for redirection, or `0` if unknown |

## Safe Alignment & zerocopy
All protocol headers derive `zerocopy::FromBytes`, `zerocopy::IntoBytes`, `zerocopy::Immutable`, and `zerocopy::KnownLayout`. Structures conform to standard 8-byte alignment requirements with explicit padding fields. No `unsafe` pointer casts or transmutations are permitted.

## Transport Framing

### 1. UDP Datagram Framing
- Each UDP datagram contains a 40-byte `PacketHeader` followed immediately by `payload_len` bytes.
- Total datagram size is bounded by `MAX_DATAGRAM_SIZE` (1472 bytes) to guarantee zero IP-level packet fragmentation across standard Ethernet MTUs.

### 2. TCP Length-Prefixed Stream Framing
Defined in [`src/client/tcp/framing.rs`](file:///home/chad/source/rust/flotilla/src/client/tcp/framing.rs):
- Stream reads process the 40-byte `PacketHeader` first via `read_exact`.
- `magic` is verified against `0x464C5431`.
- `payload_len` defines the exact size of the following payload bytes read via `read_exact`.
- Stream writes send `[PacketHeader, payload]` contiguously, flushing immediately.

### 3. gRPC / Protobuf Transport
Defined in [`proto/flotilla.proto`](file:///home/chad/source/rust/flotilla/proto/flotilla.proto) for HTTP/2 multi-language interoperability:
- Service: `FlotillaService`
- Endpoints:
  - `Propose(ProposalRequest) -> ProposalResponse`: Submits state machine commands.
  - `Step(StepRequest) -> StepResponse`: Exchanges raw datagram packets between cluster nodes.
  - `ClusterStatus(StatusRequest) -> StatusResponse`: Queries node role, term, leader, and commit index.
