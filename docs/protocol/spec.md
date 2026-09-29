# Wire Protocol Specification (L2)

## Constants
- `MAGIC`: `0x464C5431` ("FLT1" in ASCII)
- `PROTOCOL_VERSION`: `1`
- `MAX_DATAGRAM_SIZE`: `1472` bytes
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

## Safe Alignment & zerocopy
The header layout conforms to standard 8-byte alignment requirements with no internal padding bytes, allowing safe direct mapping via `zerocopy::FromBytes` and `zerocopy::IntoBytes`.
