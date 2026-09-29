# Wire Protocol Specification (L2)

## Constants
- `MAGIC`: `0x464C5431` ("FLT1" in ASCII)
- `PROTOCOL_VERSION`: `1`
- `MAX_DATAGRAM_SIZE`: `1472` bytes
- `HEADER_SIZE`: `36` bytes

## Binary Layout of `PacketHeader`

All fields are Little-Endian.

| Offset | Type | Field Name | Description |
| :--- | :--- | :--- | :--- |
| `0..4` | `u32` | `magic` | Protocol magic constant (`0x464C5431`) |
| `4..6` | `u16` | `version` | Protocol version (`1`) |
| `6..8` | `u16` | `msg_type` | RPC message type discriminant |
| `8..16` | `u64` | `sender_id` | Originating Node ID |
| `16..24` | `u64` | `receiver_id` | Destination Node ID |
| `24..32` | `u64` | `term` | Current term of sender |
| `32..36` | `u32` | `checksum` | CRC32 of payload |
| `36..40` | `u32` | `payload_len` | Byte length of payload |

Total Header Size: 40 bytes.

## Message Type Codes
- `0x0001`: `RequestVoteArgs`
- `0x0002`: `RequestVoteReply`
- `0x0003`: `AppendEntriesArgs`
- `0x0004`: `AppendEntriesReply`
- `0x0005`: `HeartbeatArgs`
- `0x0006`: `HeartbeatReply`

## Safe Alignment & zerocopy
The header layout conforms to standard 8-byte alignment requirements with no internal padding bytes, allowing safe direct mapping via `zerocopy::FromBytes` and `zerocopy::IntoBytes`.
