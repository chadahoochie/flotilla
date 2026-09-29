/// RPC message discriminant.
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum MsgType {
    #[default]
    Unknown = 0,
    RequestVoteArgs = 1,
    RequestVoteReply = 2,
    AppendEntriesArgs = 3,
    AppendEntriesReply = 4,
    HeartbeatArgs = 5,
    HeartbeatReply = 6,
}

impl MsgType {
    /// Convert a raw `u16` into a `MsgType` if valid.
    pub const fn from_u16(val: u16) -> Option<Self> {
        match val {
            1 => Some(Self::RequestVoteArgs),
            2 => Some(Self::RequestVoteReply),
            3 => Some(Self::AppendEntriesArgs),
            4 => Some(Self::AppendEntriesReply),
            5 => Some(Self::HeartbeatArgs),
            6 => Some(Self::HeartbeatReply),
            _ => None,
        }
    }

    /// Return the discriminant as `u16`.
    pub const fn to_u16(self) -> u16 {
        self as u16
    }
}
