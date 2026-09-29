/// Role of a Raft node.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Role {
    #[default]
    Follower = 0,
    Candidate = 1,
    Leader = 2,
}
