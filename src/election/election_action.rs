use crate::types::Term;

/// Actions emitted by the election subsystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectionAction {
    None,
    Campaign,
    ElectedLeader,
    StepDown(Term),
    HeartbeatTimeout,
}
