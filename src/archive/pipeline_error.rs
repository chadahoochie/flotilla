/// Errors emitted by the archive pipeline queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineError {
    QueueFull,
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::QueueFull => write!(f, "archive pipeline queue is at maximum capacity"),
        }
    }
}

impl std::error::Error for PipelineError {}
