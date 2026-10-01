use std::fmt;
use std::io;

/// Errors occurring during server listener setup and network operations.
#[derive(Debug)]
pub enum ServerError {
    Io(io::Error),
    BindFailed(String),
    Closed,
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "Server I/O error: {e}"),
            Self::BindFailed(msg) => write!(f, "Failed to bind server: {msg}"),
            Self::Closed => write!(f, "Server listener closed"),
        }
    }
}

impl std::error::Error for ServerError {}

impl From<io::Error> for ServerError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}
