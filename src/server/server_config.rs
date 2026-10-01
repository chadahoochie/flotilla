use std::net::SocketAddr;

/// Configuration for Flotilla transport server listeners.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind_addr: SocketAddr,
    pub max_connections: usize,
}

impl ServerConfig {
    /// Create a new server listener configuration.
    pub fn new(bind_addr: SocketAddr) -> Self {
        Self {
            bind_addr,
            max_connections: 1024,
        }
    }

    /// Set maximum concurrent connections.
    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.max_connections = max;
        self
    }
}
