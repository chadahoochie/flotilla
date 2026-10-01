use std::time::Duration;

/// Configuration parameters for connecting Flotilla clients to cluster nodes.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub endpoints: Vec<String>,
    pub timeout: Duration,
    pub retry_attempts: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            endpoints: vec!["127.0.0.1:9001".to_string()],
            timeout: Duration::from_millis(500),
            retry_attempts: 3,
        }
    }
}

impl ClientConfig {
    /// Create a new client configuration with default timeout and retries.
    pub fn new(endpoints: Vec<String>) -> Self {
        Self {
            endpoints,
            ..Default::default()
        }
    }

    /// Set the timeout duration for operations.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set maximum retry attempts.
    pub fn with_retries(mut self, retries: usize) -> Self {
        self.retry_attempts = retries;
        self
    }
}
