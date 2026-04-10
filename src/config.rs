use crate::error::ConsumerError;

// All configuration for the Kafka consumer.
// Validated at startup — a misconfigured consumer fails fast.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub brokers: String,
    pub group_id: String,
    pub topics: Vec<String>,
    pub session_timeout_ms: u32,
    pub heartbeat_interval_ms: u32,
    pub auto_offset_reset: String,
}

impl ConsumerConfig {
    pub fn from_env() -> Result<Self, ConsumerError> {
        let config = Self {
            brokers: std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into()),
            group_id: std::env::var("KAFKA_GROUP_ID")
                .map_err(|_| ConsumerError::Config("KAFKA_GROUP_ID must be set".into()))?,
            topics: std::env::var("KAFKA_TOPICS")
                .map_err(|_| ConsumerError::Config("KAFKA_TOPICS must be set".into()))?
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            session_timeout_ms: std::env::var("KAFKA_SESSION_TIMEOUT_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(6000),
            heartbeat_interval_ms: std::env::var("KAFKA_HEARTBEAT_INTERVAL_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(2000),
            auto_offset_reset: std::env::var("KAFKA_AUTO_OFFSET_RESET")
                .unwrap_or_else(|_| "earliest".into()),
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConsumerError> {
        if self.brokers.trim().is_empty() {
            return Err(ConsumerError::Config("brokers must not be empty".into()));
        }
        if self.group_id.trim().is_empty() {
            return Err(ConsumerError::Config("group_id must not be empty".into()));
        }
        if self.topics.is_empty() {
            return Err(ConsumerError::Config(
                "at least one topic must be configured".into(),
            ));
        }
        if !["earliest", "latest", "error"].contains(&self.auto_offset_reset.as_str()) {
            return Err(ConsumerError::Config(
                "auto_offset_reset must be earliest, latest, or error".into(),
            ));
        }
        if self.heartbeat_interval_ms >= self.session_timeout_ms {
            return Err(ConsumerError::Config(
                "heartbeat_interval_ms must be less than session_timeout_ms".into(),
            ));
        }
        Ok(())
    }
}
