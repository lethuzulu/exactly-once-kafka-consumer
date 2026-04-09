
#[derive(Debug, thiserror::Error)]
pub enum ConsumerError {

    #[error("invalid amount: must be greater than zero pence")]
    InvalidAmount,

    #[error("message has no payload")]
    EmptyPayload,

    #[error("payload is not valid UTF-8")]
    InvalidUtf8,

    #[error("deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),

    #[error("missing required payload field: {0}")]
    MissingField(String),

    #[error("handler failed: {0}")]
    HandlerFailed(String),

    #[error("kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("configuration error: {0}")]
    Config(String),

    #[error("offset commit failed: {0}")]
    OffsetCommit(String),
}

impl ConsumerError {
    // Transient errors should NOT result in an offset commit.
    // Kafka will replay the message. The dedup table makes replay safe.
    pub fn is_transient(&self) -> bool {
        matches!(self, ConsumerError::Database(_) | ConsumerError::Kafka(_))
    }

    // Permanent errors should be dead-lettered and the offset committed.
    // Replaying a permanently broken message would loop forever.
    pub fn is_permanent(&self) -> bool {
        matches!(
            self,
            ConsumerError::Deserialization(_)
                | ConsumerError::EmptyPayload
                | ConsumerError::InvalidUtf8
                | ConsumerError::MissingField(_)
                | ConsumerError::HandlerFailed(_)
        )
    }
}