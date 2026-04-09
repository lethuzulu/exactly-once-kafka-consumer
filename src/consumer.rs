use crate::{
    config::ConsumerConfig,
    db::Db,
    error::ConsumerError,
    types::{KafkaOffset, MessageEnvelope},
};

use chrono::Utc;
use rdkafka::{
    client::ClientContext,
    config::ClientConfig,
    consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer},
    message::{BorrowedMessage, Message},
};
use tokio::sync::watch;
use uuid::Uuid;

pub struct RebalanceLogger;

impl ClientContext for RebalanceLogger {}

impl ConsumerContext for RebalanceLogger {
    fn pre_rebalance(&self, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Revoke(tpl) => {
                let parts: Vec<_> = tpl
                    .elements()
                    .iter()
                    .map(|e| format!("{}:{}", e.topic(), e.partition()))
                    .collect();
                tracing::info!(partitions = ?parts, "pre_rebalance: partitions revoked");
            }
            Rebalance::Assign(tpl) => {
                let parts: Vec<_> = tpl
                    .elements()
                    .iter()
                    .map(|e| format!("{}:{}", e.topic(), e.partition()))
                    .collect();
                tracing::info!(partitions = ?parts, "pre_rebalance: partitions assigned");
            }
            Rebalance::Error(e) => tracing::error!(%e, "rebalance error"),
        }
    }

    fn post_rebalance(&self, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Revoke(_) => tracing::info!("post_rebalance: revoke complete"),
            Rebalance::Assign(_) => tracing::info!("post_rebalance: assign complete"),
            Rebalance::Error(e) => tracing::error!(%e, "post_rebalance error"),
        }
    }
}

type LoggingConsumer = StreamConsumer<RebalanceLogger>;

pub struct KafkaConsumer {
    inner: LoggingConsumer,
    db: Db,
    config: ConsumerConfig,
}

impl KafkaConsumer {
    pub fn new(config: ConsumerConfig, db: Db) -> Result<Self, ConsumerError> {
        let consumer: LoggingConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("group.id", &config.group_id)
            // CRITICAL: disable auto-commit.
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", &config.auto_offset_reset)
            .set("session.timeout.ms", config.session_timeout_ms.to_string())
            .set(
                "heartbeat.interval.ms",
                config.heartbeat_interval_ms.to_string(),
            )
            .set("enable.partition.eof", "false")
            .create_with_context(RebalanceLogger)
            .map_err(ConsumerError::Kafka)?;

        let topic_refs: Vec<&str> = config.topics.iter().map(|s| s.as_str()).collect();
        consumer
            .subscribe(&topic_refs)
            .map_err(ConsumerError::Kafka)?;

        tracing::info!(
            topics = ?config.topics,
            group  = config.group_id,
            "kafka consumer subscribed"
        );

        Ok(Self {
            inner: consumer,
            db,
            config,
        })
    }
}

impl KafkaConsumer {
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) -> Result<(), ConsumerError> {
        tracing::info!("consumer loop started");

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        tracing::info!("consumer shutting down cleanly");
                        return Ok(());
                    }
                }

                result = self.inner.recv() => {
                    match result {
                        Err(e) => {
                            tracing::error!(error = %e, "kafka recv error");
                            continue;
                        }
                        Ok(msg) => {
                            let envelope = match extract_envelope(&msg) {
                                Ok(e)  => e,
                                Err(e) => {
                                    tracing::error!(
                                        error  = %e,
                                        topic  = msg.topic(),
                                        offset = msg.offset(),
                                        "envelope extraction failed"
                                    );
                                    self.commit_offset(&msg);
                                    continue;
                                }
                            };
                        }
                    }
                }
            }
        }
    }

    fn commit_offset(&self, msg: &BorrowedMessage<'_>) {
        if let Err(e) = self.inner.commit_message(msg, CommitMode::Async) {
            tracing::error!(
                error     = %e,
                topic     = msg.topic(),
                partition = msg.partition(),
                offset    = msg.offset(),
                "offset commit failed — message may be replayed"
            );
        }
    }
}

pub fn extract_envelope(msg: &BorrowedMessage<'_>) -> Result<MessageEnvelope, ConsumerError> {
    let payload_bytes = msg.payload().ok_or(ConsumerError::EmptyPayload)?;

    let payload_str = std::str::from_utf8(payload_bytes).map_err(|_| ConsumerError::InvalidUtf8)?;

    let payload: serde_json::Value =
        serde_json::from_str(payload_str).map_err(ConsumerError::Deserialization)?;

    let key = msg
        .key()
        .and_then(|k| std::str::from_utf8(k).ok())
        .map(String::from);

    Ok(MessageEnvelope {
        id: Uuid::new_v4(),
        topic: msg.topic().to_string(),
        partition: msg.partition(),
        offset: KafkaOffset::new(msg.offset()),
        key,
        payload,
        timestamp_ms: msg.timestamp().to_millis(),
        received_at: Utc::now(),
    })
}
