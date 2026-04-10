use crate::{
    config::ConsumerConfig,
    db::Db,
    error::ConsumerError,
    types::{CustomerId, KafkaOffset, MessageEnvelope, Money, PaymentId, ProcessingResult},
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
                // The current message always finishes before shutdown takes effect.
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        tracing::info!("consumer shutting down");
                        return Ok(());
                    }
                }

                msg = tokio::task::spawn_blocking({
                    // rdkafka's poll is synchronous, we use poll() in a blocking context so it does not starve the tokio runtime.
                    // StreamConsumer::recv() is the async alternative. We use recv() directly below
                    || ()
                }) => { let _ = msg; } // placeholder — see recv() below
            }

            // the actual async receive
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
                            // transient Kafka error — log and continue.
                            tracing::error!(error = %e, "kafka recv error");
                            continue;
                        }
                        Ok(msg) => {
                            // BorrowedMessage cannot cross await points.
                            let envelope = match extract_envelope(&msg) {
                                Ok(e)  => e,
                                Err(e) => {
                                    tracing::error!(
                                        error  = %e,
                                        topic  = msg.topic(),
                                        offset = msg.offset(),
                                        "envelope extraction failed"
                                    );
                                    // dead letter and commit — do not block on bad message
                                    let dead_env = make_dead_envelope(&msg);
                                    let _ = self.db.insert_dead_letter(&dead_env, &e.to_string()).await;
                                    self.commit_offset(&msg);
                                    continue;
                                }
                            };

                            //process with exactly-once semantics.
                            match self.process(&envelope).await {
                                Ok(ProcessingResult::Processed) => {
                                    tracing::info!(
                                        id        = %envelope.id,
                                        topic     = envelope.topic,
                                        partition = envelope.partition,
                                        offset    = %envelope.offset,
                                        "payment processed — wallet credited"
                                    );
                                    self.commit_offset(&msg);
                                }

                                Ok(ProcessingResult::Duplicate) => {
                                    tracing::debug!(
                                        topic     = envelope.topic,
                                        partition = envelope.partition,
                                        offset    = %envelope.offset,
                                        "duplicate offset — skipping, committing"
                                    );
                                    self.commit_offset(&msg);
                                }

                                Ok(ProcessingResult::DeadLetter { reason }) => {
                                    tracing::warn!(
                                        id        = %envelope.id,
                                        reason,
                                        "message dead-lettered"
                                    );
                                    let _ = self.db.insert_dead_letter(&envelope, &reason).await;
                                    self.commit_offset(&msg);
                                }

                                Err(e) if e.is_transient() => {
                                    tracing::warn!(
                                        error     = %e,
                                        topic     = envelope.topic,
                                        partition = envelope.partition,
                                        offset    = %envelope.offset,
                                        "transient error — will retry on replay"
                                    );
                                }

                                Err(e) => {
                                    // permanent unexpected error — dead letter.
                                    tracing::error!(error = %e, "unexpected processing error");
                                    let _ = self.db.insert_dead_letter(&envelope, &e.to_string()).await;
                                    self.commit_offset(&msg);
                                }
                            }
                        }
                    }
                }
            }
        }
    
    }

    async fn process(
        &self,
        envelope: &MessageEnvelope,
    ) -> Result<ProcessingResult, ConsumerError> {
        let topic     = &envelope.topic;
        let partition = envelope.partition;
        let offset    = envelope.offset.value();

        if self.db.is_already_processed(topic, partition, offset).await? {
            return Ok(ProcessingResult::Duplicate);
        }

        let customer_id = envelope
            .payload["customer_id"]
            .as_str()
            .and_then(|s| uuid::Uuid::parse_str(s).ok())
            .map(CustomerId::from_uuid)
            .ok_or_else(|| ConsumerError::MissingField("customer_id".into()))?;

        let amount = envelope
            .payload["amount_pence"]
            .as_i64()
            .ok_or_else(|| ConsumerError::MissingField("amount_pence".into()))
            .and_then(Money::from_cents)?;

        let payment_id = envelope
            .payload["payment_id"]
            .as_str()
            .and_then(|s| uuid::Uuid::parse_str(s).ok())
            .map(PaymentId::from_uuid)
            .ok_or_else(|| ConsumerError::MissingField("payment_id".into()))?;

        let inserted = self
            .db
            .with_transaction(|mut tx| async move {
                Db::credit_wallet_in_tx(
                    &mut tx,
                    customer_id,
                    amount,
                    payment_id,
                    topic,
                    partition,
                    offset,
                )
                .await?;

                let inserted = Db::insert_processed_offset(&mut tx, topic, partition, offset)
                    .await?;

                Ok((inserted, tx))
            })
            .await?;

        if inserted {
            Ok(ProcessingResult::Processed)
        } else {
            Ok(ProcessingResult::Duplicate)
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

// create a minimal envelope for dead-lettering unextractable messages.
fn make_dead_envelope(msg: &BorrowedMessage<'_>) -> MessageEnvelope {
    MessageEnvelope {
        id:           Uuid::new_v4(),
        topic:        msg.topic().to_string(),
        partition:    msg.partition(),
        offset:       KafkaOffset::new(msg.offset()),
        key:          None,
        payload:      serde_json::json!({ "raw": format!("{:?}", msg.payload()) }),
        timestamp_ms: msg.timestamp().to_millis(),
        received_at:  Utc::now(),
    }
}
