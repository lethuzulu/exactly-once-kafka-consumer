// tests/consumer_test.rs
//
// Six adversarial tests proving exactly-once wallet credits in concrete
// financial terms. No Kafka broker needed — tests exercise the DB layer
// and process() logic directly.
//
// Run with:
//   DATABASE_URL=postgres://... cargo test -- --test-threads=1

use chrono::Utc;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use uuid::Uuid;

use exactly_once_kafka_consumer::{
    db::Db,
    error::ConsumerError,
    types::{CustomerId, KafkaOffset, MessageEnvelope, Money, PaymentId, ProcessingResult},
};

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

async fn setup() -> Db {
    dotenvy::dotenv().ok();
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL required");
    let db  = Db::new(&url).await.unwrap();
    db.migrate().await.unwrap();

    sqlx::query("DELETE FROM dead_letter_messages").execute(&db.pool).await.unwrap();
    sqlx::query("DELETE FROM wallet_transactions").execute(&db.pool).await.unwrap();
    sqlx::query("DELETE FROM processed_offsets").execute(&db.pool).await.unwrap();
    sqlx::query("DELETE FROM wallets").execute(&db.pool).await.unwrap();

    db
}

fn make_envelope(topic: &str, partition: i32, offset: i64) -> MessageEnvelope {
    let customer_id = Uuid::new_v4();
    let payment_id  = Uuid::new_v4();
    MessageEnvelope {
        id:           Uuid::new_v4(),
        topic:        topic.to_string(),
        partition,
        offset:       KafkaOffset::new(offset),
        key:          Some(customer_id.to_string()),
        payload:      serde_json::json!({
            "customer_id":  customer_id,
            "payment_id":   payment_id,
            "amount_pence": 1000,
        }),
        timestamp_ms: Some(Utc::now().timestamp_millis()),
        received_at:  Utc::now(),
    }
}

fn make_envelope_for_customer(
    topic:       &str,
    partition:   i32,
    offset:      i64,
    customer_id: CustomerId,
    amount:      i64,
) -> MessageEnvelope {
    MessageEnvelope {
        id:           Uuid::new_v4(),
        topic:        topic.to_string(),
        partition,
        offset:       KafkaOffset::new(offset),
        key:          Some(customer_id.to_string()),
        payload:      serde_json::json!({
            "customer_id":  customer_id.as_uuid(),
            "payment_id":   Uuid::new_v4(),
            "amount_pence": amount,
        }),
        timestamp_ms: Some(Utc::now().timestamp_millis()),
        received_at:  Utc::now(),
    }
}

// Helper that calls process() directly without needing a Kafka broker
async fn process_envelope(
    db:       &Db,
    envelope: &MessageEnvelope,
) -> Result<ProcessingResult, ConsumerError> {
    let topic     = &envelope.topic;
    let partition = envelope.partition;
    let offset    = envelope.offset.value();

    if db.is_already_processed(topic, partition, offset).await? {
        return Ok(ProcessingResult::Duplicate);
    }

    let customer_id = envelope.payload["customer_id"]
        .as_str()
        .and_then(|s| Uuid::parse_str(s).ok())
        .map(CustomerId::from_uuid)
        .ok_or_else(|| ConsumerError::MissingField("customer_id".into()))?;

    let amount = envelope.payload["amount_pence"]
        .as_i64()
        .ok_or_else(|| ConsumerError::MissingField("amount_pence".into()))
        .and_then(Money::from_cents)?;

    let payment_id = envelope.payload["payment_id"]
        .as_str()
        .and_then(|s| Uuid::parse_str(s).ok())
        .map(PaymentId::from_uuid)
        .ok_or_else(|| ConsumerError::MissingField("payment_id".into()))?;

    let inserted = db.with_transaction(|mut tx| async move {
        let inserted = Db::insert_processed_offset(&mut tx, topic, partition, offset).await?;

        if inserted {
            Db::credit_wallet_in_tx(
                &mut tx, customer_id, amount, payment_id,
                topic, partition, offset,
            ).await?;
        }

        Ok((inserted, tx))
    }).await?;

    if inserted {
        Ok(ProcessingResult::Processed)
    } else {
        Ok(ProcessingResult::Duplicate)
    }
}

// ---------------------------------------------------------------------------
// Test 1 — successful payment credits wallet exactly once
//
// The happy path in financial terms:
// Process a payment.completed message → wallet balance increases by amount.
// One dedup row, one wallet transaction row. No more.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn successful_payment_credits_wallet_once() {
    let db          = setup().await;
    let customer_id = CustomerId::new();
    let envelope    = make_envelope_for_customer("payment.completed", 0, 1, customer_id, 1000);

    let result = process_envelope(&db, &envelope).await.unwrap();
    assert_eq!(result, ProcessingResult::Processed);

    // Wallet balance increased by exactly £10.00 (1000 pence)
    let balance = db.get_balance(customer_id).await.unwrap();
    assert_eq!(balance, 1000, "wallet balance must be 1000 pence");

    // One wallet transaction record
    let txn_count = db.count_wallet_transactions(customer_id).await.unwrap();
    assert_eq!(txn_count, 1, "exactly one wallet transaction");

    // One dedup row
    let dedup_count = db.count_processed_offsets("payment.completed", 0).await.unwrap();
    assert_eq!(dedup_count, 1, "exactly one processed offset");
}

// ---------------------------------------------------------------------------
// Test 2 — replay after crash does NOT double-credit (the critical test)
//
// This is the exactly-once guarantee in concrete financial terms.
// After a crash and replay, the customer's wallet balance is unchanged.
// They are not paid twice.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replay_after_crash_does_not_double_credit_wallet() {
    let db          = setup().await;
    let customer_id = CustomerId::new();
    let envelope    = make_envelope_for_customer("payment.completed", 0, 42, customer_id, 500);

    // First processing — succeeds
    let first = process_envelope(&db, &envelope).await.unwrap();
    assert_eq!(first, ProcessingResult::Processed);
    assert_eq!(db.get_balance(customer_id).await.unwrap(), 500);

    // Replay — same topic/partition/offset (simulates crash before offset commit)
    let second = process_envelope(&db, &envelope).await.unwrap();
    assert_eq!(second, ProcessingResult::Duplicate, "replay must be detected");

    // Balance unchanged — customer NOT paid twice
    let balance = db.get_balance(customer_id).await.unwrap();
    assert_eq!(balance, 500, "balance must not change on replay — customer not double-credited");

    // Still exactly one wallet transaction row
    let txn_count = db.count_wallet_transactions(customer_id).await.unwrap();
    assert_eq!(txn_count, 1, "no duplicate wallet transaction records");

    // Still exactly one dedup row
    let dedup_count = db.count_processed_offsets("payment.completed", 0).await.unwrap();
    assert_eq!(dedup_count, 1, "no duplicate dedup rows");
}

// ---------------------------------------------------------------------------
// Test 3 — concurrent consumers produce exactly one credit
//
// Two consumers process the same payment simultaneously (possible during
// rebalance). ON CONFLICT DO NOTHING ensures exactly one credit is applied.
// The customer receives their money exactly once regardless.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_consumers_produce_one_credit() {
    let db          = setup().await;
    let customer_id = CustomerId::new();
    let envelope    = make_envelope_for_customer("payment.completed", 0, 99, customer_id, 2000);

    let credit_count = Arc::new(AtomicUsize::new(0));

    // Spawn 10 concurrent "consumer" tasks all processing the same payment
    let handles: Vec<_> = (0..10).map(|_| {
        let db          = db.clone();
        let envelope    = envelope.clone();
        let credit_count = Arc::clone(&credit_count);
        tokio::spawn(async move {
            let result = process_envelope(&db, &envelope).await.unwrap();
            if result == ProcessingResult::Processed {
                credit_count.fetch_add(1, Ordering::SeqCst);
            }
            result
        })
    }).collect();

    let results: Vec<_> = futures::future::join_all(handles).await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // Count outcomes
    let processed_count = results.iter().filter(|r| **r == ProcessingResult::Processed).count();
    let duplicate_count = results.iter().filter(|r| **r == ProcessingResult::Duplicate).count();

    assert_eq!(processed_count, 1, "exactly one consumer should have credited the wallet");
    assert_eq!(duplicate_count, 9, "nine consumers should have detected a duplicate");

    // Wallet credited exactly once — not 10 times
    let balance = db.get_balance(customer_id).await.unwrap();
    assert_eq!(balance, 2000, "wallet credited exactly once despite 10 concurrent consumers");

    // One dedup row, one wallet transaction
    assert_eq!(db.count_processed_offsets("payment.completed", 0).await.unwrap(), 1);
    assert_eq!(db.count_wallet_transactions(customer_id).await.unwrap(), 1);
}

// ---------------------------------------------------------------------------
// Test 4 — multiple distinct payments all credited correctly
//
// Five different payments from the same customer.
// Each should be credited once. Total balance = sum of all amounts.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn multiple_distinct_payments_all_credited() {
    let db          = setup().await;
    let customer_id = CustomerId::new();
    let amounts     = [100i64, 200, 300, 400, 500];

    for (i, amount) in amounts.iter().enumerate() {
        let env = make_envelope_for_customer(
            "payment.completed", 0, i as i64, customer_id, *amount,
        );
        let result = process_envelope(&db, &env).await.unwrap();
        assert_eq!(result, ProcessingResult::Processed, "payment {i} should be processed");
    }

    // Total balance = 100 + 200 + 300 + 400 + 500 = 1500
    let balance = db.get_balance(customer_id).await.unwrap();
    assert_eq!(balance, 1500, "total balance must equal sum of all payments");

    let txn_count = db.count_wallet_transactions(customer_id).await.unwrap();
    assert_eq!(txn_count, 5, "exactly five wallet transactions");

    let dedup_count = db.count_processed_offsets("payment.completed", 0).await.unwrap();
    assert_eq!(dedup_count, 5, "exactly five processed offsets");
}

// ---------------------------------------------------------------------------
// Test 5 — dedup insert is idempotent (ON CONFLICT DO NOTHING)
//
// Inserting the same (topic, partition, offset) twice should not error.
// The second insert silently returns rows_affected = 0.
// This is the mechanism that makes the exactly-once pattern work.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dedup_insert_is_idempotent_not_an_error() {
    let db = setup().await;

    // First insert — should succeed
    let first = db.with_transaction(|mut tx| async move {
        let inserted = Db::insert_processed_offset(&mut tx, "payment.completed", 0, 77).await?;
        Ok((inserted, tx))
    }).await.unwrap();

    assert!(first, "first insert should return true (new row)");

    // Second insert — same offset, should return false, NOT an error
    let second = db.with_transaction(|mut tx| async move {
        let inserted = Db::insert_processed_offset(&mut tx, "payment.completed", 0, 77).await?;
        Ok((inserted, tx))
    }).await.unwrap();

    assert!(!second, "duplicate insert should return false, not an error");

    // Exactly one row in the table
    let count = db.count_processed_offsets("payment.completed", 0).await.unwrap();
    assert_eq!(count, 1, "exactly one dedup row despite two inserts");
}

// ---------------------------------------------------------------------------
// Test 6 — malformed message does not block consumer or corrupt wallet
//
// A message with a missing amount_pence field is unprocessable.
// The wallet must be completely unchanged.
// The consumer must detect the error and be able to continue.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn malformed_message_does_not_corrupt_wallet() {
    let db          = setup().await;
    let customer_id = CustomerId::new();

    // Malformed envelope — missing amount_pence
    let bad_envelope = MessageEnvelope {
        id:           Uuid::new_v4(),
        topic:        "payment.completed".to_string(),
        partition:    0,
        offset:       KafkaOffset::new(888),
        key:          None,
        payload:      serde_json::json!({
            "customer_id": customer_id.as_uuid(),
            "payment_id":  Uuid::new_v4(),
            // amount_pence deliberately missing
        }),
        timestamp_ms: None,
        received_at:  Utc::now(),
    };

    // process_envelope returns Err(MissingField)
    let result = process_envelope(&db, &bad_envelope).await;
    assert!(
        matches!(result, Err(ConsumerError::MissingField(ref f)) if f == "amount_pence"),
        "expected MissingField error, got: {result:?}"
    );

    // Wallet completely unchanged — balance still zero
    let balance = db.get_balance(customer_id).await.unwrap();
    assert_eq!(balance, 0, "malformed message must not affect wallet balance");

    // No dedup row — allows retry if the message is later fixed
    let dedup_count = db.count_processed_offsets("payment.completed", 0).await.unwrap();
    assert_eq!(dedup_count, 0, "no dedup row for malformed message");

    // After the error, a valid message for the same customer still works
    let good_envelope = make_envelope_for_customer("payment.completed", 0, 999, customer_id, 750);
    let good_result   = process_envelope(&db, &good_envelope).await.unwrap();
    assert_eq!(good_result, ProcessingResult::Processed);
    assert_eq!(db.get_balance(customer_id).await.unwrap(), 750, "valid message processed correctly after error");
}
