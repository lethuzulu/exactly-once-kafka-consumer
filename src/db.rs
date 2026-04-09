use sqlx::{PgPool, Postgres, Transaction, postgres::PgPoolOptions};

use crate::{error::ConsumerError, types::CustomerId};

pub struct Db {
    pub pool: PgPool,
}

impl Db {
    pub async fn new(database_url: &str) -> Result<Self, ConsumerError> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }

    pub async fn migrate(&self) -> Result<(), ConsumerError> {
        sqlx::migrate!("./migrations")
            .run(&self.pool)
            .await
            .map_err(|e| ConsumerError::Database(e.into()))?;
        Ok(())
    }

    pub async fn with_transaction<'a, F, Fut, T>(&self, f: F) -> Result<T, ConsumerError>
    where
        F: FnOnce(Transaction<'a, Postgres>) -> Fut,
        Fut: Future<Output = Result<(T, Transaction<'a, Postgres>), ConsumerError>>,
    {
        let tx = self.pool.begin().await?;
        let (result, tx) = f(tx).await?;
        tx.commit().await?;
        Ok(result)
    }
}

impl Db {
    pub async fn get_balance(&self, customer_id: CustomerId) -> Result<i64, ConsumerError> {
        let _ = customer_id;
        let balance = sqlx::query_scalar!(
            "SELECT balance_pence FROM wallets WHERE customer_id = $1",
            customer_id.as_uuid(),
        )
        .fetch_optional(&self.pool)
        .await?
        .unwrap_or(0);

        Ok(balance)
    }

    pub async fn count_wallet_transactions(
        &self,
        customer_id: CustomerId,
    ) -> Result<i64, ConsumerError> {
        let count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)
            FROM   wallet_transactions wt
            JOIN   wallets w ON wt.wallet_id = w.id
            WHERE  w.customer_id = $1
            "#,
            customer_id.as_uuid(),
        )
        .fetch_one(&self.pool)
        .await?
        .unwrap_or(0);

        Ok(count)
    }

    pub async fn credit_wallet_in_tx(
        tx: &mut Transaction<'_, Postgres>,
        customer_id: CustomerId,
        amount: Money,
        payment_id: PaymentId,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<(), ConsumerError> {
        sqlx::query!(
            r#"
        INSERT INTO wallets (customer_id, balance_pence)
        VALUES ($1, $2)
        ON CONFLICT (customer_id)
        DO UPDATE SET
            balance_pence = wallets.balance_pence + $2,
            updated_at    = now()
        "#,
            customer_id.as_uuid(),
            amount.pence(),
        )
        .execute(&mut **tx)
        .await?;

        sqlx::query!(
            r#"
        INSERT INTO wallet_transactions
            (wallet_id, amount_pence, payment_id, kafka_topic, kafka_partition, kafka_offset)
        SELECT id, $2, $3, $4, $5, $6
        FROM   wallets WHERE customer_id = $1
        "#,
            customer_id.as_uuid(),
            amount.pence(),
            payment_id.as_uuid(),
            topic,
            partition,
            offset,
        )
        .execute(&mut **tx)
        .await?;

        Ok(())
    }
}

impl Db {
    pub async fn is_already_processed(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<bool, ConsumerError> {
        let exists = sqlx::query_scalar!(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM processed_offsets
                WHERE  topic     = $1
                AND    partition = $2
                AND    offset    = $3
            ) AS "exists!"
            "#,
            topic,
            partition,
            offset,
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(exists)
    }

    pub async fn insert_processed_offset(
        tx: &mut Transaction<'_, Postgres>,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<bool, ConsumerError> {
        let rows_affected = sqlx::query!(
            r#"
            INSERT INTO processed_offsets (topic, partition, offset)
            VALUES ($1, $2, $3)
            ON CONFLICT (topic, partition, offset) DO NOTHING
            "#,
            topic,
            partition,
            offset,
        )
        .execute(&mut **tx)
        .await?
        .rows_affected();

        Ok(rows_affected > 0)
    }

    pub async fn count_processed_offsets(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<i64, ConsumerError> {
        let count = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM processed_offsets WHERE topic = $1 AND partition = $2",
            topic,
            partition,
        )
        .fetch_one(&self.pool)
        .await?
        .unwrap_or(0);

        Ok(count)
    }
}
