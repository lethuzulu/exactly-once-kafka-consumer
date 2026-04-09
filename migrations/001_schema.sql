CREATE TABLE wallets (
    id            UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id   UUID    NOT NULL UNIQUE,
    balance_pence BIGINT  NOT NULL DEFAULT 0,
    currency      TEXT    NOT NULL DEFAULT 'GBP',
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT balance_non_negative CHECK (balance_pence >= 0)
);

CREATE TABLE wallet_transactions (
    id               UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
    wallet_id        UUID    NOT NULL REFERENCES wallets(id),
    amount_pence     BIGINT  NOT NULL,
    payment_id       UUID    NOT NULL,
    kafka_topic      TEXT    NOT NULL,
    kafka_partition  INT     NOT NULL,
    kafka_offset     BIGINT  NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT amount_positive CHECK (amount_pence > 0)
);