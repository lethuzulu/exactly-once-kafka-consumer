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

CREATE INDEX wallet_txn_wallet_idx ON wallet_transactions (wallet_id, created_at DESC);

CREATE TABLE processed_offsets (
    topic        TEXT        NOT NULL,
    partition    INT         NOT NULL,
    "offset"     BIGINT      NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (topic, partition, "offset")
);

CREATE INDEX processed_offsets_tp_idx
    ON processed_offsets (topic, partition, "offset" DESC);

CREATE TABLE dead_letter_messages (
    id            UUID        PRIMARY KEY,
    topic         TEXT        NOT NULL,
    partition     INT         NOT NULL,
    "offset"      BIGINT      NOT NULL,
    message_key   TEXT,
    payload       JSONB       NOT NULL DEFAULT '{}',
    error_reason  TEXT        NOT NULL,
    received_at   TIMESTAMPTZ NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX dead_letter_topic_idx ON dead_letter_messages (topic, created_at DESC);