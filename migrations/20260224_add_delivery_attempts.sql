CREATE TABLE IF NOT EXISTS delivery_attempts (
    id INTEGER PRIMARY KEY,
    digest_id INTEGER NULL REFERENCES published_news(id),
    chunk_idx INTEGER NOT NULL,
    status VARCHAR(32) NOT NULL,
    error_type VARCHAR(64) NULL,
    error_message TEXT NULL,
    attempted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_delivery_attempts_digest_chunk
    ON delivery_attempts (digest_id, chunk_idx);

CREATE INDEX IF NOT EXISTS ix_delivery_attempts_attempted_at
    ON delivery_attempts (attempted_at);
