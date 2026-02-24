-- Add foreign keys and performance indexes for period/stat queries.
-- Migration is written for SQLite deployments.

PRAGMA foreign_keys=OFF;

BEGIN TRANSACTION;

-- Rebuild raw_news to add FK(source_id -> sources.id)
CREATE TABLE raw_news_new (
    id INTEGER PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES sources(id),
    title VARCHAR(1024) NOT NULL,
    summary TEXT NOT NULL,
    url VARCHAR(1024) NOT NULL,
    external_id VARCHAR(512) NOT NULL,
    published_at DATETIME NOT NULL,
    collected_at DATETIME,
    tags JSON,
    CONSTRAINT uix_source_external UNIQUE (source_id, external_id)
);

INSERT INTO raw_news_new (id, source_id, title, summary, url, external_id, published_at, collected_at, tags)
SELECT rn.id, rn.source_id, rn.title, rn.summary, rn.url, rn.external_id, rn.published_at, rn.collected_at, rn.tags
FROM raw_news rn
WHERE EXISTS (SELECT 1 FROM sources s WHERE s.id = rn.source_id);

DROP TABLE raw_news;
ALTER TABLE raw_news_new RENAME TO raw_news;

-- Rebuild rejected_news to add FK(raw_news_id -> raw_news.id, source_id -> sources.id)
CREATE TABLE rejected_news_new (
    id INTEGER PRIMARY KEY,
    raw_news_id INTEGER NOT NULL REFERENCES raw_news(id),
    source_id INTEGER NOT NULL REFERENCES sources(id),
    reason VARCHAR(255) NOT NULL,
    rejected_at DATETIME,
    CONSTRAINT uix_rejected_news_raw_news_id UNIQUE (raw_news_id)
);

INSERT INTO rejected_news_new (id, raw_news_id, source_id, reason, rejected_at)
SELECT rj.id, rj.raw_news_id, rj.source_id, rj.reason, rj.rejected_at
FROM rejected_news rj
WHERE EXISTS (SELECT 1 FROM raw_news rn WHERE rn.id = rj.raw_news_id)
  AND EXISTS (SELECT 1 FROM sources s WHERE s.id = rj.source_id);

DROP TABLE rejected_news;
ALTER TABLE rejected_news_new RENAME TO rejected_news;

-- New and existing index coverage for frequent filters.
CREATE INDEX IF NOT EXISTS ix_raw_news_published_at
    ON raw_news (published_at);

CREATE INDEX IF NOT EXISTS ix_published_news_period
    ON published_news (period_type, period_start, period_end);

CREATE INDEX IF NOT EXISTS ix_rejected_news_rejected_at
    ON rejected_news (rejected_at);

CREATE INDEX IF NOT EXISTS ix_rejected_news_raw_news_id
    ON rejected_news (raw_news_id);

COMMIT;

PRAGMA foreign_keys=ON;
