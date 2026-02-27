ALTER TABLE published_news
    ADD COLUMN status VARCHAR(16) NOT NULL DEFAULT 'prepared';

UPDATE published_news
SET status = CASE
    WHEN json_extract(COALESCE(quality_metrics, '{}'), '$.delivery_status') IN ('prepared', 'sending', 'sent', 'failed', 'partial')
        THEN json_extract(COALESCE(quality_metrics, '{}'), '$.delivery_status')
    WHEN json_extract(COALESCE(quality_metrics, '{}'), '$.delivery_status') = 'success'
        THEN 'sent'
    ELSE 'prepared'
END;

CREATE INDEX IF NOT EXISTS ix_published_news_status_period
    ON published_news (status, period_type, period_start, period_end);
