-- Cleanup duplicates in rejected_news and enforce uniqueness by raw_news_id.

-- Keep the earliest row (smallest id) for each raw_news_id.
DELETE FROM rejected_news
WHERE id IN (
    SELECT id
    FROM (
        SELECT id,
               ROW_NUMBER() OVER (PARTITION BY raw_news_id ORDER BY id ASC) AS rn
        FROM rejected_news
    ) t
    WHERE t.rn > 1
);

-- Add a unique index/constraint to prevent duplicates.
CREATE UNIQUE INDEX IF NOT EXISTS uix_rejected_news_raw_news_id
    ON rejected_news (raw_news_id);
