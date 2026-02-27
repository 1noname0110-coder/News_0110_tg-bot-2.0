UPDATE published_news
SET status = CASE
    WHEN status IN ('prepared', 'sending', 'sent', 'partial', 'failed') THEN status
    WHEN status = 'success' THEN 'sent'
    ELSE 'prepared'
END;

ALTER TABLE published_news
    ADD CONSTRAINT ck_published_news_status
    CHECK (status IN ('prepared', 'sending', 'sent', 'partial', 'failed'));
