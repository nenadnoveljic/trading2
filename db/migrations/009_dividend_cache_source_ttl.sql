-- IRBANK / yfinance dividend bundle metadata and TTL (90-day refresh for irbank)
ALTER TABLE companies ADD COLUMN dividend_data_source VARCHAR(20) NULL;
ALTER TABLE companies ADD COLUMN dividend_cache_expires_at TIMESTAMPTZ NULL;

COMMENT ON COLUMN companies.dividend_data_source IS 'Origin of cached dividend bundle: irbank | yfinance; NULL legacy or unknown';
COMMENT ON COLUMN companies.dividend_cache_expires_at IS 'When irbank bundle should be refreshed; NULL for yfinance or verified/manual';
