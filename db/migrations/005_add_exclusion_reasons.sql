-- Migration: Add exclusion_reasons lookup table
-- This normalizes the inconsistent reason strings in the companies table

-- Step 1: Create lookup table
CREATE TABLE exclusion_reasons (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) UNIQUE NOT NULL,
    description TEXT,
    is_permanent BOOLEAN DEFAULT FALSE
);

-- Insert standard reasons
INSERT INTO exclusion_reasons (code, description, is_permanent) VALUES
    ('year_loss', 'Company had a year with net loss', TRUE),
    ('dividends_gap', 'Company has gaps in dividend history', TRUE),
    ('unknown', 'Unknown reason', TRUE),
    ('al_ratio', 'Assets/Liabilities ratio below threshold', FALSE),
    ('current_ratio', 'Current ratio below threshold', FALSE),
    ('quick_ratio', 'Quick ratio below threshold', FALSE),
    ('cash_debt', 'Cash does not cover debt', FALSE),
    ('short_div_history', 'Short dividend payment history', FALSE),
    ('pe_ratio', 'P/E ratio concerns', FALSE),
    ('not_tradeable', 'Stock is not tradeable', FALSE);

-- Step 2: Add foreign key columns to companies table
ALTER TABLE companies 
    ADD COLUMN disqualified_reason_id INTEGER REFERENCES exclusion_reasons(id),
    ADD COLUMN defer_reason_id INTEGER REFERENCES exclusion_reasons(id);

-- Step 3: Migrate existing disqualified_reason strings to IDs
UPDATE companies SET disqualified_reason_id = (
    SELECT id FROM exclusion_reasons WHERE code = 'year_loss'
) WHERE disqualified_reason IN ('year loss', 'loss');

UPDATE companies SET disqualified_reason_id = (
    SELECT id FROM exclusion_reasons WHERE code = 'dividends_gap'
) WHERE disqualified_reason IN ('dividends gap', 'dividend gap');

UPDATE companies SET disqualified_reason_id = (
    SELECT id FROM exclusion_reasons WHERE code = 'unknown'
) WHERE disqualified_reason = 'unknown';

-- Step 4: Migrate existing dont_consider_reason strings to IDs
UPDATE companies SET defer_reason_id = (
    SELECT id FROM exclusion_reasons WHERE code = 'al_ratio'
) WHERE dont_consider_reason LIKE 'AL_ratio%' 
   OR dont_consider_reason LIKE 'assets%';

UPDATE companies SET defer_reason_id = (
    SELECT id FROM exclusion_reasons WHERE code = 'current_ratio'
) WHERE dont_consider_reason IN ('current ratio', 'low current ratio');

UPDATE companies SET defer_reason_id = (
    SELECT id FROM exclusion_reasons WHERE code = 'quick_ratio'
) WHERE dont_consider_reason = 'quick ratio';

UPDATE companies SET defer_reason_id = (
    SELECT id FROM exclusion_reasons WHERE code = 'cash_debt'
) WHERE dont_consider_reason IN (
    'not enough cash to cover debt', 
    'cash doesn''t cover debt', 
    'insufficient cash to cover the debt'
);

UPDATE companies SET defer_reason_id = (
    SELECT id FROM exclusion_reasons WHERE code = 'short_div_history'
) WHERE dont_consider_reason IN ('short dividends history', 'dividend_history');

UPDATE companies SET defer_reason_id = (
    SELECT id FROM exclusion_reasons WHERE code = 'pe_ratio'
) WHERE dont_consider_reason IN ('P/E', 'inconsistent p/e ratio');

UPDATE companies SET defer_reason_id = (
    SELECT id FROM exclusion_reasons WHERE code = 'not_tradeable'
) WHERE dont_consider_reason = 'not tradeable';

UPDATE companies SET defer_reason_id = (
    SELECT id FROM exclusion_reasons WHERE code = 'unknown'
) WHERE dont_consider_reason = 'UNKNOWN';
