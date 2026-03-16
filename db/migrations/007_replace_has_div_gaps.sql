-- Add new integer column
ALTER TABLE companies ADD COLUMN last_no_div_year INTEGER NULL;
COMMENT ON COLUMN companies.last_no_div_year IS 'Most recent year with no dividend (0 = checked, no gap found; NULL = not checked)';

-- Clear permanent disqualifications for dividends_gap so they get re-evaluated
UPDATE companies
SET is_disqualified = FALSE,
    disqualified_reason_id = NULL
WHERE disqualified_reason_id = (SELECT id FROM exclusion_reasons WHERE code = 'dividends_gap');

-- Drop old boolean column
ALTER TABLE companies DROP COLUMN has_div_gaps;
