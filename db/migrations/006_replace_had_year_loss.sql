-- Add new column
ALTER TABLE companies ADD COLUMN last_year_loss INTEGER NULL;
COMMENT ON COLUMN companies.last_year_loss IS 'Year of most recent net loss (0 = checked, no loss found; NULL = not checked)';

-- Clear is_disqualified for companies disqualified due to year_loss
-- so they get re-evaluated with the new 20-year rule
UPDATE companies
SET is_disqualified = FALSE,
    disqualified_reason_id = NULL
WHERE disqualified_reason_id = (SELECT id FROM exclusion_reasons WHERE code = 'year_loss');

-- Drop old boolean column
ALTER TABLE companies DROP COLUMN had_year_loss;
