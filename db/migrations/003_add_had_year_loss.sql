-- Migration: Add had_year_loss column to companies table
-- Date: 2025-02-21
-- Description: Track whether a company had a year with net loss (NULL = not checked)

ALTER TABLE companies 
ADD COLUMN had_year_loss BOOLEAN NULL;

-- Add comment for documentation
COMMENT ON COLUMN companies.had_year_loss IS 'Whether this company had any year with net loss (NULL = not checked yet)';
