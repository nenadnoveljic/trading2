-- Migration: Add had_quarter_loss column to companies table
-- Date: 2025-11-23
-- Description: Adds a boolean column to track whether a company had a quarterly loss

ALTER TABLE companies 
ADD COLUMN had_quarter_loss BOOLEAN DEFAULT FALSE NOT NULL;

-- Add comment for documentation
COMMENT ON COLUMN companies.had_quarter_loss IS 'Whether this company had a quarterly loss';

