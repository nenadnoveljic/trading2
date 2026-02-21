-- Migration: Add stock info cache columns to companies table
-- Date: 2025-02-21
-- Description: Cache first_div_year, has_gaps, AL_ratio from yfinance

ALTER TABLE companies 
ADD COLUMN first_div_year INTEGER NULL;

ALTER TABLE companies 
ADD COLUMN has_div_gaps BOOLEAN NULL;

ALTER TABLE companies 
ADD COLUMN al_ratio NUMERIC(10,2) NULL;

-- Add comments for documentation
COMMENT ON COLUMN companies.first_div_year IS 'Year of first dividend payment (cached from yfinance)';
COMMENT ON COLUMN companies.has_div_gaps IS 'Whether there are gaps in annual dividend payments (cached from yfinance)';
COMMENT ON COLUMN companies.al_ratio IS 'Assets/Liabilities ratio (cached from yfinance)';
