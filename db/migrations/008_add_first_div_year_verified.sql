ALTER TABLE companies ADD COLUMN first_div_year_verified BOOLEAN NOT NULL DEFAULT FALSE;
COMMENT ON COLUMN companies.first_div_year_verified IS 'Whether first_div_year has been manually verified as the true first dividend year';
