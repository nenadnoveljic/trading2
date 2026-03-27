-- One-off after IRBANK year_loss + bundle semantics: force refresh from irbank.net on next screen.
-- Skips rows with first_div_year_verified (still use Postgres bundle for dividends by design).
UPDATE companies
SET
    dividend_data_source = NULL,
    dividend_cache_expires_at = NULL
WHERE dividend_data_source = 'irbank'
  AND COALESCE(first_div_year_verified, false) = false;
