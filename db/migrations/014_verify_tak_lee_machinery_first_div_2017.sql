-- Manual verification: Tak Lee Machinery Holdings Ltd (2102.HK) — first dividend 2017.
UPDATE companies
SET
    first_div_year = 2017,
    first_div_year_verified = TRUE,
    updated_at = NOW()
WHERE company_name = 'Tak Lee Machinery Holdings Ltd';
