-- Manual verification: COSCO SHIPPING Holdings Co Ltd (e.g. 1919.HK / CICOF) — first dividend 2022.
UPDATE companies
SET
    first_div_year = 2022,
    first_div_year_verified = TRUE,
    updated_at = NOW()
WHERE company_name = 'COSCO SHIPPING Holdings Co Ltd';
