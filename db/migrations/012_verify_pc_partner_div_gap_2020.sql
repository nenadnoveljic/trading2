-- Manual verification: PC Partner Group Ltd (e.g. 1263.HK / PCT.SI) —
-- first dividend 2012; annual dividend gap in 2020.
UPDATE companies
SET
    first_div_year = 2012,
    first_div_year_verified = TRUE,
    last_no_div_year = 2020,
    updated_at = NOW()
WHERE company_name = 'PC Partner Group Ltd';
