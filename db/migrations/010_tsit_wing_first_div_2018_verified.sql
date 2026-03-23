-- Data fix: Tsit Wing International Holdings Ltd first dividend 2018 manually verified
UPDATE companies
SET first_div_year = 2018,
    first_div_year_verified = TRUE,
    updated_at = NOW()
WHERE company_name = 'Tsit Wing International Holdings Ltd';
