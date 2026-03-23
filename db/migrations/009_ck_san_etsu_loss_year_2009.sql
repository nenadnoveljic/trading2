-- Data fix: CK San-Etsu Co Ltd had a net loss in 2009
UPDATE companies
SET last_year_loss = GREATEST(COALESCE(last_year_loss, 0), 2009),
    updated_at = NOW()
WHERE company_name = 'CK San-Etsu Co Ltd';
