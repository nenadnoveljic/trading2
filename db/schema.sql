-- PostgreSQL Stock Database Schema
-- Database: stocks
-- Note: This schema assumes the 'stocks' database already exists
-- Run setup.sh to create the database and execute this schema

-- Table: stock_markets
-- Stores information about different stock markets/exchanges
CREATE TABLE stock_markets (
    id SERIAL PRIMARY KEY,
    abbreviation VARCHAR(10) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    not_tradeable_until TIMESTAMP NULL
);

-- Table: companies
-- Stores company information (normalized - one company can have multiple market listings)
CREATE TABLE companies (
    id SERIAL PRIMARY KEY,
    company_name VARCHAR(255) UNIQUE NOT NULL,
    had_quarter_loss BOOLEAN DEFAULT FALSE NOT NULL,
    is_disqualified BOOLEAN DEFAULT FALSE NOT NULL,
    disqualified_reason TEXT NULL,
    dont_consider_until TIMESTAMP NULL,
    dont_consider_reason TEXT NULL
);

-- Table: stock_listings
-- Stores individual stock listings on different markets
CREATE TABLE stock_listings (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(50) UNIQUE NOT NULL,
    company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    market_id INTEGER NOT NULL REFERENCES stock_markets(id) ON DELETE RESTRICT,
    CONSTRAINT unique_company_market UNIQUE (company_id, market_id)
);

-- Table: portfolio
-- Tracks which companies are currently owned in the portfolio
CREATE TABLE portfolio (
    id SERIAL PRIMARY KEY,
    company_id INTEGER UNIQUE NOT NULL REFERENCES companies(id) ON DELETE CASCADE
);

-- Indexes for performance
CREATE INDEX idx_stock_listings_symbol ON stock_listings(symbol);
CREATE INDEX idx_stock_listings_company_id ON stock_listings(company_id);
CREATE INDEX idx_stock_listings_market_id ON stock_listings(market_id);
CREATE INDEX idx_companies_company_name ON companies(company_name);
CREATE INDEX idx_companies_is_disqualified ON companies(is_disqualified);
CREATE INDEX idx_portfolio_company_id ON portfolio(company_id);

-- Comments for documentation
COMMENT ON TABLE stock_markets IS 'Different stock exchanges/markets';
COMMENT ON TABLE companies IS 'Companies - normalized so one company can have multiple market listings';
COMMENT ON TABLE stock_listings IS 'Individual stock listings on different markets';
COMMENT ON TABLE portfolio IS 'Companies currently owned in the portfolio';

COMMENT ON COLUMN stock_markets.abbreviation IS 'Market abbreviation (e.g., BO, NS, HK, KL, T)';
COMMENT ON COLUMN stock_markets.not_tradeable_until IS 'Timestamp until which the entire market is not tradeable';
COMMENT ON COLUMN companies.company_name IS 'Company name - the unique identifier for a company';
COMMENT ON COLUMN companies.had_quarter_loss IS 'Whether this company had a quarterly loss';
COMMENT ON COLUMN companies.is_disqualified IS 'Whether this company is disqualified from consideration';
COMMENT ON COLUMN companies.disqualified_reason IS 'Reason why the company is disqualified';
COMMENT ON COLUMN companies.dont_consider_until IS 'Timestamp until which this company should not be considered for trading';
COMMENT ON COLUMN companies.dont_consider_reason IS 'Reason for the temporary trading restriction';
COMMENT ON COLUMN stock_listings.symbol IS 'Full stock symbol including market suffix (e.g., TAPARIA.BO)';
COMMENT ON COLUMN stock_listings.company_id IS 'Reference to the company this listing belongs to';
COMMENT ON COLUMN stock_listings.market_id IS 'Reference to the market where this stock is listed';

