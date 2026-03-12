# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Activate virtual environment
source myenv/bin/activate

# Initialize/reset PostgreSQL database
bash db/setup.sh
python bin/import_db.py

# Download fresh screener data (requires Chrome with remote debugging)
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --remote-debugging-port=9222 --user-data-dir=/tmp/chrome_debug &
python bin/download.py

# Run the stock screener (main workflow)
python bin/screen.py

# Analyze portfolio
python bin/portfolio.py
```

No automated test suite exists — validation is done by running the scripts manually.

## Architecture

The application is a dividend stock screener. Data flows through three stages:

**1. Download (`bin/download.py`)** — Selenium scraper that connects to a running Chrome instance (port 9222) and downloads PE/PB screener CSVs from marketinout.com into `downloads/`.

**2. Screen (`bin/screen.py`)** — Core engine:
- Merges PE/PB CSVs via `lib/data.py`, filters by `Current Ratio ≥ 1.5`
- Queries PostgreSQL to skip already-excluded companies
- Iteratively fetches financial data from Yahoo Finance (`lib/dividends.py`) in batches of 10 until at least 5 valid stocks are found
- Applies disqualification/deferral rules (see below) and caches results to DB
- Displays top results sorted by `PE*PB`

**3. Portfolio (`bin/portfolio.py`)** — Reads downloaded `portfolio_*.csv` files to flag high-valuation or negative-EPS portfolio holdings by Piotroski F-Score.

### Database (PostgreSQL, database: `stocks`)

Key tables:
- `companies` — canonical company record with cached financial flags (`had_year_loss`, `first_div_year`, `has_div_gaps`) and exclusion state (`is_disqualified`, `dont_consider_until`)
- `stock_listings` — symbol ↔ company mapping per market
- `exclusion_reasons` — lookup for permanent vs. temporary deferral codes
- `stock_markets` — markets with optional `not_tradeable_until` (skips entire suffix)

Migrations are in `db/migrations/` and must be applied manually in order.

### Filtering Rules (in `lib/dividends.py` + `bin/screen.py`)

| Condition | Action |
|---|---|
| AL ratio < 1.5 | Defer 6 months |
| Year loss detected | Permanently disqualify |
| Cash + Receivables < Total Debt | Defer 3 months |
| Dividend gaps | Permanently disqualify |
| Symbol 404 from yfinance | Defer 1 month |

### Symbol Translation

MarketInOut uses different suffixes than yfinance. The map in `lib/dividends.py` (`SYMBOL_SUFFIX_MAP`) handles translations (e.g., `.KO` → `.KS` for Korea, `.BV` → `.SA` for Brazil).

### Key Thresholds (in `bin/screen.py`)

- `CURRENT_RATIO_THRESHOLD = 1.5`
- `AL_RATIO_THRESHOLD = 1.5`
- `BATCH_SIZE = 10`
- `MIN_DISPLAY_COUNT = 5`
