#!/usr/bin/env python3
"""
Database Import Script
Populates the stocks database from CSV files.
"""

import os
import re
import csv
import glob
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values

# Database connection settings
DB_NAME = "stocks"
DB_HOST = "localhost"

# Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DB_DIR = os.path.join(PROJECT_ROOT, "db")


def get_connection():
    """Get a database connection."""
    return psycopg2.connect(dbname=DB_NAME, host=DB_HOST)


def extract_market(symbol: str) -> str | None:
    """Extract market abbreviation from symbol suffix (e.g., TAPARIA.BO -> BO)."""
    if '.' in symbol:
        return symbol.rsplit('.', 1)[1]
    return None


def ensure_market_exists(cursor, market_abbrev: str) -> int:
    """Ensure market exists and return its ID."""
    cursor.execute(
        """
        INSERT INTO stock_markets (abbreviation, name)
        VALUES (%s, %s)
        ON CONFLICT (abbreviation) DO UPDATE SET abbreviation = EXCLUDED.abbreviation
        RETURNING id
        """,
        (market_abbrev, market_abbrev)  # Use abbreviation as name for now
    )
    return cursor.fetchone()[0]


def ensure_company_exists(cursor, company_name: str) -> int:
    """Ensure company exists and return its ID."""
    cursor.execute(
        """
        INSERT INTO companies (company_name)
        VALUES (%s)
        ON CONFLICT (company_name) DO UPDATE SET company_name = EXCLUDED.company_name
        RETURNING id
        """,
        (company_name,)
    )
    return cursor.fetchone()[0]


def ensure_stock_listing_exists(cursor, symbol: str, company_id: int, market_id: int):
    """Ensure stock listing exists."""
    cursor.execute(
        """
        INSERT INTO stock_listings (symbol, company_id, market_id)
        VALUES (%s, %s, %s)
        ON CONFLICT (symbol) DO NOTHING
        """,
        (symbol, company_id, market_id)
    )


def load_csv(filepath: str) -> list[dict]:
    """Load a CSV file and return list of dicts."""
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return [row for row in reader]


def load_first_dividend_files(cursor) -> set[str]:
    """
    Load all [YYYY]_first_dividend.csv files.
    Returns set of company names that were loaded (ignore_until_set).
    """
    print("\n=== Loading First Dividend Files ===")
    
    # Find all first_dividend files and sort by year (earliest first)
    pattern = os.path.join(DB_DIR, "*_first_dividend.csv")
    files = glob.glob(pattern)
    
    # Extract year from filename and sort by year ascending (earliest first)
    year_files = []
    for filepath in files:
        filename = os.path.basename(filepath)
        match = re.match(r'(\d{4})_first_dividend\.csv', filename)
        if match:
            year = int(match.group(1))
            year_files.append((year, filepath))
    
    year_files.sort(key=lambda x: x[0])  # Sort by year ascending
    
    ignore_until_set = set()  # Track company names already processed
    
    for year, filepath in year_files:
        print(f"  Processing {os.path.basename(filepath)} (year {year})...")
        
        # dont_consider_until is Jan 1 of NEXT year
        dont_consider_until = datetime(year + 1, 1, 1)
        
        rows = load_csv(filepath)
        count = 0
        
        for row in rows:
            symbol = row.get('Symbol', '').strip()
            name = row.get('Name', '').strip()
            
            if not symbol or not name:
                continue
            
            # Skip if already processed (we want earliest date)
            if name in ignore_until_set:
                continue
            
            market_abbrev = extract_market(symbol)
            
            # Insert company with dont_consider_until
            cursor.execute(
                """
                INSERT INTO companies (company_name, dont_consider_until, dont_consider_reason)
                VALUES (%s, %s, %s)
                ON CONFLICT (company_name) DO UPDATE 
                SET dont_consider_until = EXCLUDED.dont_consider_until,
                    dont_consider_reason = EXCLUDED.dont_consider_reason
                RETURNING id
                """,
                (name, dont_consider_until, 'UNKNOWN')
            )
            company_id = cursor.fetchone()[0]
            
            # Insert market and stock listing if market exists
            if market_abbrev:
                market_id = ensure_market_exists(cursor, market_abbrev)
                ensure_stock_listing_exists(cursor, symbol, company_id, market_id)
            
            ignore_until_set.add(name)
            count += 1
        
        print(f"    Loaded {count} companies")
    
    print(f"  Total companies in ignore_until_set: {len(ignore_until_set)}")
    return ignore_until_set


def load_disqualified(cursor, ignore_until_set: set[str]):
    """Load disqualified stocks, skipping those in ignore_until_set."""
    print("\n=== Loading Disqualified Stocks ===")
    
    filepath = os.path.join(DB_DIR, "disqualified.csv")
    if not os.path.exists(filepath):
        print("  No disqualified.csv found, skipping...")
        return
    
    rows = load_csv(filepath)
    count = 0
    skipped = 0
    
    for row in rows:
        symbol = row.get('Symbol', '').strip()
        name = row.get('Name', '').strip()
        
        if not symbol or not name:
            continue
        
        # Skip if in ignore_until_set (first dividend takes precedence)
        if name in ignore_until_set:
            skipped += 1
            continue
        
        market_abbrev = extract_market(symbol)
        
        # Insert company as disqualified
        cursor.execute(
            """
            INSERT INTO companies (company_name, is_disqualified, disqualified_reason)
            VALUES (%s, %s, %s)
            ON CONFLICT (company_name) DO UPDATE 
            SET is_disqualified = EXCLUDED.is_disqualified,
                disqualified_reason = EXCLUDED.disqualified_reason
            RETURNING id
            """,
            (name, True, 'unknown')
        )
        company_id = cursor.fetchone()[0]
        
        # Insert market and stock listing if market exists
        if market_abbrev:
            market_id = ensure_market_exists(cursor, market_abbrev)
            ensure_stock_listing_exists(cursor, symbol, company_id, market_id)
        
        count += 1
    
    print(f"  Loaded {count} disqualified companies (skipped {skipped} in ignore_until_set)")


def load_quarterly_loss(cursor):
    """Load stocks with quarterly loss."""
    print("\n=== Loading Quarterly Loss Stocks ===")
    
    filepath = os.path.join(DB_DIR, "with_quartal_loss.csv")
    if not os.path.exists(filepath):
        print("  No with_quartal_loss.csv found, skipping...")
        return
    
    rows = load_csv(filepath)
    count = 0
    
    for row in rows:
        symbol = row.get('Symbol', '').strip()
        name = row.get('Name', '').strip()
        
        if not symbol or not name:
            continue
        
        market_abbrev = extract_market(symbol)
        
        # Insert or update company with had_quarter_loss
        cursor.execute(
            """
            INSERT INTO companies (company_name, had_quarter_loss)
            VALUES (%s, %s)
            ON CONFLICT (company_name) DO UPDATE 
            SET had_quarter_loss = EXCLUDED.had_quarter_loss
            RETURNING id
            """,
            (name, True)
        )
        company_id = cursor.fetchone()[0]
        
        # Insert market and stock listing if market exists
        if market_abbrev:
            market_id = ensure_market_exists(cursor, market_abbrev)
            ensure_stock_listing_exists(cursor, symbol, company_id, market_id)
        
        count += 1
    
    print(f"  Loaded {count} companies with quarterly loss")


def main():
    print("=" * 50)
    print("Stock Database Import")
    print("=" * 50)
    
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # 1. Load first dividend files (earliest date wins)
        ignore_until_set = load_first_dividend_files(cursor)
        
        # 2. Load disqualified stocks (skip those in ignore_until_set)
        load_disqualified(cursor, ignore_until_set)
        
        # 3. Load quarterly loss stocks
        load_quarterly_loss(cursor)
        
        # Commit all changes
        conn.commit()
        
        # Print summary
        print("\n" + "=" * 50)
        print("Import Complete!")
        print("=" * 50)
        
        cursor.execute("SELECT COUNT(*) FROM companies")
        print(f"Total companies: {cursor.fetchone()[0]}")
        
        cursor.execute("SELECT COUNT(*) FROM stock_markets")
        print(f"Total markets: {cursor.fetchone()[0]}")
        
        cursor.execute("SELECT COUNT(*) FROM stock_listings")
        print(f"Total stock listings: {cursor.fetchone()[0]}")
        
        cursor.execute("SELECT COUNT(*) FROM companies WHERE is_disqualified = TRUE")
        print(f"Disqualified companies: {cursor.fetchone()[0]}")
        
        cursor.execute("SELECT COUNT(*) FROM companies WHERE had_quarter_loss = TRUE")
        print(f"Companies with quarterly loss: {cursor.fetchone()[0]}")
        
        cursor.execute("SELECT COUNT(*) FROM companies WHERE dont_consider_until IS NOT NULL")
        print(f"Companies with dont_consider_until: {cursor.fetchone()[0]}")
        
    except Exception as e:
        conn.rollback()
        print(f"\nError: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()

