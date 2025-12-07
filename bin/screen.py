import pandas as pd
import os
import sys
import psycopg2

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.downloader import COPIED_DOWNLOADS_DIR
from lib.data import SYMBOL, PE_PB, get_merged_pd

# Database connection settings
DB_NAME = "stocks"
DB_HOST = "localhost"


def get_connection():
    """Get a database connection."""
    return psycopg2.connect(dbname=DB_NAME, host=DB_HOST)


def get_excluded_symbols() -> set[str]:
    """
    Get symbols that should be excluded from screening:
    - Disqualified companies
    - Companies with dont_consider_until > NOW()
    - Companies in portfolio
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT sl.symbol 
        FROM stock_listings sl
        JOIN companies c ON sl.company_id = c.id
        WHERE c.is_disqualified = TRUE 
           OR c.dont_consider_until > NOW()
           OR c.id IN (SELECT company_id FROM portfolio)
    """)
    
    symbols = {row[0] for row in cursor.fetchall()}
    
    cursor.close()
    conn.close()
    
    return symbols


def get_quarterly_loss_symbols() -> set[str]:
    """Get symbols of companies with quarterly loss."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT sl.symbol 
        FROM stock_listings sl
        JOIN companies c ON sl.company_id = c.id
        WHERE c.had_quarter_loss = TRUE
    """)
    
    symbols = {row[0] for row in cursor.fetchall()}
    
    cursor.close()
    conn.close()
    
    return symbols


# Load and merge PE/PB data
merged_df = get_merged_pd(
    os.path.join(COPIED_DOWNLOADS_DIR, 'PE.csv'), 
    os.path.join(COPIED_DOWNLOADS_DIR, 'PB.csv')
)

# Get excluded symbols from database
excluded_symbols = get_excluded_symbols()

# Filter out excluded stocks
filtered_df = merged_df[~merged_df[SYMBOL].isin(excluded_symbols)]

# Get quarterly loss symbols from database
quarterly_loss_symbols = get_quarterly_loss_symbols()

# Add quartal_loss column
filtered_df = filtered_df.copy()
filtered_df['quartal_loss'] = filtered_df[SYMBOL].isin(quarterly_loss_symbols)

# Sort by quartal_loss (False first), then by PE*PB ascending
sorted_df = filtered_df.sort_values(by=['quartal_loss', PE_PB], ascending=[True, True])

print(sorted_df.head(50))
print(len(sorted_df))
