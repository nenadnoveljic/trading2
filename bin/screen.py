import pandas as pd
import os
import sys
import psycopg2

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.downloader import COPIED_DOWNLOADS_DIR
from lib.data import SYMBOL, PE_PB, NAME, get_merged_pd
from lib.dividends import get_dividend_info_batch

# Database connection settings
DB_NAME = "stocks"
DB_HOST = "localhost"


def get_connection():
    """Get a database connection."""
    return psycopg2.connect(dbname=DB_NAME, host=DB_HOST)


def get_portfolio_symbols_from_csv() -> set[str]:
    """Get portfolio symbols from portfolio_fin.csv."""
    filepath = os.path.join(COPIED_DOWNLOADS_DIR, 'portfolio_fin.csv')
    if os.path.exists(filepath):
        df = pd.read_csv(filepath, encoding='latin-1')
        return set(df['Symbol'].dropna())
    return set()


def get_excluded_symbols() -> set[str]:
    """
    Get symbols that should be excluded from screening:
    - Disqualified companies
    - Companies with dont_consider_until > NOW()
    - Stocks from markets with not_tradeable_until > NOW()
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT sl.symbol 
        FROM stock_listings sl
        JOIN companies c ON sl.company_id = c.id
        JOIN stock_markets sm ON sl.market_id = sm.id
        WHERE c.is_disqualified = TRUE 
           OR c.dont_consider_until > NOW()
           OR sm.not_tradeable_until > NOW()
    """)
    
    symbols = {row[0] for row in cursor.fetchall()}
    
    cursor.close()
    conn.close()
    
    # Add portfolio symbols from CSV
    symbols.update(get_portfolio_symbols_from_csv())
    
    return symbols


def get_excluded_company_names() -> set[str]:
    """Get company names that should be excluded (disqualified or deferred)."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT company_name 
        FROM companies 
        WHERE is_disqualified = TRUE 
           OR dont_consider_until > NOW()
    """)
    
    names = {row[0] for row in cursor.fetchall()}
    cursor.close()
    conn.close()
    return names


def get_deferred_market_suffixes() -> set[str]:
    """Get market suffixes that are currently deferred."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT abbreviation 
        FROM stock_markets 
        WHERE not_tradeable_until > NOW()
    """)
    
    suffixes = {'.' + row[0] for row in cursor.fetchall()}
    cursor.close()
    conn.close()
    return suffixes


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

# Get exclusions
excluded_symbols = get_excluded_symbols()
excluded_names = get_excluded_company_names()

# Get deferred market suffixes
deferred_suffixes = get_deferred_market_suffixes()

# Filter out excluded stocks (by symbol OR by company name)
filtered_df = merged_df[
    ~merged_df[SYMBOL].isin(excluded_symbols) & 
    ~merged_df[NAME].isin(excluded_names)
]

# Also filter by deferred market suffix
if deferred_suffixes:
    filtered_df = filtered_df[
        ~filtered_df[SYMBOL].apply(lambda s: any(s.endswith(suffix) for suffix in deferred_suffixes))
    ]

# Get quarterly loss symbols from database
quarterly_loss_symbols = get_quarterly_loss_symbols()

# Add quartal_loss column
filtered_df = filtered_df.copy()
filtered_df['quartal_loss'] = filtered_df[SYMBOL].isin(quarterly_loss_symbols)

# Sort by quartal_loss (False first), then by PE*PB ascending
sorted_df = filtered_df.sort_values(by=['quartal_loss', PE_PB], ascending=[True, True])

# Fetch dividend info for top 5 results
top_n = 5
top_symbols = sorted_df[SYMBOL].head(top_n).tolist()
dividend_info = get_dividend_info_batch(top_symbols)

# Add dividend columns (only populated for top N)
sorted_df = sorted_df.copy()
sorted_df['first_div_year'] = sorted_df[SYMBOL].map(
    lambda s: dividend_info.get(s, {}).get('first_div_year')
)
sorted_df['div_gaps'] = sorted_df[SYMBOL].map(
    lambda s: dividend_info.get(s, {}).get('has_gaps')
)

print(sorted_df.head(50))
print(len(sorted_df))
