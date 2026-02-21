import pandas as pd
import os
import sys
import psycopg2

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.downloader import COPIED_DOWNLOADS_DIR
from lib.data import SYMBOL, PE_PB, NAME, get_merged_pd
from lib.dividends import get_stock_info_batch
from lib.git_utils import get_git_commit

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)

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


def get_stock_info_cache(company_names: list[str]) -> dict[str, dict]:
    """
    Get cached stock info for companies.
    
    Returns dict mapping company_name to dict with:
        - year_loss: bool or None (None = not checked)
        - first_div_year: int or None
        - has_gaps: bool or None
        - AL_ratio: float or None
    """
    if not company_names:
        return {}
    
    conn = get_connection()
    cursor = conn.cursor()
    
    placeholders = ','.join(['%s'] * len(company_names))
    cursor.execute(f"""
        SELECT company_name, had_year_loss, first_div_year, has_div_gaps, al_ratio
        FROM companies 
        WHERE company_name IN ({placeholders})
    """, company_names)
    
    result = {}
    for row in cursor.fetchall():
        result[row[0]] = {
            'year_loss': row[1],
            'first_div_year': row[2],
            'has_gaps': row[3],
            'AL_ratio': float(row[4]) if row[4] is not None else None
        }
    
    cursor.close()
    conn.close()
    return result


def disqualify_company_for_year_loss(company_name: str) -> bool:
    """
    Permanently disqualify a company due to year loss.
    Creates the company if it doesn't exist.
    
    Returns True if company was disqualified, False if already disqualified.
    """
    conn = get_connection()
    cursor = conn.cursor()
    git_commit = get_git_commit()
    
    cursor.execute("SELECT id, is_disqualified FROM companies WHERE company_name = %s", (company_name,))
    row = cursor.fetchone()
    
    if row:
        company_id, is_disqualified = row
        if is_disqualified:
            cursor.close()
            conn.close()
            return False
        
        cursor.execute("""
            UPDATE companies 
            SET is_disqualified = TRUE,
                disqualified_reason = 'year loss',
                had_year_loss = TRUE,
                updated_at = NOW(),
                updated_by = %s
            WHERE id = %s
        """, (git_commit, company_id))
    else:
        cursor.execute("""
            INSERT INTO companies (company_name, is_disqualified, disqualified_reason, had_year_loss, updated_at, updated_by)
            VALUES (%s, TRUE, 'year loss', TRUE, NOW(), %s)
        """, (company_name, git_commit))
    
    conn.commit()
    cursor.close()
    conn.close()
    return True


def update_stock_info_cache(company_name: str, stock_info: dict):
    """
    Update cached stock info for a company (creates if not exists).
    
    stock_info dict can contain: year_loss, first_div_year, has_gaps, AL_ratio
    """
    conn = get_connection()
    cursor = conn.cursor()
    git_commit = get_git_commit()
    
    year_loss = stock_info.get('year_loss')
    first_div_year = stock_info.get('first_div_year')
    has_gaps = stock_info.get('has_gaps')
    al_ratio = stock_info.get('AL_ratio')
    
    cursor.execute("SELECT id FROM companies WHERE company_name = %s", (company_name,))
    row = cursor.fetchone()
    
    if row:
        cursor.execute("""
            UPDATE companies 
            SET had_year_loss = %s, 
                first_div_year = %s, 
                has_div_gaps = %s, 
                al_ratio = %s,
                updated_at = NOW(), 
                updated_by = %s
            WHERE id = %s
        """, (year_loss, first_div_year, has_gaps, al_ratio, git_commit, row[0]))
    else:
        cursor.execute("""
            INSERT INTO companies (company_name, had_year_loss, first_div_year, has_div_gaps, al_ratio, updated_at, updated_by)
            VALUES (%s, %s, %s, %s, %s, NOW(), %s)
        """, (company_name, year_loss, first_div_year, has_gaps, al_ratio, git_commit))
    
    conn.commit()
    cursor.close()
    conn.close()


def defer_company_for_al_ratio(company_name: str, al_ratio: float) -> bool:
    """
    Defer a company for 6 months due to low assets/liabilities ratio.
    Creates the company if it doesn't exist.
    
    Returns True if company was deferred, False if already deferred.
    """
    conn = get_connection()
    cursor = conn.cursor()
    git_commit = get_git_commit()
    
    # Check if company exists and is already deferred for AL ratio
    cursor.execute("""
        SELECT id, dont_consider_until, dont_consider_reason 
        FROM companies 
        WHERE company_name = %s
    """, (company_name,))
    
    row = cursor.fetchone()
    
    if row:
        company_id, dont_until, reason = row
        # Already deferred for AL ratio - skip
        if dont_until and reason and 'AL_ratio' in reason:
            cursor.close()
            conn.close()
            return False
        
        # Update existing company
        cursor.execute("""
            UPDATE companies 
            SET dont_consider_until = NOW() + INTERVAL '6 months',
                dont_consider_reason = %s,
                updated_at = NOW(),
                updated_by = %s
            WHERE id = %s
        """, (f'AL_ratio < 2 ({al_ratio})', git_commit, company_id))
    else:
        # Insert new company
        cursor.execute("""
            INSERT INTO companies (company_name, dont_consider_until, dont_consider_reason, updated_at, updated_by)
            VALUES (%s, NOW() + INTERVAL '6 months', %s, NOW(), %s)
        """, (company_name, f'AL_ratio < 2 ({al_ratio})', git_commit))
    
    conn.commit()
    cursor.close()
    conn.close()
    return True


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

# Fetch stock info (dividends + ratios) for top 5 results
top_n = 5
top_symbols = sorted_df[SYMBOL].head(top_n).tolist()
top_names = sorted_df[sorted_df[SYMBOL].isin(top_symbols)][[SYMBOL, NAME]].set_index(SYMBOL)[NAME].to_dict()

# Check cache to avoid unnecessary yfinance calls
stock_info_cache = get_stock_info_cache(list(top_names.values()))

# Determine which symbols need yfinance fetching (year_loss not yet cached means not checked)
symbols_to_fetch = [s for s in top_symbols if top_names[s] not in stock_info_cache or stock_info_cache[top_names[s]].get('year_loss') is None]
symbols_cached = [s for s in top_symbols if top_names[s] in stock_info_cache and stock_info_cache[top_names[s]].get('year_loss') is not None]

if symbols_cached:
    print(f"Using cached stock info for {len(symbols_cached)} companies")

# Fetch from yfinance only for uncached symbols
stock_info = {}
if symbols_to_fetch:
    stock_info = get_stock_info_batch(symbols_to_fetch)

# Merge cached stock info for cached symbols
for symbol in symbols_cached:
    stock_info[symbol] = stock_info_cache[top_names[symbol]]

# Add info columns (only populated for top N)
sorted_df = sorted_df.copy()
sorted_df['first_div_year'] = sorted_df[SYMBOL].map(
    lambda s: stock_info.get(s, {}).get('first_div_year')
)
sorted_df['div_gaps'] = sorted_df[SYMBOL].map(
    lambda s: stock_info.get(s, {}).get('has_gaps')
)
sorted_df['AL_ratio'] = sorted_df[SYMBOL].map(
    lambda s: stock_info.get(s, {}).get('AL_ratio')
)
sorted_df['year_loss'] = sorted_df[SYMBOL].map(
    lambda s: stock_info.get(s, {}).get('year_loss')
)

# Check for stocks with AL_ratio < 2 and defer them
AL_RATIO_THRESHOLD = 2.0
deferred_symbols = []
for symbol in top_symbols:
    info = stock_info.get(symbol, {})
    al_ratio = info.get('AL_ratio')
    if al_ratio is not None and al_ratio < AL_RATIO_THRESHOLD:
        # Get company name from the dataframe
        company_name = sorted_df[sorted_df[SYMBOL] == symbol][NAME].iloc[0]
        if defer_company_for_al_ratio(company_name, al_ratio):
            deferred_symbols.append((symbol, company_name, al_ratio))

if deferred_symbols:
    print(f"\nDeferred {len(deferred_symbols)} companies for 6 months due to AL_ratio < {AL_RATIO_THRESHOLD}:")
    for sym, name, ratio in deferred_symbols:
        print(f"  {sym}: {name} (AL_ratio: {ratio})")
    print()

# Process fetched symbols: disqualify if year_loss, update cache for all
disqualified_for_loss = []
for symbol in symbols_to_fetch:
    info = stock_info.get(symbol, {})
    year_loss = info.get('year_loss')
    company_name = top_names[symbol]
    
    if year_loss is True:
        if disqualify_company_for_year_loss(company_name):
            disqualified_for_loss.append((symbol, company_name))
    else:
        # Cache all stock info (year_loss, first_div_year, has_gaps, AL_ratio)
        update_stock_info_cache(company_name, info)

if disqualified_for_loss:
    print(f"\nPermanently disqualified {len(disqualified_for_loss)} companies due to year loss:")
    for sym, name in disqualified_for_loss:
        print(f"  {sym}: {name}")
    print()

print(sorted_df.head(50))
print(len(sorted_df))
