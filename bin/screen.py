import pandas as pd
import os
import sys
import psycopg2

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.downloader import COPIED_DOWNLOADS_DIR
from lib.data import SYMBOL, PE_PB, NAME, CURRENT_RATIO, get_merged_pd
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


def get_exclusion_reason_id(code: str) -> int | None:
    """Get exclusion reason ID by code from the exclusion_reasons table."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM exclusion_reasons WHERE code = %s", (code,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row[0] if row else None


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


def get_quarterly_loss_status() -> dict[str, bool | None]:
    """
    Get quarterly loss status by company name.
    
    Returns dict mapping company_name to:
        - True: has quarterly loss
        - False: no quarterly loss (checked)
        - None: not checked (company not in database)
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT company_name, had_quarter_loss 
        FROM companies
    """)
    
    result = {row[0]: row[1] for row in cursor.fetchall()}
    
    cursor.close()
    conn.close()
    
    return result


def get_stock_info_cache(company_names: list[str]) -> dict[str, dict]:
    """
    Get cached stock info for companies.
    
    Only returns cached True values for: year_loss, has_gaps
    Also returns: first_div_year
    Does NOT return: AL_ratio (not cached, always fetch fresh)
    """
    if not company_names:
        return {}
    
    conn = get_connection()
    cursor = conn.cursor()
    
    placeholders = ','.join(['%s'] * len(company_names))
    cursor.execute(f"""
        SELECT company_name, had_year_loss, first_div_year, has_div_gaps
        FROM companies 
        WHERE company_name IN ({placeholders})
    """, company_names)
    
    result = {}
    for row in cursor.fetchall():
        result[row[0]] = {
            'year_loss': row[1],
            'first_div_year': row[2],
            'has_gaps': row[3]
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
    reason_id = get_exclusion_reason_id('year_loss')
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
                disqualified_reason_id = %s,
                had_year_loss = TRUE,
                updated_at = NOW(),
                updated_by = %s
            WHERE id = %s
        """, (reason_id, git_commit, company_id))
    else:
        cursor.execute("""
            INSERT INTO companies (company_name, is_disqualified, disqualified_reason_id, had_year_loss, updated_at, updated_by)
            VALUES (%s, TRUE, %s, TRUE, NOW(), %s)
        """, (company_name, reason_id, git_commit))
    
    conn.commit()
    cursor.close()
    conn.close()
    return True


def disqualify_company_for_div_gaps(company_name: str) -> bool:
    """
    Permanently disqualify a company due to dividend gaps.
    Creates the company if it doesn't exist.
    
    Returns True if company was disqualified, False if already disqualified.
    """
    reason_id = get_exclusion_reason_id('dividends_gap')
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
                disqualified_reason_id = %s,
                has_div_gaps = TRUE,
                updated_at = NOW(),
                updated_by = %s
            WHERE id = %s
        """, (reason_id, git_commit, company_id))
    else:
        cursor.execute("""
            INSERT INTO companies (company_name, is_disqualified, disqualified_reason_id, has_div_gaps, updated_at, updated_by)
            VALUES (%s, TRUE, %s, TRUE, NOW(), %s)
        """, (company_name, reason_id, git_commit))
    
    conn.commit()
    cursor.close()
    conn.close()
    return True


def update_stock_info_cache(company_name: str, stock_info: dict):
    """
    Update cached stock info for a company (creates if not exists).
    
    Only caches True values for: year_loss, has_gaps
    Also caches: first_div_year (for dividend history)
    Does NOT cache: AL_ratio (always fetch fresh)
    """
    conn = get_connection()
    cursor = conn.cursor()
    git_commit = get_git_commit()
    
    # Only cache True values for these flags
    year_loss = stock_info.get('year_loss') if stock_info.get('year_loss') is True else None
    has_gaps = stock_info.get('has_gaps') if stock_info.get('has_gaps') is True else None
    first_div_year = stock_info.get('first_div_year')
    
    # Skip if nothing to cache
    if year_loss is None and has_gaps is None and first_div_year is None:
        cursor.close()
        conn.close()
        return
    
    cursor.execute("SELECT id FROM companies WHERE company_name = %s", (company_name,))
    row = cursor.fetchone()
    
    if row:
        cursor.execute("""
            UPDATE companies 
            SET had_year_loss = COALESCE(%s, had_year_loss), 
                first_div_year = COALESCE(%s, first_div_year), 
                has_div_gaps = COALESCE(%s, has_div_gaps),
                updated_at = NOW(), 
                updated_by = %s
            WHERE id = %s
        """, (year_loss, first_div_year, has_gaps, git_commit, row[0]))
    else:
        cursor.execute("""
            INSERT INTO companies (company_name, had_year_loss, first_div_year, has_div_gaps, updated_at, updated_by)
            VALUES (%s, %s, %s, %s, NOW(), %s)
        """, (company_name, year_loss, first_div_year, has_gaps, git_commit))
    
    conn.commit()
    cursor.close()
    conn.close()


def defer_company_for_al_ratio(company_name: str, al_ratio: float) -> bool:
    """
    Defer a company for 6 months due to low assets/liabilities ratio.
    Creates the company if it doesn't exist.
    
    Returns True if company was deferred, False if already deferred.
    """
    reason_id = get_exclusion_reason_id('al_ratio')
    conn = get_connection()
    cursor = conn.cursor()
    git_commit = get_git_commit()
    
    # Check if company exists and is already deferred for AL ratio
    cursor.execute("""
        SELECT id, dont_consider_until, defer_reason_id 
        FROM companies 
        WHERE company_name = %s
    """, (company_name,))
    
    row = cursor.fetchone()
    
    if row:
        company_id, dont_until, existing_reason_id = row
        # Already deferred for AL ratio - skip
        if dont_until and existing_reason_id == reason_id:
            cursor.close()
            conn.close()
            return False
        
        # Update existing company
        cursor.execute("""
            UPDATE companies 
            SET dont_consider_until = NOW() + INTERVAL '6 months',
                defer_reason_id = %s,
                updated_at = NOW(),
                updated_by = %s
            WHERE id = %s
        """, (reason_id, git_commit, company_id))
    else:
        # Insert new company
        cursor.execute("""
            INSERT INTO companies (company_name, dont_consider_until, defer_reason_id, updated_at, updated_by)
            VALUES (%s, NOW() + INTERVAL '6 months', %s, NOW(), %s)
        """, (company_name, reason_id, git_commit))
    
    conn.commit()
    cursor.close()
    conn.close()
    return True


def defer_company_for_not_found(company_name: str) -> bool:
    """
    Defer a company for 1 month due to symbol not found (404 error).
    Creates the company if it doesn't exist.
    
    Returns True if company was deferred, False if already deferred.
    """
    reason_id = get_exclusion_reason_id('quote_not_found')
    conn = get_connection()
    cursor = conn.cursor()
    git_commit = get_git_commit()
    
    cursor.execute("""
        SELECT id, dont_consider_until, defer_reason_id 
        FROM companies 
        WHERE company_name = %s
    """, (company_name,))
    
    row = cursor.fetchone()
    
    if row:
        company_id, dont_until, existing_reason_id = row
        # Already deferred for not_tradeable - skip
        if dont_until and existing_reason_id == reason_id:
            cursor.close()
            conn.close()
            return False
        
        cursor.execute("""
            UPDATE companies 
            SET dont_consider_until = NOW() + INTERVAL '1 month',
                defer_reason_id = %s,
                updated_at = NOW(),
                updated_by = %s
            WHERE id = %s
        """, (reason_id, git_commit, company_id))
    else:
        cursor.execute("""
            INSERT INTO companies (company_name, dont_consider_until, defer_reason_id, updated_at, updated_by)
            VALUES (%s, NOW() + INTERVAL '1 month', %s, NOW(), %s)
        """, (company_name, reason_id, git_commit))
    
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

# Filter: keep only Current Ratio >= 2 or NaN (unknown)
filtered_df = filtered_df[
    (filtered_df[CURRENT_RATIO] >= 2) | (filtered_df[CURRENT_RATIO].isna())
]

# Get quarterly loss status from database (True/False/None) by company name
quarterly_loss_status = get_quarterly_loss_status()

# Add quartal_loss column (None for unknown, False for checked no loss, True for loss)
filtered_df = filtered_df.copy()
filtered_df['quartal_loss'] = filtered_df[NAME].map(quarterly_loss_status)

# Sort: True (quarterly loss) at end, False/NaN sorted by PE*PB
# Create sort key: True=1 (end), False/NaN=0 (sort by PE*PB)
filtered_df['_sort_loss'] = filtered_df['quartal_loss'].apply(lambda x: 1 if x is True else 0)
sorted_df = filtered_df.sort_values(by=['_sort_loss', PE_PB], ascending=[True, True])
sorted_df = sorted_df.drop(columns=['_sort_loss'])

# Fetch stock info iteratively until we have MIN_DISPLAY_COUNT valid stocks
MIN_DISPLAY_COUNT = 5
BATCH_SIZE = 10
AL_RATIO_THRESHOLD = 2.0

all_stock_info = {}
all_excluded = set()
all_deferred_not_found = []
all_deferred_al_ratio = []
all_disqualified_loss = []
all_disqualified_div_gaps = []

# Track which symbols we've already processed
processed_symbols = set()
remaining_df = sorted_df.copy()

while True:
    # Get next batch of unprocessed symbols
    candidates = remaining_df[~remaining_df[SYMBOL].isin(processed_symbols)][SYMBOL].head(BATCH_SIZE).tolist()
    
    if not candidates:
        break  # No more candidates
    
    top_names = remaining_df[remaining_df[SYMBOL].isin(candidates)][[SYMBOL, NAME]].set_index(SYMBOL)[NAME].to_dict()
    
    # Get cached True values for year_loss/div_gaps
    stock_info_cache = get_stock_info_cache(list(top_names.values()))
    
    # Fetch from yfinance
    stock_info, not_found_symbols = get_stock_info_batch(candidates, names=top_names)
    
    # Process not found symbols
    for symbol in not_found_symbols:
        company_name = top_names[symbol]
        if defer_company_for_not_found(company_name):
            all_deferred_not_found.append((symbol, company_name))
        all_excluded.add(symbol)
    
    # Override with cached True values
    for symbol in candidates:
        if symbol in not_found_symbols:
            continue
        company_name = top_names[symbol]
        if company_name in stock_info_cache:
            cached = stock_info_cache[company_name]
            if cached.get('year_loss') is True:
                stock_info[symbol]['year_loss'] = True
            if cached.get('has_gaps') is True:
                stock_info[symbol]['has_gaps'] = True
            if cached.get('first_div_year') is not None:
                stock_info[symbol]['first_div_year'] = cached['first_div_year']
    
    # Process each symbol
    for symbol in candidates:
        if symbol in not_found_symbols:
            continue
            
        info = stock_info.get(symbol, {})
        company_name = top_names[symbol]
        
        # Check AL_ratio
        al_ratio = info.get('AL_ratio')
        if al_ratio is not None and al_ratio < AL_RATIO_THRESHOLD:
            if defer_company_for_al_ratio(company_name, al_ratio):
                all_deferred_al_ratio.append((symbol, company_name, al_ratio))
            all_excluded.add(symbol)
        
        # Check year_loss
        if info.get('year_loss') is True:
            if disqualify_company_for_year_loss(company_name):
                all_disqualified_loss.append((symbol, company_name))
            all_excluded.add(symbol)
        
        # Check div_gaps
        if info.get('has_gaps') is True:
            if disqualify_company_for_div_gaps(company_name):
                all_disqualified_div_gaps.append((symbol, company_name))
            all_excluded.add(symbol)
        
        # Cache info
        update_stock_info_cache(company_name, info)
        
        # Store info for display
        all_stock_info[symbol] = info
    
    processed_symbols.update(candidates)
    
    # Check if we have enough valid stocks
    valid_count = len(processed_symbols - all_excluded)
    if valid_count >= MIN_DISPLAY_COUNT:
        break

# Print summaries
if all_deferred_not_found:
    print(f"\nDeferred {len(all_deferred_not_found)} companies for 1 month (symbol not found):")
    for sym, name in all_deferred_not_found:
        print(f"  {sym}: {name}")

if all_deferred_al_ratio:
    print(f"\nDeferred {len(all_deferred_al_ratio)} companies for 6 months due to AL_ratio < {AL_RATIO_THRESHOLD}:")
    for sym, name, ratio in all_deferred_al_ratio:
        print(f"  {sym}: {name} (AL_ratio: {ratio})")
    print()

if all_disqualified_loss:
    print(f"\nPermanently disqualified {len(all_disqualified_loss)} companies due to year loss:")
    for sym, name in all_disqualified_loss:
        print(f"  {sym}: {name}")
    print()

if all_disqualified_div_gaps:
    print(f"\nPermanently disqualified {len(all_disqualified_div_gaps)} companies due to dividend gaps:")
    for sym, name in all_disqualified_div_gaps:
        print(f"  {sym}: {name}")
    print()

# Add info columns for processed symbols
sorted_df = sorted_df.copy()
sorted_df['first_div_year'] = sorted_df[SYMBOL].map(
    lambda s: all_stock_info.get(s, {}).get('first_div_year')
)
sorted_df['div_gaps'] = sorted_df[SYMBOL].map(
    lambda s: all_stock_info.get(s, {}).get('has_gaps')
)
sorted_df['AL_ratio'] = sorted_df[SYMBOL].map(
    lambda s: all_stock_info.get(s, {}).get('AL_ratio')
)
sorted_df['year_loss'] = sorted_df[SYMBOL].map(
    lambda s: all_stock_info.get(s, {}).get('year_loss')
)
sorted_df['cash_debt_ok'] = sorted_df[SYMBOL].map(
    lambda s: all_stock_info.get(s, {}).get('cash_debt_ok')
)

# Fallback: fill NaN Current Ratio from yfinance
for symbol in processed_symbols:
    if symbol in all_excluded:
        continue
    mask = sorted_df[SYMBOL] == symbol
    if mask.any() and pd.isna(sorted_df.loc[mask, CURRENT_RATIO].values[0]):
        yf_current_ratio = all_stock_info.get(symbol, {}).get('current_ratio')
        if yf_current_ratio is not None:
            sorted_df.loc[mask, CURRENT_RATIO] = yf_current_ratio

# Filter out excluded stocks from display
display_df = sorted_df[
    sorted_df[SYMBOL].isin(processed_symbols) & 
    ~sorted_df[SYMBOL].isin(all_excluded)
]

print(display_df.head(50))
print(len(display_df))
