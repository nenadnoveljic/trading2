import pandas as pd
import os
import sys
import psycopg2
import datetime

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


def _deferral_still_active(dont_until: datetime.datetime | None) -> bool:
    """True if dont_consider_until is strictly in the future (aligned with PostgreSQL NOW())."""
    if dont_until is None:
        return False
    return dont_until > datetime.datetime.now()


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


def get_last_exclusion_reasons(symbols: list[str], company_names: list[str]) -> dict[str, str]:
    """Get the last exclusion reason for displayed stocks.

    Checks two sources:
    - Company-level: expired deferrals (dont_consider_until in the past)
    - Market-level: expired market restrictions (not_tradeable_until in the past)

    Returns dict of company_name -> reason code.
    """
    if not company_names:
        return {}
    conn = get_connection()
    cursor = conn.cursor()
    result = {}

    name_ph = ','.join(['%s'] * len(company_names))
    cursor.execute(f"""
        SELECT c.company_name, er.code
        FROM companies c
        JOIN exclusion_reasons er ON c.defer_reason_id = er.id
        WHERE c.company_name IN ({name_ph})
          AND c.is_disqualified = FALSE
          AND c.dont_consider_until IS NOT NULL
          AND c.dont_consider_until <= NOW()
    """, company_names)
    for row in cursor.fetchall():
        result[row[0]] = row[1]

    cursor.execute("""
        SELECT abbreviation
        FROM stock_markets
        WHERE not_tradeable_until IS NOT NULL
          AND not_tradeable_until <= NOW()
    """)
    expired_markets = {row[0] for row in cursor.fetchall()}

    if expired_markets:
        sym_to_name = dict(zip(symbols, company_names))
        for symbol, name in sym_to_name.items():
            if name in result:
                continue
            suffix = symbol.rsplit('.', 1)[-1] if '.' in symbol else None
            if suffix and suffix in expired_markets:
                result[name] = 'market: ' + suffix

    cursor.close()
    conn.close()
    return result


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

    Returns: year_loss (int or None), first_div_year, last_no_div_year (int or None)
    Does NOT return: AL_ratio (not cached, always fetch fresh)
    """
    if not company_names:
        return {}

    conn = get_connection()
    cursor = conn.cursor()

    placeholders = ','.join(['%s'] * len(company_names))
    cursor.execute(f"""
        SELECT company_name, last_year_loss, first_div_year, last_no_div_year, first_div_year_verified,
               dividend_data_source, dividend_cache_expires_at
        FROM companies
        WHERE company_name IN ({placeholders})
    """, company_names)

    result = {}
    for row in cursor.fetchall():
        result[row[0]] = {
            'year_loss': row[1],   # int or None
            'first_div_year': row[2],
            'last_no_div_year': row[3],  # int or None
            'first_div_year_verified': row[4],  # bool
            'dividend_data_source': row[5],
            'dividend_cache_expires_at': row[6],
        }

    cursor.close()
    conn.close()
    return result



def update_stock_info_cache(company_name: str, stock_info: dict):
    """
    Update cached stock info for a company (creates if not exists).

    Caches any non-None int for year_loss and last_no_div_year (uses GREATEST logic to never downgrade).
    Also caches: first_div_year
    Does NOT cache: AL_ratio (always fetch fresh)

    When stock_info contains dividend_data_source (irbank | yfinance), updates the dividend bundle
    authoritatively and sets dividend_cache_expires_at (90 days for irbank, NULL for yfinance).
    """
    conn = get_connection()
    cursor = conn.cursor()
    git_commit = get_git_commit()

    stock_info = dict(stock_info)
    div_source = stock_info.pop('dividend_data_source', None)
    div_expires = stock_info.pop('dividend_cache_expires_at', None)
    bundle_update = div_source is not None

    year_loss = stock_info.get('year_loss')  # int or None; cache if not None
    last_no_div_year = stock_info.get('last_no_div_year')  # int or None; cache if not None
    first_div_year = stock_info.get('first_div_year')

    # Skip if nothing to cache
    if year_loss is None and last_no_div_year is None and first_div_year is None and not bundle_update:
        cursor.close()
        conn.close()
        return

    cursor.execute("SELECT id FROM companies WHERE company_name = %s", (company_name,))
    row = cursor.fetchone()

    if row:
        if bundle_update:
            cursor.execute("""
                UPDATE companies
                SET last_year_loss = CASE
                        WHEN %s IS NOT NULL AND %s > COALESCE(last_year_loss, -1) THEN %s
                        ELSE last_year_loss
                    END,
                    last_no_div_year = COALESCE(%s, last_no_div_year),
                    first_div_year = COALESCE(%s, first_div_year),
                    dividend_data_source = %s,
                    dividend_cache_expires_at = %s,
                    updated_at = NOW(),
                    updated_by = %s
                WHERE id = %s
            """, (year_loss, year_loss, year_loss,
                  last_no_div_year, first_div_year,
                  div_source, div_expires, git_commit, row[0]))
        else:
            cursor.execute("""
                UPDATE companies
                SET last_year_loss = CASE
                        WHEN %s IS NOT NULL AND %s > COALESCE(last_year_loss, -1) THEN %s
                        ELSE last_year_loss
                    END,
                    last_no_div_year = CASE
                        WHEN %s IS NOT NULL AND %s > COALESCE(last_no_div_year, -1) THEN %s
                        ELSE last_no_div_year
                    END,
                    first_div_year = COALESCE(%s, first_div_year),
                    updated_at = NOW(),
                    updated_by = %s
                WHERE id = %s
            """, (year_loss, year_loss, year_loss,
                  last_no_div_year, last_no_div_year, last_no_div_year,
                  first_div_year, git_commit, row[0]))
    else:
        if bundle_update:
            cursor.execute("""
                INSERT INTO companies (
                    company_name, last_year_loss, last_no_div_year, first_div_year,
                    dividend_data_source, dividend_cache_expires_at, updated_at, updated_by
                )
                VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s)
            """, (company_name, year_loss, last_no_div_year, first_div_year, div_source, div_expires, git_commit))
        else:
            cursor.execute("""
                INSERT INTO companies (company_name, last_year_loss, last_no_div_year, first_div_year, updated_at, updated_by)
                VALUES (%s, %s, %s, %s, NOW(), %s)
            """, (company_name, year_loss, last_no_div_year, first_div_year, git_commit))

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
        # Skip only while the same deferral is still active (expired deferrals must renew)
        if _deferral_still_active(dont_until) and existing_reason_id == reason_id:
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
        # Skip only while quote_not_found deferral is still active
        if _deferral_still_active(dont_until) and existing_reason_id == reason_id:
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


def defer_company_for_cash_debt(company_name: str) -> bool:
    """
    Defer a company for 3 months due to cash not covering debt.
    Creates the company if it doesn't exist.
    
    Returns True if company was deferred, False if already deferred.
    """
    reason_id = get_exclusion_reason_id('cash_debt')
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
        if _deferral_still_active(dont_until) and existing_reason_id == reason_id:
            cursor.close()
            conn.close()
            return False

        cursor.execute("""
            UPDATE companies 
            SET dont_consider_until = NOW() + INTERVAL '3 months',
                defer_reason_id = %s,
                updated_at = NOW(),
                updated_by = %s
            WHERE id = %s
        """, (reason_id, git_commit, company_id))
    else:
        cursor.execute("""
            INSERT INTO companies (company_name, dont_consider_until, defer_reason_id, updated_at, updated_by)
            VALUES (%s, NOW() + INTERVAL '3 months', %s, NOW(), %s)
        """, (company_name, reason_id, git_commit))
    
    conn.commit()
    cursor.close()
    conn.close()
    return True


CURRENT_RATIO_THRESHOLD = 1.5
AL_RATIO_THRESHOLD = 1.5
# When True, defer 6 months when AL_ratio < AL_RATIO_THRESHOLD
AL_RATIO_FILTER_ENABLED = True
YEAR_LOSS_LOOKBACK = 20
FIRST_DIV_HISTORY_YEARS = 10


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

filtered_df = filtered_df[
    (filtered_df[CURRENT_RATIO] >= CURRENT_RATIO_THRESHOLD) | (filtered_df[CURRENT_RATIO].isna())
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
MIN_DISPLAY_COUNT = 30
BATCH_SIZE = 10

all_stock_info = {}
all_excluded = set()
all_deferred_not_found = []
all_deferred_al_ratio = []
all_deferred_cash_debt = []
all_excluded_loss = []
all_excluded_div_gaps = []
all_excluded_short_div_history = []

# Track which symbols we've already processed
processed_symbols = set()
remaining_df = sorted_df.copy()
screener_loop_stop_reason = "unknown"

while True:
    # Get next batch of unprocessed symbols
    candidates = remaining_df[~remaining_df[SYMBOL].isin(processed_symbols)][SYMBOL].head(BATCH_SIZE).tolist()
    
    if not candidates:
        screener_loop_stop_reason = "no_more_candidates"
        break  # No more candidates
    
    top_names = remaining_df[remaining_df[SYMBOL].isin(candidates)][[SYMBOL, NAME]].set_index(SYMBOL)[NAME].to_dict()
    
    # Get cached True values for year_loss/div_gaps
    stock_info_cache = get_stock_info_cache(list(top_names.values()))
    
    # Fetch from yfinance
    stock_info, not_found_symbols = get_stock_info_batch(
        candidates, names=top_names, pg_dividend_cache=stock_info_cache
    )
    
    # Process not found symbols
    for symbol in not_found_symbols:
        company_name = top_names[symbol]
        if defer_company_for_not_found(company_name):
            all_deferred_not_found.append((symbol, company_name))
        all_excluded.add(symbol)
    
    # Override with cached values (take the more recent known loss year)
    for symbol in candidates:
        if symbol in not_found_symbols:
            continue
        company_name = top_names[symbol]
        if company_name in stock_info_cache:
            cached = stock_info_cache[company_name]
            # Do not overwrite dividend bundle or year_loss from a fresh IRBANK/yfinance fetch this run
            if 'dividend_data_source' not in stock_info[symbol]:
                cached_year = cached.get('year_loss')
                fresh_year = stock_info[symbol].get('year_loss')
                if cached_year is not None and (fresh_year is None or cached_year > fresh_year):
                    stock_info[symbol]['year_loss'] = cached_year
                cached_div = cached.get('last_no_div_year')
                fresh_div = stock_info[symbol].get('last_no_div_year')
                if cached_div is not None and (fresh_div is None or cached_div > fresh_div):
                    stock_info[symbol]['last_no_div_year'] = cached_div
                if cached.get('first_div_year') is not None:
                    stock_info[symbol]['first_div_year'] = cached['first_div_year']
    
    # Process each symbol
    for symbol in candidates:
        if symbol in not_found_symbols:
            continue
            
        info = stock_info.get(symbol, {})
        company_name = top_names[symbol]
        
        # Check AL_ratio
        if AL_RATIO_FILTER_ENABLED:
            al_ratio = info.get('AL_ratio')
            if al_ratio is not None and al_ratio < AL_RATIO_THRESHOLD:
                if defer_company_for_al_ratio(company_name, al_ratio):
                    all_deferred_al_ratio.append((symbol, company_name, al_ratio))
                all_excluded.add(symbol)
        
        # Check year_loss (exclude if loss within last YEAR_LOSS_LOOKBACK years)
        current_year = datetime.date.today().year
        year_loss = info.get('year_loss')
        if year_loss is not None and year_loss > 0 and year_loss >= (current_year - YEAR_LOSS_LOOKBACK):
            all_excluded_loss.append((symbol, company_name, year_loss))
            all_excluded.add(symbol)
        
        # Check cash_debt_ok
        if info.get('cash_debt_ok') is False:
            if defer_company_for_cash_debt(company_name):
                all_deferred_cash_debt.append((symbol, company_name))
            all_excluded.add(symbol)
        
        # Check last_no_div_year (exclude if gap within last 20 years)
        last_no_div = info.get('last_no_div_year')
        if last_no_div is not None and last_no_div > 0 and last_no_div >= (current_year - YEAR_LOSS_LOOKBACK):
            all_excluded_div_gaps.append((symbol, company_name, last_no_div))
            all_excluded.add(symbol)

        # Check first_div_year (only filter when manually verified)
        if company_name in stock_info_cache:
            cached_verified = stock_info_cache[company_name]
            first_div_verified = cached_verified.get('first_div_year_verified')
            first_div_cached = cached_verified.get('first_div_year')
            if first_div_verified and first_div_cached is not None and first_div_cached > (current_year - FIRST_DIV_HISTORY_YEARS):
                all_excluded_short_div_history.append((symbol, company_name, first_div_cached))
                all_excluded.add(symbol)

        # Cache info
        update_stock_info_cache(company_name, info)
        
        # Store info for display
        all_stock_info[symbol] = info
    
    processed_symbols.update(candidates)
    
    # Check if we have enough valid stocks
    valid_count = len(processed_symbols - all_excluded)
    if valid_count >= MIN_DISPLAY_COUNT:
        screener_loop_stop_reason = "reached_min_display"
        break

valid_pre_final = len(processed_symbols - all_excluded)

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

if all_deferred_cash_debt:
    print(f"\nDeferred {len(all_deferred_cash_debt)} companies for 3 months (cash doesn't cover debt):")
    for sym, name in all_deferred_cash_debt:
        print(f"  {sym}: {name}")
    print()

if all_excluded_loss:
    print(f"\nExcluded {len(all_excluded_loss)} companies due to year loss in last {YEAR_LOSS_LOOKBACK} years:")
    for sym, name, year in all_excluded_loss:
        print(f"  {sym}: {name} (last loss: {year})")
    print()

if all_excluded_div_gaps:
    print(f"\nExcluded {len(all_excluded_div_gaps)} companies due to dividend gaps (within {YEAR_LOSS_LOOKBACK} years):")
    for sym, name, year in all_excluded_div_gaps:
        print(f"  {sym}: {name} (last gap: {year})")
    print()

if all_excluded_short_div_history:
    print(f"\nExcluded {len(all_excluded_short_div_history)} companies due to short dividend history (< {FIRST_DIV_HISTORY_YEARS} years):")
    for sym, name, year in all_excluded_short_div_history:
        print(f"  {sym}: {name} (first div: {year})")
    print()

# Final check: re-read last_year_loss from DB for survivors and apply 20-year filter
# (catches cases where batch loop wrote a fresher loss year for a previously-clean symbol)
current_year = datetime.date.today().year
survivors = list(processed_symbols - all_excluded)
if survivors:
    survivor_names = {s: sorted_df.loc[sorted_df[SYMBOL] == s, NAME].iloc[0] for s in survivors}
    final_cache = get_stock_info_cache(list(survivor_names.values()))
    for symbol in survivors:
        company_name = survivor_names[symbol]
        cached = final_cache.get(company_name, {})
        db_year = cached.get('year_loss')
        if db_year is not None and db_year > 0 and db_year >= (current_year - YEAR_LOSS_LOOKBACK):
            all_excluded.add(symbol)
        db_div = cached.get('last_no_div_year')
        if db_div is not None and db_div > 0 and db_div >= (current_year - YEAR_LOSS_LOOKBACK):
            all_excluded.add(symbol)
        db_first_div = cached.get('first_div_year')
        if cached.get('first_div_year_verified') and db_first_div is not None and db_first_div > (current_year - FIRST_DIV_HISTORY_YEARS):
            all_excluded.add(symbol)

valid_post_final = len(processed_symbols - all_excluded)
print(
    f"\n[Screener] stop={screener_loop_stop_reason} "
    f"valid_pre_final={valid_pre_final} valid_after_db_recheck={valid_post_final} "
    f"target={MIN_DISPLAY_COUNT} symbols_tried={len(processed_symbols)}",
    flush=True,
)

# Add info columns for processed symbols
sorted_df = sorted_df.copy()
verified_first_div_names = {
    name for name, cached in final_cache.items()
    if cached.get('first_div_year_verified')
} if survivors else set()
display_names = sorted_df[NAME]
sorted_df['first_div_year'] = sorted_df.apply(
    lambda row: (
        str(all_stock_info.get(row[SYMBOL], {}).get('first_div_year')) + '*'
        if row[NAME] in verified_first_div_names and all_stock_info.get(row[SYMBOL], {}).get('first_div_year') is not None
        else all_stock_info.get(row[SYMBOL], {}).get('first_div_year')
    ),
    axis=1
)
sorted_df['div_gaps'] = sorted_df[SYMBOL].map(
    lambda s: all_stock_info.get(s, {}).get('last_no_div_year')
).astype('Int64')
sorted_df['AL_ratio'] = sorted_df[SYMBOL].map(
    lambda s: all_stock_info.get(s, {}).get('AL_ratio')
)
sorted_df['year_loss'] = sorted_df[SYMBOL].map(
    lambda s: all_stock_info.get(s, {}).get('year_loss')
).astype('Int64')
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
].copy()

last_reasons = get_last_exclusion_reasons(display_df[SYMBOL].tolist(), display_df[NAME].tolist())
display_df['last_exclusion'] = display_df[NAME].map(last_reasons)

display_df = display_df.rename(columns={
    'P / E': 'PE',
    'Price / Book Ratio': 'PB',
    'Current Ratio': 'CR',
    'AL_ratio': 'AL',
    'year_loss': 'y_loss',
    'first_div_year': '1st_div',
    'cash_debt_ok': 'cash_ok',
    'last_exclusion': 'last_excl',
})
display_df = display_df.drop(columns=['quartal_loss', 'EPS', PE_PB], errors='ignore')

print(display_df.head(30))
print(len(display_df))
