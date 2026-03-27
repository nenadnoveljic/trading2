import time
import datetime as _dt
import yfinance as yf

from lib.irbank import try_irbank_fiscal_fields

DIVIDEND_IRBANK_TTL = _dt.timedelta(days=90)


class SymbolNotFoundError(Exception):
    """Raised when a symbol is not found (404 error)."""
    pass


# Symbol suffix translations for yfinance compatibility
SYMBOL_SUFFIX_MAP = {
    '.KO': '.KS',   # Korean KOSPI: MarketInOut uses .KO, yfinance uses .KS
    '.BV': '.SA',   # Brazilian: MarketInOut uses .BV, yfinance uses .SA
}


def translate_symbol_for_yfinance(symbol: str) -> str:
    """Translate symbol suffix for yfinance compatibility."""
    for old_suffix, new_suffix in SYMBOL_SUFFIX_MAP.items():
        if symbol.endswith(old_suffix):
            return symbol[:-len(old_suffix)] + new_suffix
    return symbol


def get_assets_liabilities_ratio(symbol: str) -> float | None:
    """
    Fetch assets/liabilities ratio from yfinance balance sheet.
    
    Returns Total Assets / Total Liabilities, or None if unavailable.
    """
    try:
        yf_symbol = translate_symbol_for_yfinance(symbol)
        ticker = yf.Ticker(yf_symbol)
        balance_sheet = ticker.balance_sheet
        
        if balance_sheet.empty:
            return None
        
        # Get most recent column (latest quarter/year)
        latest = balance_sheet.iloc[:, 0]
        
        total_assets = latest.get('Total Assets')
        total_liabilities = latest.get('Total Liabilities Net Minority Interest')
        
        if total_liabilities is None:
            total_liabilities = latest.get('Total Liabilities')
        
        if total_assets is not None and total_liabilities is not None and total_liabilities != 0:
            return round(float(total_assets / total_liabilities), 2)
        
        return None
    except Exception:
        return None


def _detect_year_loss(ticker) -> int | None:
    """Detect the most recent year with a net loss.

    Returns:
        int: Most recent year with a net loss (e.g. 2018)
        0:   Checked successfully, no loss found
        None: Could not determine
    """
    import pandas as pd

    def _find_net_income_row(df):
        preferred = ['NetIncome', 'Net Income', 'NetIncomeCommonStockholders', 'NetIncomeContinuousOperations']
        for r in preferred:
            if r in df.index:
                return r
        for idx in df.index:
            s = str(idx).lower().replace(' ', '')
            if 'netincome' in s or 'net income' in s:
                return idx
        return None

    # Try yfinance: check both annual and quarterly (quarterly can reveal losses in years not in annual window)
    import datetime as _dt
    LOOKBACK_YEARS = 20
    cutoff_year = _dt.date.today().year - LOOKBACK_YEARS

    got_data = False
    max_loss_year = None
    oldest_year_seen = None  # track how far back the data goes

    for freq in ['yearly', 'quarterly']:
        df = None
        if hasattr(ticker, 'get_income_stmt'):
            try:
                df = ticker.get_income_stmt(pretty=True, freq=freq)
            except Exception:
                pass
        if df is None or df.empty:
            df = ticker.financials if freq == 'yearly' else getattr(ticker, 'quarterly_financials', pd.DataFrame())
        if df is None:
            df = pd.DataFrame()

        if not df.empty:
            row = _find_net_income_row(df)
            if row is not None:
                got_data = True
                series = df.loc[row]
                numeric = pd.to_numeric(series, errors='coerce')
                try:
                    col_years = pd.to_datetime(numeric.index).year
                    oldest = int(col_years.min())
                    if oldest_year_seen is None or oldest < oldest_year_seen:
                        oldest_year_seen = oldest
                except Exception:
                    pass
                if freq == 'quarterly':
                    try:
                        years = pd.to_datetime(numeric.index).year
                        yearly = numeric.groupby(years).sum()
                        loss_years = yearly[yearly < 0].index.tolist()
                        if loss_years:
                            year = int(max(loss_years))
                            if max_loss_year is None or year > max_loss_year:
                                max_loss_year = year
                    except Exception:
                        loss_mask = numeric < 0
                        if loss_mask.any():
                            try:
                                loss_years = pd.to_datetime(numeric[loss_mask].index).year.tolist()
                                year = int(max(loss_years))
                                if max_loss_year is None or year > max_loss_year:
                                    max_loss_year = year
                            except Exception:
                                pass
                else:
                    loss_mask = numeric < 0
                    if loss_mask.any():
                        try:
                            loss_years = pd.to_datetime(numeric[loss_mask].index).year.tolist()
                            year = int(max(loss_years))
                            if max_loss_year is None or year > max_loss_year:
                                max_loss_year = year
                        except Exception:
                            pass

    if got_data:
        if max_loss_year is not None:
            return max_loss_year
        # Data found but no loss — only mark clean if coverage reaches the cutoff year
        if oldest_year_seen is not None and oldest_year_seen <= cutoff_year:
            return 0
        return None  # data too recent to rule out older losses

    # Fallback: Yahoo quoteSummary API (works when timeseries returns empty, e.g. Canadian tickers)
    try:
        data = getattr(ticker, '_data', None)
        if data is not None and hasattr(data, 'cache_get'):
            url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker.ticker}?modules=incomeStatementHistory,incomeStatementHistoryQuarterly"
            resp = data.cache_get(url=url)
            if resp.status_code == 200:
                js = resp.json()
                result = js.get('quoteSummary', {}).get('result', [{}])
                if result:
                    found_any = False
                    fallback_max_loss_year = None
                    fallback_oldest_year = None
                    for module in ['incomeStatementHistory', 'incomeStatementHistoryQuarterly']:
                        mod_data = result[0].get(module, {})
                        for stmt in mod_data.get('incomeStatementHistory', []):
                            ni = stmt.get('netIncome')
                            if isinstance(ni, dict):
                                raw = ni.get('raw')
                                if raw is not None:
                                    found_any = True
                                    end_date = stmt.get('endDate', {})
                                    ts = end_date.get('raw') if isinstance(end_date, dict) else None
                                    if ts is not None:
                                        year = _dt.datetime.utcfromtimestamp(ts).year
                                        if fallback_oldest_year is None or year < fallback_oldest_year:
                                            fallback_oldest_year = year
                                        if raw < 0:
                                            if fallback_max_loss_year is None or year > fallback_max_loss_year:
                                                fallback_max_loss_year = year
                    if found_any:
                        if fallback_max_loss_year is not None:
                            return fallback_max_loss_year
                        if fallback_oldest_year is not None and fallback_oldest_year <= cutoff_year:
                            return 0
                        return None  # data too recent to rule out older losses
    except Exception:
        pass

    return None


def _use_pg_dividend_bundle(pg: dict | None) -> bool:
    """True when dividend fields should come from Postgres (skip IRBANK / yfinance dividend fetch)."""
    if not pg:
        return False
    if pg.get("first_div_year_verified"):
        return True
    if pg.get("dividend_data_source") == "irbank" and pg.get("dividend_cache_expires_at"):
        exp = pg["dividend_cache_expires_at"]
        if getattr(exp, "tzinfo", None) is None:
            exp = exp.replace(tzinfo=_dt.timezone.utc)
        return exp > _dt.datetime.now(_dt.timezone.utc)
    return False


def get_stock_info(symbol: str, dividend_pg_bundle: dict | None = None) -> dict:
    """
    Fetch financial information for a symbol.
    
    Returns dict with:
        - first_div_year: Year of first dividend (int or None)
        - last_no_div_year: int | None — most recent year with no dividend, 0=no gap, None=unknown
        - AL_ratio: Total Assets / Total Liabilities (float or None)
        - year_loss: int | None — year of most recent net loss, 0=no loss, None=unknown
        - current_ratio: Current Ratio from yfinance (float or None)
        - cash_debt_ok: Whether Cash + Receivables >= Total Debt (bool or None)
    """
    result = {
        "first_div_year": None,
        "last_no_div_year": None,
        "AL_ratio": None,
        "year_loss": None,
        "current_ratio": None,
        "cash_debt_ok": None
    }
    
    yf_symbol = translate_symbol_for_yfinance(symbol)
    
    try:
        ticker = yf.Ticker(yf_symbol)
        
        # Check if symbol is valid by trying to get info first
        info = ticker.info
        # If info is empty or only has minimal fields, symbol likely doesn't exist
        if not info or len(info) <= 1 or info.get('trailingPegRatio') is None and info.get('regularMarketPrice') is None and info.get('previousClose') is None:
            # Check balance sheet as backup
            balance_sheet = ticker.balance_sheet
            financials = ticker.financials
            if balance_sheet.empty and financials.empty:
                raise SymbolNotFoundError(f"Symbol {symbol} not found or delisted")
        
        # Dividend history: PG bundle if verified or unexpired irbank; else IRBANK then yfinance
        using_pg_dividend_bundle = bool(
            dividend_pg_bundle and _use_pg_dividend_bundle(dividend_pg_bundle)
        )
        if using_pg_dividend_bundle:
            result["first_div_year"] = dividend_pg_bundle.get("first_div_year")
            result["last_no_div_year"] = dividend_pg_bundle.get("last_no_div_year")
            result["year_loss"] = dividend_pg_bundle.get("year_loss")
            # Postgres bundle predates IRBANK 会社業績; refresh loss year from the same page as dividends.
            if symbol.endswith(".T"):
                ir_supp = try_irbank_fiscal_fields(symbol)
                if ir_supp is not None and ir_supp["year_loss"] is not None:
                    pg_y = result["year_loss"]
                    ir_y = ir_supp["year_loss"]
                    loss_years = [y for y in (pg_y, ir_y) if y is not None and y > 0]
                    if loss_years:
                        result["year_loss"] = max(loss_years)
                    elif ir_y == 0:
                        result["year_loss"] = 0
                    if ir_supp["first_div_year"] is not None or ir_supp["year_loss"] is not None:
                        result["dividend_data_source"] = "irbank"
                        result["dividend_cache_expires_at"] = (
                            _dt.datetime.now(_dt.timezone.utc) + DIVIDEND_IRBANK_TTL
                        )
        else:
            ir = try_irbank_fiscal_fields(symbol)
            divs = ticker.dividends
            if ir is not None and ir["first_div_year"] is not None:
                result["first_div_year"] = ir["first_div_year"]
                result["last_no_div_year"] = ir["last_no_div_year"]
            else:
                if len(divs) > 0:
                    first_year = divs.index.min().year
                    last_year = divs.index.max().year
                    result["first_div_year"] = first_year

                    dividend_years = set(divs.index.year)
                    cutoff_year = _dt.date.today().year - 20
                    check_start = max(first_year, cutoff_year)
                    if last_year > check_start:
                        expected_years = set(range(check_start, last_year + 1))
                        missing_years = expected_years - dividend_years
                        if missing_years:
                            result["last_no_div_year"] = max(missing_years)
                        else:
                            result["last_no_div_year"] = 0
                    else:
                        result["last_no_div_year"] = 0

            if ir is not None and ir["year_loss"] is not None:
                result["year_loss"] = ir["year_loss"]
            else:
                result["year_loss"] = _detect_year_loss(ticker)

            irbank_snapshot = ir is not None and (
                ir["first_div_year"] is not None or ir["year_loss"] is not None
            )
            if irbank_snapshot:
                result["dividend_data_source"] = "irbank"
                result["dividend_cache_expires_at"] = (
                    _dt.datetime.now(_dt.timezone.utc) + DIVIDEND_IRBANK_TTL
                )
            elif (ir is None or ir["first_div_year"] is None) and len(divs) > 0:
                result["dividend_data_source"] = "yfinance"
                result["dividend_cache_expires_at"] = None

        # Get assets/liabilities ratio
        result["AL_ratio"] = get_assets_liabilities_ratio(symbol)
        
        # Get current ratio from info
        if info and 'currentRatio' in info and info['currentRatio'] is not None:
            result["current_ratio"] = round(float(info['currentRatio']), 2)
        
        # Check if Cash + Receivables >= Total Debt
        balance_sheet = ticker.balance_sheet
        if not balance_sheet.empty:
            latest = balance_sheet.iloc[:, 0]
            # Use Cash & Short Term Investments (more comprehensive) as primary
            cash = latest.get('Cash Cash Equivalents And Short Term Investments') or latest.get('Cash And Cash Equivalents')
            # Sum all receivables types (treat None as 0)
            accounts_recv = latest.get('Accounts Receivable') or 0
            other_recv = latest.get('Other Receivables') or 0
            receivables = accounts_recv + other_recv
            total_debt = latest.get('Total Debt')
            
            if cash is not None and total_debt is not None:
                result["cash_debt_ok"] = bool((cash + receivables) >= total_debt)
        
    except SymbolNotFoundError:
        raise
    except Exception as e:
        error_str = str(e)
        if '404' in error_str or 'Not Found' in error_str or 'Quote not found' in error_str:
            raise SymbolNotFoundError(f"Symbol {symbol} not found: {error_str}")
    
    return result


def get_dividend_info(symbol: str) -> dict:
    """
    Fetch dividend information for a symbol.

    Returns dict with:
        - first_div_year: Year of first dividend (int or None)
        - last_no_div_year: int | None — most recent year with no dividend, 0=no gap, None=unknown
    """
    info = get_stock_info(symbol)
    return {
        "first_div_year": info["first_div_year"],
        "last_no_div_year": info["last_no_div_year"]
    }


def get_stock_info_batch(
    symbols: list[str],
    delay: float = 0.2,
    names: dict[str, str] | None = None,
    pg_dividend_cache: dict[str, dict] | None = None,
) -> tuple[dict[str, dict], list[str]]:
    """
    Fetch full stock info for multiple symbols.
    
    Args:
        symbols: List of stock symbols
        delay: Delay between API calls in seconds
        names: Optional dict mapping symbol to company name for better error messages
        pg_dividend_cache: Optional dict company_name -> row from get_stock_info_cache (dividend TTL / verified)
    
    Returns:
        Tuple of:
        - Dictionary mapping symbol to stock info dict
        - List of symbols that returned 404 (not found)
    """
    result = {}
    not_found = []
    for i, symbol in enumerate(symbols):
        try:
            div_bundle = None
            if names and pg_dividend_cache:
                cname = names.get(symbol)
                if cname:
                    div_bundle = pg_dividend_cache.get(cname)
            result[symbol] = get_stock_info(symbol, dividend_pg_bundle=div_bundle)
        except SymbolNotFoundError:
            name = names.get(symbol, '') if names else ''
            name_str = f" ({name})" if name else ""
            print(f"  -> {symbol}{name_str}: quote not found, deferring for 1 month")
            not_found.append(symbol)
            result[symbol] = {
                "first_div_year": None,
                "last_no_div_year": None,
                "AL_ratio": None,
                "year_loss": None,
                "current_ratio": None,
                "cash_debt_ok": None
            }
        if i < len(symbols) - 1:
            time.sleep(delay)
    return result, not_found


def get_dividend_info_batch(symbols: list[str], delay: float = 0.2) -> dict[str, dict]:
    """
    Fetch dividend info for multiple symbols.
    
    Args:
        symbols: List of stock symbols
        delay: Delay between API calls in seconds
    
    Returns:
        Dictionary mapping symbol to dividend info dict
    """
    full_info, _ = get_stock_info_batch(symbols, delay)
    return {
        sym: {"first_div_year": info["first_div_year"], "last_no_div_year": info["last_no_div_year"]}
        for sym, info in full_info.items()
    }
