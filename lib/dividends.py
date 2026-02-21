import time
import yfinance as yf


def get_assets_liabilities_ratio(symbol: str) -> float | None:
    """
    Fetch assets/liabilities ratio from yfinance balance sheet.
    
    Returns Total Assets / Total Liabilities, or None if unavailable.
    """
    try:
        ticker = yf.Ticker(symbol)
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


def get_stock_info(symbol: str) -> dict:
    """
    Fetch financial information for a symbol.
    
    Returns dict with:
        - first_div_year: Year of first dividend (int or None)
        - has_gaps: Whether there are gaps in annual dividends (bool or None)
        - assets_liab_ratio: Total Assets / Total Liabilities (float or None)
    """
    result = {
        "first_div_year": None,
        "has_gaps": None,
        "assets_liab_ratio": None
    }
    
    try:
        ticker = yf.Ticker(symbol)
        
        # Get dividend info
        divs = ticker.dividends
        if len(divs) > 0:
            first_year = divs.index.min().year
            last_year = divs.index.max().year
            result["first_div_year"] = first_year
            
            dividend_years = set(divs.index.year)
            if last_year > first_year:
                expected_years = set(range(first_year, last_year + 1))
                missing_years = expected_years - dividend_years
                result["has_gaps"] = len(missing_years) > 0
            else:
                result["has_gaps"] = False
        
        # Get assets/liabilities ratio
        result["assets_liab_ratio"] = get_assets_liabilities_ratio(symbol)
        
    except Exception:
        pass
    
    return result


def get_dividend_info(symbol: str) -> dict:
    """
    Fetch dividend information for a symbol.
    
    Returns dict with:
        - first_div_year: Year of first dividend (int or None)
        - has_gaps: Whether there are gaps in annual dividends (bool or None)
    """
    info = get_stock_info(symbol)
    return {
        "first_div_year": info["first_div_year"],
        "has_gaps": info["has_gaps"]
    }


def get_stock_info_batch(symbols: list[str], delay: float = 0.2) -> dict[str, dict]:
    """
    Fetch full stock info for multiple symbols.
    
    Args:
        symbols: List of stock symbols
        delay: Delay between API calls in seconds
    
    Returns:
        Dictionary mapping symbol to stock info dict
    """
    result = {}
    for i, symbol in enumerate(symbols):
        result[symbol] = get_stock_info(symbol)
        if i < len(symbols) - 1:
            time.sleep(delay)
    return result


def get_dividend_info_batch(symbols: list[str], delay: float = 0.2) -> dict[str, dict]:
    """
    Fetch dividend info for multiple symbols.
    
    Args:
        symbols: List of stock symbols
        delay: Delay between API calls in seconds
    
    Returns:
        Dictionary mapping symbol to dividend info dict
    """
    full_info = get_stock_info_batch(symbols, delay)
    return {
        sym: {"first_div_year": info["first_div_year"], "has_gaps": info["has_gaps"]}
        for sym, info in full_info.items()
    }
