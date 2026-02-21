import time
import yfinance as yf


def get_dividend_info(symbol: str) -> dict:
    """
    Fetch dividend information for a symbol.
    
    Returns dict with:
        - first_div_year: Year of first dividend (int or None)
        - has_gaps: Whether there are gaps in annual dividends (bool or None)
    """
    try:
        ticker = yf.Ticker(symbol)
        divs = ticker.dividends
        
        if len(divs) == 0:
            return {"first_div_year": None, "has_gaps": None}
        
        first_year = divs.index.min().year
        last_year = divs.index.max().year
        
        # Check for gaps: get unique years with dividends
        dividend_years = set(divs.index.year)
        
        # Check if any year is missing between first and last
        has_gaps = False
        if last_year > first_year:
            expected_years = set(range(first_year, last_year + 1))
            missing_years = expected_years - dividend_years
            has_gaps = len(missing_years) > 0
        
        return {
            "first_div_year": first_year,
            "has_gaps": has_gaps
        }
    except Exception:
        return {"first_div_year": None, "has_gaps": None}


def get_dividend_info_batch(symbols: list[str], delay: float = 0.2) -> dict[str, dict]:
    """
    Fetch dividend info for multiple symbols.
    
    Args:
        symbols: List of stock symbols
        delay: Delay between API calls in seconds
    
    Returns:
        Dictionary mapping symbol to dividend info dict
    """
    result = {}
    for i, symbol in enumerate(symbols):
        result[symbol] = get_dividend_info(symbol)
        if i < len(symbols) - 1:
            time.sleep(delay)
    return result
