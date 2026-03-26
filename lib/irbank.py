"""
IRBANK (https://irbank.net) — Tokyo Stock Exchange (.T) fiscal dividend history.

Used ahead of yfinance for first_div_year / last_no_div_year when the results
page parses successfully. See https://irbank.net/<code>/results (例: 7417).
"""

from __future__ import annotations

import datetime as _dt
import re
import urllib.error
import urllib.request

IRBANK_RESULTS_URL = "https://irbank.net/{code}/results"
USER_AGENT = "Mozilla/5.0 (compatible; Stocks-screener/1.0; +local)"


def tokyo_numeric_code(symbol: str) -> str | None:
    """Return TSE number from YFinance-style symbol (e.g. 7417.T -> 7417)."""
    if not symbol.endswith(".T"):
        return None
    code = symbol[:-2].strip()
    return code if code.isdigit() else None


def fetch_results_html(code: str, timeout: float = 15.0) -> str | None:
    """GET 決算まとめ HTML, or None on failure."""
    url = IRBANK_RESULTS_URL.format(code=code)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status != 200:
                return None
            return resp.read().decode("utf-8", errors="replace")
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError):
        return None


def _strip_tags(fragment: str) -> str:
    return re.sub(r"<[^>]+>", "", fragment).strip()


def _parse_dividend_tbody_after_c_dividend(html: str) -> list[tuple[int, float | None]]:
    """
    Parse fiscal years and per-share dividend (yen) from IRBANK 配当 table.
    Returns list of (fiscal_start_year, dps_or_none) e.g. (2010, 5.0), (2009, None).
    """
    m = re.search(r'id="c_dividend"[^>]*>.*?</h2>\s*<div>\s*<table[^>]*>.*?<tbody>(.*?)</tbody>', html, re.DOTALL | re.IGNORECASE)
    if not m:
        return []
    tbody = m.group(1)
    rows: list[tuple[int, float | None]] = []
    for tr in re.finditer(r"<tr[^>]*>(.*?)</tr>", tbody, re.DOTALL | re.IGNORECASE):
        row = tr.group(1)
        tds = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL | re.IGNORECASE)
        if len(tds) < 2:
            continue
        fy_cell = _strip_tags(tds[0])
        if "予" in fy_cell:
            continue
        fy_m = re.search(r"(\d{4}/\d{2})", fy_cell)
        if not fy_m:
            continue
        year = int(fy_m.group(1).split("/")[0])
        dps_cell = tds[1]
        dps_plain = _strip_tags(dps_cell)
        if dps_plain in ("-", "−", "", "—"):
            rows.append((year, None))
            continue
        try:
            dps = float(dps_plain.replace(",", ""))
        except ValueError:
            rows.append((year, None))
            continue
        rows.append((year, dps))
    return rows


def dividend_first_and_gap_years(rows: list[tuple[int, float | None]]) -> tuple[int | None, int | None]:
    """
    first_div_year: first fiscal year (March year label) with DPS > 0.
    last_no_div_year: max calendar/fiscal year in last-20 window missing annual dividend,
                      0 if none, None if undetermined.
    """
    if not rows:
        return None, None
    with_div = [y for y, d in rows if d is not None and d > 0]
    if not with_div:
        return None, None
    first_div_year = min(with_div)
    dividend_years = set(with_div)
    last_year = max(dividend_years)
    today = _dt.date.today().year
    cutoff_year = today - 20
    check_start = max(first_div_year, cutoff_year)
    if last_year <= check_start:
        return first_div_year, 0
    expected = set(range(check_start, last_year + 1))
    missing = expected - dividend_years
    if missing:
        last_no = max(missing)
    else:
        last_no = 0
    return first_div_year, last_no


def try_irbank_dividend_fields(symbol: str) -> dict | None:
    """
    Fetch IRBANK 決算まとめ and return dividend fields, or None if unusable.

    Returns:
        {"first_div_year": int|None, "last_no_div_year": int|None} when parsed
        with at least one DPS > 0 row; else None (caller should use yfinance).
    """
    code = tokyo_numeric_code(symbol)
    if not code:
        return None
    html = fetch_results_html(code)
    if not html:
        return None
    rows = _parse_dividend_tbody_after_c_dividend(html)
    first_y, gap_y = dividend_first_and_gap_years(rows)
    if first_y is None:
        return None
    return {"first_div_year": first_y, "last_no_div_year": gap_y}
