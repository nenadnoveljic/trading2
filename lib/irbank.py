"""
IRBANK (https://irbank.net) — Tokyo Stock Exchange (.T) fiscal dividend history.

Used ahead of yfinance for first_div_year / last_no_div_year and (when the
対象 table parses) year_loss from the same 決算まとめ page.
See https://irbank.net/<code>/results (例: 7417).
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


def _pl_thead_first_row(html_fragment: str) -> str | None:
    m = re.search(r"<thead[^>]*>\s*<tr[^>]*>(.*?)</tr>", html_fragment, re.DOTALL | re.IGNORECASE)
    return m.group(1) if m else None


def _pl_net_income_column_index(html: str) -> int | None:
    m = re.search(
        r'id="c_pl"[^>]*>.*?</h2>\s*<div>\s*<table[^>]*>(.*?)</table>',
        html,
        re.DOTALL | re.IGNORECASE,
    )
    if not m:
        return None
    thead_row = _pl_thead_first_row(m.group(1))
    if not thead_row:
        return None
    ths = re.findall(r"<th[^>]*>(.*?)</th>", thead_row, re.DOTALL | re.IGNORECASE)
    for i, th_html in enumerate(ths):
        if _th_is_net_income_pl_column(th_html):
            return i
    return None


def _th_is_net_income_pl_column(th_html: str) -> bool:
    """True if this <th> is net income for the 会社業績 table (IRBANK label varies by issuer)."""
    title_m = re.search(r'title="([^"]*)"', th_html, re.IGNORECASE)
    title = (title_m.group(1) if title_m else "").strip()
    text = _strip_tags(th_html).strip()
    if "当期利益" in text or "当期利益" in title:
        return True
    if "当期純利益" in text or "当期純利益" in title:
        return True
    if text == "純利" or title == "純利":
        return True
    return False


def _pl_cell_is_loss(plain: str) -> bool:
    """True when IRBANK 会社業績 net income cell indicates a loss for that fiscal year."""
    plain = plain.strip()
    if not plain or plain in ("-", "−", "", "—"):
        return False
    if "赤字" in plain:
        return True
    s = plain.replace(",", "").replace(" ", "")
    if re.search(r"△\s*\d", s):
        return True
    m_oku = re.search(r"(-?\d+\.?\d*)\s*億", s)
    if m_oku:
        return float(m_oku.group(1)) < 0
    s2 = (
        s.replace("−", "-")
        .replace("△", "-")
    )
    try:
        v = float(re.sub(r"[^\d.\-]", "", s2))
        return v < 0
    except ValueError:
        return False


def _parse_year_loss_from_pl(html: str) -> int | None:
    """
    Latest fiscal start-year with a loss in 会社業績 net income column (当期利益 / 純利 / 当期純利益).

    Returns:
        int: max loss year (fiscal label YYYY/MM -> YYYY)
        0: table and column found, no loss rows
        None: section/column missing or no parseable body rows
    """
    col_idx = _pl_net_income_column_index(html)
    if col_idx is None:
        return None
    m = re.search(
        r'id="c_pl"[^>]*>.*?</h2>\s*<div>\s*<table[^>]*>.*?<tbody>(.*?)</tbody>',
        html,
        re.DOTALL | re.IGNORECASE,
    )
    if not m:
        return None
    tbody = m.group(1)
    loss_years: list[int] = []
    saw_fy_row = False
    for tr in re.finditer(r"<tr[^>]*>(.*?)</tr>", tbody, re.DOTALL | re.IGNORECASE):
        row = tr.group(1)
        tds = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL | re.IGNORECASE)
        if not tds:
            continue
        fy_cell = _strip_tags(tds[0])
        if "予" in fy_cell:
            continue
        fy_m = re.search(r"(\d{4}/\d{2})", fy_cell)
        if not fy_m:
            continue
        saw_fy_row = True
        year = int(fy_m.group(1).split("/")[0])
        if col_idx >= len(tds):
            continue
        ni_plain = _strip_tags(tds[col_idx])
        if _pl_cell_is_loss(ni_plain):
            loss_years.append(year)
    if not saw_fy_row:
        return None
    if not loss_years:
        return 0
    return max(loss_years)


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


def try_irbank_fiscal_fields(symbol: str) -> dict | None:
    """
    One GET to IRBANK 決算まとめ: dividend block + 会社業績 (当期利益).

    Returns:
        None if symbol is not .T or HTTP fetch failed.
        Otherwise {"first_div_year", "last_no_div_year", "year_loss"} — dividend
        keys may be None when no DPS row; year_loss is int (>=0) or None if PL
        column/table could not be parsed.
    """
    code = tokyo_numeric_code(symbol)
    if not code:
        return None
    html = fetch_results_html(code)
    if not html:
        return None
    yloss = _parse_year_loss_from_pl(html)
    rows = _parse_dividend_tbody_after_c_dividend(html)
    first_y, gap_y = dividend_first_and_gap_years(rows)
    return {
        "first_div_year": first_y,
        "last_no_div_year": gap_y,
        "year_loss": yloss,
    }
