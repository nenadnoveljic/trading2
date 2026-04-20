import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.data import get_merged_pd, PE, PB, PE_PB, PRICE
from lib.downloader import get_portfolio_filename
from report_types import ReportType
import pandas as pd

df = get_merged_pd(pe_file=get_portfolio_filename(ReportType.FUND.value), pb_file=get_portfolio_filename(ReportType.FIN.value))


print("=== Stocks with negative EPS (sorted by PB * |EPS| / price, descending) ===")
_neg = df[df.EPS < 0].copy()
if _neg.empty:
    print(_neg)
else:
    if PRICE in _neg.columns:
        _price = pd.to_numeric(_neg[PRICE], errors='coerce')
    else:
        _price = pd.to_numeric(_neg[PE], errors='coerce') * pd.to_numeric(_neg['EPS'], errors='coerce')
    _den = _price.abs()
    _den = _den.mask(_den == 0)
    _neg['_sort_neg_eps'] = (
        pd.to_numeric(_neg[PB], errors='coerce')
        * pd.to_numeric(_neg['EPS'], errors='coerce').abs()
        / _den
    )
    print(_neg.sort_values(by='_sort_neg_eps', ascending=False, na_position='last').drop(columns=['_sort_neg_eps']))

print("\n=== Stocks with high PE (>10) or PB (>1) ===")
print(df[(df[PE] > 10) | (df[PB] > 1)].sort_values(by=PE_PB, ascending=False))

# Read the financial strength portfolio file
fs_file = get_portfolio_filename(ReportType.FINANCIAL_STRENGTH.value)
fs_df = pd.read_csv(fs_file, encoding='latin-1')

merged_df = pd.merge(df, fs_df, on='Symbol', suffixes=('', '_fs'))


# Work with Piotroski F-Score (common financial strength indicator, scale 0-9)
if 'Piotroski F-Score' in merged_df.columns:
    # Filter stocks with F-Score < 5 (weaker companies)
    weak_companies = merged_df[merged_df['Piotroski F-Score'] < 5]
    print(f"\n=== Companies with Piotroski F-Score < 5 ({len(weak_companies)} companies) ===")
    if not weak_companies.empty:
        weak_sorted = weak_companies.sort_values(by='Piotroski F-Score', ascending=True)
        print(
            weak_sorted.sort_values(
                by=['Piotroski F-Score', 'PE*PB'],
                ascending=[True, False]
            )[['Symbol', 'Name', 'Piotroski F-Score', 'PE*PB']]
        )
