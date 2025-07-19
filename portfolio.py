from lib.data import get_merged_pd, PE, PB, PE_PB
from lib.downloader import get_portfolio_filename
from report_types import ReportType
import pandas as pd

df = get_merged_pd(pe_file=get_portfolio_filename(ReportType.FUND.value), pb_file=get_portfolio_filename(ReportType.FIN.value))


print("=== Stocks with negative EPS ===")
print(df[df.EPS < 0])

print("\n=== Stocks with high PE (>10) or PB (>1) ===")
print(df[(df[PE] > 10) | (df[PB] > 1)].sort_values(by=PE_PB, ascending=False))

# Read the financial strength portfolio file
fs_file = get_portfolio_filename(ReportType.FINANCIAL_STRENGTH.value)
fs_df = pd.read_csv(fs_file)

merged_df = pd.merge(df, fs_df, on='Symbol', suffixes=('', '_fs'))


# Work with Piotroski F-Score (common financial strength indicator, scale 0-9)
if 'Piotroski F-Score' in merged_df.columns:
    print(f"\n=== Piotroski F-Score Analysis ===")
    
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
