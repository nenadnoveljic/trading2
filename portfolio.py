from lib import DIR, get_merged_pd, PE, PB, PE_PB, get_portfolio_filename
from report_types import ReportType

df = get_merged_pd(pe_file=get_portfolio_filename(ReportType.FUND.value), pb_file=get_portfolio_filename(ReportType.FIN.value))

print(df[df.EPS < 0])

print(df[(df[PE] > 10) | (df[PB] > 1)].sort_values(by=PE_PB, ascending=False))
