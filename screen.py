import pandas as pd
import os
import datetime
from lib import DIR, SYMBOL, PE_PB, get_merged_pd, get_portfolio_filename_with_symbols

merged_df = get_merged_pd(os.path.join(DIR, 'PE.csv'), os.path.join(DIR, 'PB.csv'))

# Filter out stocks that are already in the portfolio
disqualified_file = os.path.join(DIR, 'disqualified.csv')
def filter_out_stocks_from_file(df, file_path):
    if os.path.exists(file_path):
        exclude_df = pd.read_csv(file_path)
        df = df[~df[SYMBOL].isin(exclude_df[SYMBOL])]
    return df

filtered_df = filter_out_stocks_from_file(merged_df, disqualified_file)
filtered_df = filter_out_stocks_from_file(filtered_df, get_portfolio_filename_with_symbols())

current_year = datetime.datetime.now().year
for year in range(current_year - 10, current_year):
    dividend_file = os.path.join(DIR, f'{year}_first_dividend.csv')
    if os.path.exists(dividend_file):
        filtered_df = filter_out_stocks_from_file(filtered_df, dividend_file)
        

sorted_df = filtered_df.sort_values(by=PE_PB, ascending=True)
print(sorted_df.head(25))
print(len(sorted_df))
      