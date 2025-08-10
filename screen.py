import pandas as pd
import os
import datetime
from lib.downloader import DIR,COPIED_DOWNLOADS_DIR, get_portfolio_filename_with_symbols
from lib.data import SYMBOL, PE_PB, get_merged_pd

DB_DIR = "db"

merged_df = get_merged_pd(os.path.join(COPIED_DOWNLOADS_DIR, 'PE.csv'), os.path.join(COPIED_DOWNLOADS_DIR, 'PB.csv'))

# Filter out stocks that are already in the portfolio
disqualified_file = os.path.join(DB_DIR, 'disqualified.csv')
def filter_out_stocks_from_file(df, file_path):
    if os.path.exists(file_path):
        exclude_df = pd.read_csv(file_path)
        df = df[~df[SYMBOL].isin(exclude_df[SYMBOL])]
    return df

filtered_df = filter_out_stocks_from_file(merged_df, disqualified_file)
filtered_df = filter_out_stocks_from_file(filtered_df, get_portfolio_filename_with_symbols())

current_year = datetime.datetime.now().year
for year in range(current_year - 10, current_year):
    dividend_file = os.path.join(DB_DIR, f'{year}_first_dividend.csv')
    if os.path.exists(dividend_file):
        filtered_df = filter_out_stocks_from_file(filtered_df, dividend_file)
        
with_quartal_loss = pd.read_csv(os.path.join(DB_DIR, 'with_quartal_loss.csv'))
# Merge with_quartal_loss with filtered_df on SYMBOL
merged_with_loss = pd.merge(
    filtered_df,
    with_quartal_loss[[SYMBOL]],
    on=SYMBOL,
    how='left',
    indicator=True
)

# Set 'quartal_loss' column: True if present in with_quartal_loss, else False
merged_with_loss['quartal_loss'] = merged_with_loss['_merge'] == 'both'
# Drop the merge indicator column
merged_with_loss = merged_with_loss.drop(columns=['_merge'])

# Use merged_with_loss as the new filtered_df for further processing
filtered_df = merged_with_loss


sorted_df = filtered_df.sort_values(by=['quartal_loss', PE_PB], ascending=[True, True])
print(sorted_df.head(50))
print(len(sorted_df))
      