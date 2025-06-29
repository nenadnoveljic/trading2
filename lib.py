import os
import pandas as pd

NAME = 'Name'
PE = 'P / E'
PB = 'Price / Book Ratio'
PE_PB = 'PE*PB'
SYMBOL = 'Symbol' 

DIR = os.path.dirname(os.path.abspath(__file__))
PORTFOLIO_FILE = os.path.join(DIR, 'portfolio.csv')

def get_merged_pd(pe_file: str, pb_file: str) -> pd.DataFrame:
    pe_df = pd.read_csv(pe_file)[[SYMBOL, NAME, PE, "EPS"]]
    pb_df = pd.read_csv(pb_file)[[SYMBOL, PB]]

    merged_df = pd.merge(pe_df, pb_df, on=SYMBOL)
    merged_df[PE_PB] = merged_df[PE] * merged_df[PB]
    return merged_df
