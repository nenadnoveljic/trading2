import os
import pandas as pd
from report_types import ReportType

# Column name constants
NAME = 'Name'
PE = 'P / E'
PB = 'Price / Book Ratio'
PE_PB = 'PE*PB'
SYMBOL = 'Symbol' 

def get_merged_pd(pe_file: str, pb_file: str) -> pd.DataFrame:
    """Merge PE and PB data files into a single DataFrame"""
    pe_df = pd.read_csv(pe_file)
    pb_df = pd.read_csv(pb_file)
    
    pe_df = pe_df[[SYMBOL, NAME, PE, "EPS"]]
    pb_df = pb_df[[SYMBOL, PB]]

    merged_df = pd.merge(pe_df, pb_df, on=SYMBOL)
    merged_df[PE_PB] = merged_df[PE] * merged_df[PB]
    return merged_df