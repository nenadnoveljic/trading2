import os
import pandas as pd
from report_types import ReportType

NAME = 'Name'
PE = 'P / E'
PB = 'Price / Book Ratio'
PE_PB = 'PE*PB'
SYMBOL = 'Symbol' 

COPIED_DOWNLOADS_DIR = os.path.join("..", os.path.dirname(os.path.abspath(__file__)), "downloads")
DOWNLOAD_DIR = "/Users/nenad.noveljic/Downloads"
DIR = os.path.join(".", os.path.dirname(os.path.abspath(__file__)))

def get_merged_pd(pe_file: str, pb_file: str) -> pd.DataFrame:
    pe_df = pd.read_csv(pe_file)
    pb_df = pd.read_csv(pb_file)
    
    pe_df = pe_df[[SYMBOL, NAME, PE, "EPS"]]
    pb_df = pb_df[[SYMBOL, PB]]

    merged_df = pd.merge(pe_df, pb_df, on=SYMBOL)
    merged_df[PE_PB] = merged_df[PE] * merged_df[PB]
    return merged_df

def get_portfolio_filename(view: ReportType) -> str:
    return os.path.join(COPIED_DOWNLOADS_DIR, f"portfolio_{view}.csv")

def get_portfolio_filename_with_symbols() -> str:
    return get_portfolio_filename(ReportType.FIN.value)