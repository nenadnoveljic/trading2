import os
from report_types import ReportType

# Directory paths
DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Project root
COPIED_DOWNLOADS_DIR = os.path.join(DIR, "downloads")
DOWNLOAD_DIR = "/Users/nenad.noveljic/Downloads"

def get_portfolio_filename(view: ReportType) -> str:
    """Get filename for portfolio data based on view type"""
    return os.path.join(COPIED_DOWNLOADS_DIR, f"portfolio_{view}.csv")

def get_pe_filename() -> str:
    """Get PE screener filename"""
    return os.path.join(COPIED_DOWNLOADS_DIR, "PE.csv")

def get_pb_filename() -> str:
    """Get PB screener filename"""
    return os.path.join(COPIED_DOWNLOADS_DIR, "PB.csv")

def get_portfolio_filename_with_symbols() -> str:
    """Get portfolio filename with symbols (uses FIN view)"""
    return get_portfolio_filename(ReportType.FIN.value) 