import os
import re
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from shutil import move
from report_types import ReportType
from lib.downloader import COPIED_DOWNLOADS_DIR, get_portfolio_filename

DOWNLOAD_DIR = "/Users/nenad.noveljic/Downloads"
ROOT_URL = "https://www.marketinout.com"
TOOLS_URL = f"{ROOT_URL}/tools"

def get_screener_url(view: str) -> str:
    return f"{ROOT_URL}/stock-screener/stocks.php?f=1&screen_id=413136&view={view}"

def get_portfolio_url(view: ReportType) -> str:
    return f"{TOOLS_URL}/portfolio.php?view={view}&bd=&sorting=company&plt=a"

def get_portfolio_download_url(view: ReportType) -> str:
    return f"{TOOLS_URL}/csv.csv?pid=10810&view={view}"

def get_screener_download_url() -> str:
    return f"{ROOT_URL}/stock-screener/csv_stocks.csv?f=1&screen_id=413136"

def download_single_portfolio_view(driver: webdriver, view: ReportType) -> None:
    """Download portfolio data for a single report type view"""
    download(
        driver, 
        get_portfolio_url(view.value), 
        get_portfolio_download_url(view.value), 
        "MyPortfolio.csv", 
        get_portfolio_filename(view.value)
    )
    
def download_portfolio(driver: webdriver) -> None:
    download_single_portfolio_view(driver, ReportType.FUND)
    download_single_portfolio_view(driver, ReportType.FIN)

def download(driver: webdriver, url: str, download_url: str, downloaded_file: str, destination: str) -> None:
    driver.get(url)
    # Request the next page within the same session
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, "//img[@src='/img/svg/excel.svg' and @title='Export to CSV']"))
    )
    
    driver.get(download_url)
    
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, "//img[@src='/img/svg/excel.svg' and @alt='Export to CSV']"))
    )
    downloaded_file = os.path.join(DOWNLOAD_DIR, downloaded_file)
    while not os.path.exists(downloaded_file):
        time.sleep(1)
    destination_file = os.path.join(COPIED_DOWNLOADS_DIR, destination)
    print(f"Moving {downloaded_file} to {destination_file}")
    move(downloaded_file, destination_file)

regex_csvs = [ re.compile(r"MyValue.*\.csv"), re.compile(r"MyPortfolio.*\.csv")]
# Iterate through all files in the directory
for filename in os.listdir(DOWNLOAD_DIR):
    filepath = os.path.join(DOWNLOAD_DIR, filename)
    
    # Check if it's a file and matches the regex
    for regex_csv in regex_csvs:
        if os.path.isfile(filepath) and regex_csv.match(filename):
            try:
                os.remove(filepath)
                print(f"Removed: {filepath}\n")
            except Exception as e:
                print(f"Failed to remove {filepath}: {e}")

# Set up the web driver (e.g., Chrome)
# Connect to manually started Chrome browser
# "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" --remote-debugging-port=9222 --user-data-dir=/tmp/chrome_debug &
options = webdriver.ChromeOptions()
options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
driver = webdriver.Chrome(options=options)

#time.sleep(5)
print("Connected! Current page:", driver.title)
print("Current URL:", driver.current_url)

try:    
    download(driver, get_screener_url(ReportType.FUND.value), get_screener_download_url(), "MyValue.csv", "PE.csv")
    download(driver, get_screener_url(ReportType.FIN.value), get_screener_download_url(), "MyValue.csv", "PB.csv")
    download_portfolio(driver)

finally:
    # Close the browser
    driver.quit()

