import os
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from selenium.common.exceptions import NoSuchElementException
from shutil import move
import random
from report_types import ReportType
from lib import get_portfolio_filename, DIR

DOWNLOAD_DIR = "/Users/nenad.noveljic/Downloads"
ROOT_URL = "https://www.marketinout.com"
TOOLS_URL = f"{ROOT_URL}/tools"

def check_captcha(driver: webdriver) -> None:
    try:
        # TODO: get teh exactl Invalid captcha element
        page = driver.find_element(By.ID, "captcha-error")
        if page.is_displayed():
            raise Exception("Captcha error")
    except NoSuchElementException:
        return False

def get_screener_url(view: str) -> str:
    return f"{ROOT_URL}/stock-screener/stocks.php?f=1&screen_id=413136&view={view}"

def get_portfolio_url(view: ReportType) -> str:
    return f"{TOOLS_URL}/portfolio.php?view={view}&bd=&sorting=company&plt=a"

def get_portfolio_download_url(view: ReportType) -> str:
    return f"{TOOLS_URL}/csv.csv?pid=10810&view={view}"

def get_screener_download_url() -> str:
    return f"{ROOT_URL}/stock-screener/csv_stocks.csv?f=1&screen_id=413136"
    
def download_portfolio(driver: webdriver) -> None:
    download(
        driver, get_portfolio_url(ReportType.FUND.value), get_portfolio_download_url(ReportType.FUND.value), "MyPortfolio.csv", 
        get_portfolio_filename(ReportType.FUND.value)
    )
    download(
        driver, get_portfolio_url(ReportType.FIN.value), get_portfolio_download_url(ReportType.FIN.value), "MyPortfolio.csv", 
        get_portfolio_filename(ReportType.FIN.value)
    )

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
    move(os.path.join(downloaded_file), 
         os.path.join(os.path.dirname((os.path.abspath(__file__))), f"{destination}"))

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

