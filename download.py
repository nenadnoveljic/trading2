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

DOWNLOAD_DIR = "/Users/nenad.noveljic/Downloads"
ROOT_URL = "https://www.marketinout.com"
TOOLS_ROOT_URL = f"{ROOT_URL}/tools"

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

def get_portfolio_url(view: str) -> str:
    return f"{TOOLS_ROOT_URL}/portfolio.php?view={view}&bd=&sorting=company&plt=a"

def get_portfolio_download_url(view: str) -> str:
    return f"{TOOLS_ROOT_URL}/csv.csv?pid=10810&view={view}"

def get_screener_download_url() -> str:
    return f"{ROOT_URL}/stock-screener/csv_stocks.csv?f=1&screen_id=413136"
    
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
         os.path.join(os.path.dirname((os.path.abspath(__file__))), f"{destination}.csv"))

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
options = webdriver.ChromeOptions()
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_experimental_option('excludeSwitches', ['enable-automation'])
options.add_experimental_option('useAutomationExtension', False)
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
driver = webdriver.Chrome(options=options)

def sleep_random():
    """
    Sleep for a random amount of time between 1000 and 3000 milliseconds.
    This helps to avoid detection as a bot by introducing randomized delays.
    """
    sleep_time_ms = 1000 + 2000 * random.random()  # Random value between 1000 and 3000 milliseconds
    time.sleep(sleep_time_ms / 1000)  # Convert milliseconds to seconds for time.sleep
    return sleep_time_ms

def enter_username(driver: webdriver, username: str) -> None:
    """
    Enter username with human-like typing behavior
    """
    username_field = driver.find_element(By.ID, "l")
    for char in username:
        username_field.send_keys(char)
        time.sleep(random.uniform(0.1, 0.3))

try:
    with open('config.ini', 'r') as config_file:
        for line in config_file:
            if line.startswith('password='):
                password = line.split('=', 1)[1].strip()
            if line.startswith('user='):
                user = line.split('=', 1)[1].strip()

    # Open the login page
    driver.get("https://www.marketinout.com/home/login.php")
    
    # Locate the username text field and enter the username
    # Use the enter_username function to input the username
    enter_username(driver, user)
    
    check_captcha(driver)
    sleep_random()
    # Check for captcha before entering username
    check_captcha(driver)
    
    # Add a random delay after entering username to mimic human behavior
    sleep_random()
    
    # Check if there's an error message about invalid captcha
    try:
        error_message = driver.find_element(By.XPATH, "//div[contains(@class, 'error') or contains(text(), 'Invalid captcha')]")
        if "captcha" in error_message.text.lower():
            print("Captcha error detected. Refreshing and trying again...")
            driver.refresh()
            # Wait for page to reload
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "l"))
            )
            # Re-enter username
            enter_username(driver, user)
            check_captcha(driver)
            sleep_random()
    except:
        # No error message found, continue with login
        pass
    
    # Locate and press the submit button
    submit_button = driver.find_element(By.NAME, "submit_button")
    submit_button.click()
    
    check_captcha(driver)
    
    # Wait for the password field to become visible
    password_field = WebDriverWait(driver, 10).until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, "input[type='password']"))
    )
    
    sleep_random()
    check_captcha(driver)

    
    for char in password:
        password_field.send_keys(char)
        time.sleep(random.uniform(0.1, 0.3))

    sleep_random()
    check_captcha(driver)
    # Press the submit button again
    submit_button = driver.find_element(By.NAME, "submit_button")
    submit_button.click()
    
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, '//a[@href="/home/my_account.php"]'))
    )

    download(driver, get_screener_url("fund"), get_screener_download_url(), "MyValue.csv", "PE")
    download(driver, get_screener_url("fin"), get_screener_download_url(), "MyValue.csv", "PB")
    download(driver, get_portfolio_url("fund"), get_portfolio_download_url("fund"), "MyPortfolio.csv", "portfolio")
    download(driver,get_portfolio_url("fin"), get_portfolio_download_url("fin"), "MyPortfolio.csv", "portfolio_PB")  

finally:
    # Close the browser
    driver.quit()

