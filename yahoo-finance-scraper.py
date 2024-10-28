import os
import time
import pandas as pd
import logging
from dotenv import load_dotenv
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Load environment variables from .env file
load_dotenv()

# Environment variables
TICKER_FILE = os.getenv("TICKER_FILE", "Tickers.csv")  # Path to ticker symbols file
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "today_stock_market_snapshot.csv")  # Output CSV file name

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize the DataFrame to store data
columns = ['Ticker', 'Company Name', 'Market Cap', 'Current Price', 'Open', 'Volume', 'PE', 'EPS', 'BETA', 'Earnings Date']
df = pd.DataFrame(columns=columns)

def init_driver():
    """Initialize and configure the Chrome driver."""
    options = uc.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    return uc.Chrome(options=options)

def scrape_ticker_data(driver, ticker):
    """Scrapes data for a given stock ticker from Yahoo Finance."""
    url = "https://finance.yahoo.com/"
    driver.get(url)
    try:
        # Search for ticker
        search_box = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#yfin-usr-qry"))
        )
        search_box.send_keys(ticker)

        search_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#header-desktop-search-button"))
        )
        search_button.click()
        
        time.sleep(2)  # Short pause for page load after search
        
        # Extract required information
        data = {
            "Ticker": ticker,
            "Company Name": get_element_text(driver, '#quote-header-info h1'),
            "Market Cap": get_element_text(driver, 'tr:contains("Market Cap") td.Ta\(end\)'),
            "Current Price": get_element_text(driver, 'fin-streamer.Fw\(b\).Fz\(36px\)'),
            "Open": get_element_text(driver, 'tr:contains("Open") td.Ta\(end\)'),
            "Volume": get_element_text(driver, 'tr:contains("Volume") td.Ta\(end\)'),
            "PE": get_element_text(driver, 'tr:contains("PE Ratio") td.Ta\(end\)'),
            "EPS": get_element_text(driver, 'tr:contains("EPS") td.Ta\(end\)'),
            "BETA": get_element_text(driver, 'tr:contains("Beta") td.Ta\(end\)'),
            "Earnings Date": get_element_text(driver, 'tr:contains("Earnings Date") td.Ta\(end\)')
        }
        logging.info(f"Data scraped for {ticker}: {data}")
        return data

    except (TimeoutException, NoSuchElementException) as e:
        logging.warning(f"Failed to scrape data for {ticker}: {e}")
        return None

def get_element_text(driver, css_selector):
    """Helper function to retrieve text from a page element by CSS selector."""
    try:
        element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
        )
        return element.text
    except (TimeoutException, NoSuchElementException):
        logging.warning(f"Element not found for selector: {css_selector}")
        return None

def main():
    """Main function to run the scraping script."""
    with open(TICKER_FILE, 'r') as f:
        tickers = [line.strip() for line in f]

    driver = init_driver()
    try:
        for count, ticker in enumerate(tickers, 1):
            logging.info(f"Processing {count}/{len(tickers)}: {ticker}")
            data = scrape_ticker_data(driver, ticker)
            if data:
                global df
                df = df.append(data, ignore_index=True)
            time.sleep(1)  # Brief pause between requests

        df.to_csv(OUTPUT_FILE, index=False)
        logging.info(f"Scraping completed. Data saved to {OUTPUT_FILE}")

    finally:
        driver.quit()

if __name__ == "__main__":
    main()
