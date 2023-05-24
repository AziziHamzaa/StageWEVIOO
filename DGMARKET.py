from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup
import pandas as pd
import locale
from datetime import datetime
from pymongo import MongoClient
from tqdm import tqdm
import configparser
import logging
import os

if not os.path.exists("logs"):
    os.makedirs("logs")

logging.basicConfig(filename='logs/scraper.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



class DgMarketScraper:
    def __init__(self, chromedriver_path, headless=False):
        self.chromedriver_path = chromedriver_path
        self.headless = headless

    @staticmethod
    def convert_date_format(date_string):
        input_format = "%B %d, %Y"
        output_format = "%d/%m/%Y"
        locale.setlocale(locale.LC_TIME, 'fr_FR.UTF-8')
        date_object = datetime.strptime(date_string, input_format)
        formatted_date = date_object.strftime(output_format)
        return formatted_date

    def scrape_appel_doffres(self, url, tbody_selector, row_selector):
        logging.info('DGMARKET Scraping started.')
        options = Options()
        options.headless = self.headless

        service = Service(self.chromedriver_path)
        driver = webdriver.Chrome(service=service, options=options)

        driver.get(url)
        driver.implicitly_wait(5)
        df = pd.DataFrame(columns=["Date", "Pays", "Description"])

        current_page = 1
        progress_bar = tqdm(desc="DGMARKET Scraping pages", unit=" page")
        while True:
        
            progress_bar.set_postfix({"Status": "In progress"})
            progress_bar.update(1)

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            try:
                table = soup.find("table", class_="table_list")
                rows = table.tbody.find_all(tbody_selector)  # Extract the rows
                for row in rows:
                    try:
                        tds = row.find_all(row_selector)
                        new_row = {
                            'Date': self.convert_date_format(tds[0].find('div', {'class': 'ln_date'}).text.strip()),
                            'Pays': tds[0].find_all('p')[0].find('span', {'class': 'ln_listing'}).find('a').text.strip(),
                            'Description': tds[0].find('div', {'class': 'ln_notice_title'}).find('a').text.strip()
                        }
                        new_df = pd.DataFrame(new_row, index=[0])
                        df = pd.concat([df, new_df], ignore_index=True)
                    except:
                        pass
            except Exception as e:
                logging.error(f'An error occurred during DGMARKET scraping: {str(e)}')
                pass

            try:
                next_button = driver.find_element(By.XPATH,
                                                '//a[contains(@href, "selPageNumber") and contains(@href, "d-446978-p={0}")]'.format(
                                                    current_page + 1))
                next_button.click()
                driver.implicitly_wait(5)
                current_page += 1
            except NoSuchElementException:
                break
            
        progress_bar.set_postfix({"Status": "Completed"})
        progress_bar.close()

        driver.quit()
        logging.info('DGMARKET Scraping completed.')
        return df

    def store_mongodb(self, ip_address_mongodb, database_name, collection_name, df, upsert=True):
        logging.info('DGMARKET Storing data started.')
        client = MongoClient(ip_address_mongodb)

        db = client[database_name]
        collection = db[collection_name]

        progress_bar = tqdm(desc="DGMARKET Storing data", total=len(df))

        for _ , row in df.iterrows():
            update_data = {
                'Date': row['Date'],
                'Pays': row['Pays'],
                'Description': row['Description']
            }

            collection.update_one({'Date': row["Date"], 'Description': row["Description"]}, {'$set': update_data}, upsert=upsert)

            progress_bar.update(1)

        progress_bar.close()
        client.close()
        logging.info('DGMARKET Storing data completed.')
