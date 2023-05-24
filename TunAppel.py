from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm
import configparser
import logging
import os


if not os.path.exists("logs"):
    os.makedirs("logs")
logging.basicConfig(filename='logs/scraper.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class TunAppelScraper:
    def __init__(self, chromedriver_path, headless=False):
        self.chromedriver_path = chromedriver_path
        self.headless = headless

    def scrape_appel_doffres(self, url, tbody_selector, row_selector):
        logging.info('TunAppel Scraping started.')
        options = Options()
        options.headless = self.headless

        service = Service(self.chromedriver_path)
        driver = webdriver.Chrome(service=service, options=options)

        driver.get(url)
        driver.implicitly_wait(5)

        df = pd.DataFrame(columns=["Nom", "Date", "Pays", "Reference", "Description", "DateEcheance"])

        progress_bar = tqdm(desc="TunAppel Scraping pages", unit=" page")

        while True:
            progress_bar.set_postfix({"Status": "In progress"})
            progress_bar.update(1)

            soup = BeautifulSoup(driver.page_source, 'html.parser')

            table = soup.find("table", class_="table_taille")
            try:
                rows = table.tbody.find_all(tbody_selector)

                for row in rows:
                    tds = row.find_all(row_selector)
                    new_row = {
                        'Nom': tds[1].text.strip(),
                        'Date': tds[2].text.strip(),
                        'Pays': tds[3].text.strip().split("./")[1], # the output is like "Nat./TUN" thats why we use .split("./")[1]
                        'Reference': tds[4].text.strip(),
                        'Description': tds[5].text.strip(),
                        'DateEcheance': tds[7].text.strip()
                    }
                    new_df = pd.DataFrame(new_row, index=[0])
                    df = pd.concat([df, new_df], ignore_index=True)
            except Exception as e:
                logging.error(f'An error occurred during TunAppel scraping: {str(e)}')

            try:
                next_button = driver.find_element(By.XPATH, '//a[contains(@class, "pagination") and @title="Page suivant"]')
                next_button.click()
                driver.implicitly_wait(5)
            except NoSuchElementException:
                break

        progress_bar.set_postfix({"Status": "Completed"})
        progress_bar.close()

        driver.quit()
        logging.info('TunAppel Scraping completed.')
        return df

    def store_mongodb(self, ip_address_mongodb, database_name, collection_name, df, upsert=True):
        logging.info('TunAppel Storing data started.')
        client = MongoClient(ip_address_mongodb)

        db = client[database_name]
        collection = db[collection_name]

        progress_bar = tqdm(desc="TunAppel Storing data", total=len(df))

        for _ , row in df.iterrows():
            update_data = {
                'Nom': row['Nom'],
                'Date': row['Date'],
                'Pays': row['Pays'],
                'Reference': row['Reference'],
                'Description': row['Description'],
                'DateEcheance': row['DateEcheance']
            }

            collection.update_one({'Date': row["Date"], 'Description': row["Description"]}, {'$set': update_data}, upsert=upsert)

            progress_bar.update(1)

        progress_bar.close()

        client.close()
        logging.info('TunAppel Storing data completed.')

