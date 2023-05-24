from DGMARKET import *
from TunAppel import *

def main():

    config = configparser.ConfigParser()
    config.read('config.ini')

    chromedriver_path = config.get('General', 'chromedriver_path')
    mongodb_address = config.get('MongoDB', 'ip_address')
    collection_name = config.get('MongoDB', 'collection_name')

    #-------------- DGMARKET ---------------> 

    url_DGMARKET = config.get('General', 'url_DGMARKET')
    
    tbody_selector_DGMARKET = config.get('General', 'tbody_selector_DGMARKET')
    row_selector_DGMARKET = config.get('General', 'row_selector_DGMARKET')

    scraper_DGMARKET = DgMarketScraper(chromedriver_path, headless=config.getboolean('Scraper', 'headless'))
    df = scraper_DGMARKET.scrape_appel_doffres(url_DGMARKET, tbody_selector_DGMARKET, row_selector_DGMARKET)
    
    db_name_DGMARKET = config.get('MongoDB', 'database_name_DGMARKET')
    scraper_DGMARKET.store_mongodb(mongodb_address, db_name_DGMARKET, collection_name, df, upsert=True)

    #-------------- Tunisie Appel Offres ---------------> 
    

    url_TunAppel = config.get('General', 'url_TunAppel')
    tbody_selector_TunAppel = config.get('General', 'tbody_selector_TunAppel')
    row_selector_TunAppel = config.get('General', 'row_selector_TunAppel')

    scraper_TunAppel = TunAppelScraper(chromedriver_path, headless=config.getboolean('Scraper', 'headless'))
    df = scraper_TunAppel.scrape_appel_doffres(url_TunAppel, tbody_selector_TunAppel, row_selector_TunAppel)

    
    db_name_TunAppel = config.get('MongoDB', 'database_name_TunAppel')

    scraper_TunAppel.store_mongodb(mongodb_address, db_name_TunAppel, collection_name, df, upsert=True)

    #-------------- Tuneps --------------->    


if __name__ == '__main__':
    main()