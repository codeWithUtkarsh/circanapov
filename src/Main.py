from WebScraperEngine import WebScraperEngine
from RetailerFactory import RetailerFactory
from Utility import log_message,print_banner

def main():
    config_file = r"C:\Users\SudheerRChinthala\circana\Srikanth\PoV\config\UrlStrategiesConfig.json"  # Load URL strategies from the config
    retailer_name = 'walmart'
    sku = '5164753187'
    print_banner("Scraper Engine")
    #log_message(f"Retailer - {retailer_name}, SKU - {sku}")
    engine = WebScraperEngine(config_file)
    
    # Try scraping using all available strategies
    scraped_data = engine.scrape_data(retailer_name, SKU=sku)
    #log_message(f'{scraped_data}')
    # Store the scraped data
    # if scraped_data and 'error' not in scraped_data:
    #     engine.store_data(scraped_data)
    # else:
    #     print("Failed to retrieve data using available strategies.")

if __name__ == "__main__":
    main()
