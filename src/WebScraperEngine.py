from RetailerFactory import RetailerFactory
from typing import Dict, Any, List
from Utility import log_message
from scrapfly import ScrapflyClient, ScrapeConfig, ScrapflyError
import json

class WebScraperEngine:
    def __init__(self, config_file: str):
        self.config = self.load_config(config_file)
        self.retailer_factory = RetailerFactory(config_file)
        self.scrapfly = ScrapflyClient(key="scp-live-01fc8534987042f4a2fe553c5cf6a2df", max_concurrency=2)

    def process_url(self, retailer_name: str, strategy_name: str, **kwargs) -> str:
        retailer = self.retailer_factory.get_retailer(retailer_name)
        url = retailer.get_url(strategy_name, **kwargs)
        return url
    
    def load_config(self, config_file: str) -> Dict[str, Any]:
        with open(config_file, 'r') as file:
            return json.load(file)

    def scrape_data(self, retailer_name: str, **kwargs) -> Dict[str, Any]:
        '''
        Here we would have excel/csv/db data load
        create a dataframe df accordingly
        Iterate over each element of df and continue processing.
        For test hardcoded below values
        '''
        retailer_name = 'walmart'
        sku = '5164753187'                                                        
        log_message(f"Retailer - {retailer_name}, SKU - {sku}")
        retailer = self.retailer_factory.get_retailer(retailer_name)
        log_message(f'------------------------------------------------------------------')
        log_message(f'Processing Strategies for {retailer_name} - {list(retailer.strategies)}')
        log_message(f'------------------------------------------------------------------')
        # Iterate over all strategies available for the retailer
        for strategy_name in retailer.strategies.keys():
            try:
                url = self.process_url(retailer_name, strategy_name, **kwargs)
                log_message(f'------------------------------------------------------------------')
                log_message(f"Trying strategy: {strategy_name}, URL: {url}")
                log_message(f'------------------------------------------------------------------')
                
                # Get the current strategy's fetch type
                strategy = retailer.strategies[strategy_name] 
                strategy_config = self.config[retailer_name][strategy_name]
                data_fetch_type = strategy_config.get("DataFetchType", "Both")  # Default to Both if not specified
                log_message(f"DataFetchType: {data_fetch_type}")
                json_data, html_data = None, None
                
                # Scrape data based on DataFetchType
                if data_fetch_type == "fulljson":
                    json_data = retailer.process_json(url,self.scrapfly)
                elif data_fetch_type == "fullhtml":
                    html_data = retailer.process_html(url)
                elif data_fetch_type == "Both":
                    json_data = retailer.process_json(url)
                    html_data = retailer.process_html(url)
                
                # Merge JSON and HTML data if both are fetched
                if json_data and html_data:
                    merged_data = retailer.merge_data(json_data, html_data)
                else:
                    merged_data = json_data or html_data  # Return whichever is available

                # If valid data is returned, break the loop and return the merged data
                if merged_data and 'error' not in merged_data:
                    log_message(f"Strategy '{strategy_name}' succeeded.")
                    return merged_data
                else:
                    log_message(f"Strategy '{strategy_name}' failed to retrieve valid data.")
            
            except Exception as e:
                log_message(f"Error while processing strategy '{strategy_name}': {e}")
        
        # If no strategy succeeds, raise an error or return empty data
        log_message(f"No valid data found using any strategy for retailer: {retailer_name}")
        return {"error": "No valid data found"}

    # def store_data(self, data: Dict[str, Any], filename: str):
    #     # """Store the scraped data to a file."""
    #     # with open(filename, 'w') as f:
    #     #     json.dump(data, f, indent=4)
    #     log_message(f"Data stored to {filename}")
    def store_data(scraped_data):
        log_message(f"Data saved successfully")

    