from RetailerBase import RetailerBase
from typing import Dict, Any, List
from Utility import log_message
import json
from scrapfly import ScrapflyClient, ScrapeConfig, ScrapflyError
from bs4 import BeautifulSoup
from datetime import datetime
import re
import os
import pprint
from jsonpath_ng import jsonpath, parse


class WalmartRetailer(RetailerBase):

    current_date = datetime.now().strftime('%Y%m%d')
    current_date_min = datetime.now().strftime('%Y%m%d%H%M%S%f')[:-7]
    DATA_DIR = "data"

    fields = {
    "ProductName": "props/pageProps/initialData/data/product/name",
    "UPC": {current_date_min},
    "shippingPolicy": "props/pageProps/initialData/data/product/fulfillmentLabel/2/shippingText",
    "ReturnPolicy": "props/pageProps/initialData/data/product/returnPolicy/returnPolicyText",
    "Price": "props/pageProps/initialData/data/product/priceInfo/currentPrice/priceString",
    "ProductHighlights": "props/pageProps/initialData/data/idml/productHighlights",
    "Description": "props/pageProps/initialData/data/idml/longDescription",
    "Specifications": "props/pageProps/initialData/data/idml/specifications",
    "ShortDescription": "props/pageProps/initialData/data/idml/shortDescription",
    "Product": "props/pageProps/initialData/data/product",
    "IDML": "props/pageProps/initialData/data/idml",
    "Reviews": "props/pageProps/initialData/data/reviews",
    "ImageURL": "props/pageProps/initialData/data/image"
    }
    
    json_paths = {
        "ProductName": "$.props.pageProps.initialData.data.product.name",
        "shippingPolicy": "$.props.pageProps.initialData.data.fulfillment.shippingPolicy",
        "ReturnPolicy": "$.props.pageProps.initialData.data.product.returnPolicy.returnPolicyText",
        "Price": "$.props.pageProps.initialData.data.product.priceInfo.currentPrice.priceString",
        "Screen size": "$.props.pageProps.initialData.data.idml.productHighlights[*]",
    #     "Ram memory": "$.props.pageProps.initialData.data.idml.productHighlights[?(@.name == 'Ram memory')].value",
    #     "Weight": "$.props.pageProps.initialData.data.idml.productHighlights[?(@.name == 'Weight')].value"
     }

    def extract_value(self,json_data, field_name, json_paths):
        if field_name in json_paths:
            json_path_expr = parse(json_paths[field_name])
            matches = json_path_expr.find(json_data)
            # Extract the value directly based on JSON path expression
            return [match.value for match in matches] if matches else None
        return None
    
    def process_json(self, url: str, scrapfly:ScrapflyClient) -> Dict[str, Any]:
        # Implement Walmart-specific JSON processing
        # log_message(f"In Walmart-specific JSON processing {url}")
        # listing_result = scrapfly.scrape(ScrapeConfig(url=url, render_js=True, country="US", asp=True, retry=False, rendering_wait=10000))
        # listing_html = listing_result.scrape_result['content']
        output_dir = r"C:\Users\SudheerRChinthala\circana\Srikanth\Data"
        output_file = os.path.join(output_dir, "WalmartListing.html")
        # with open(output_file, 'w', encoding='utf-8') as f:
        #     f.write(listing_html)
        # return
        with open(output_file, 'r', encoding='utf-8') as f:
            listing_html = f.read()
        soup = BeautifulSoup(listing_html, 'html.parser')   
        product_info = self.extract_product_info_from_html(soup)
        json_data = self.extract_json_data(soup, '__NEXT_DATA__')
        # file_path = r"C:\Users\SudheerRChinthala\circana\Srikanth\Data\WalmartJSONData.json"
        # with open(file_path, 'r', encoding='utf-8') as f:
        #     json_data = json.load(f)
    
        if json_data:
            json_output = json.dumps(json_data, indent=2, ensure_ascii=False)
            #data_json = self.extract_product_info_from_json(json_data)
            #print(f"got json {data_json}")
            if 'gtin13' in product_info:
                gtin13_value = product_info['gtin13']
                extracted_data = {key: (gtin13_value if key == "UPC" else self.get_value(json_data, path)) for key, path in self.fields.items()}
            else:
                extracted_data = {key: self.get_value(json_data, path) for key, path in self.fields.items()}
            #print(extracted_data)
            # print('Calling ..')
            # screen_size_expr = parse("$.props.pageProps.initialData.data.idml.productHighlights[*].value")
            # screen_size_matches = screen_size_expr.find(json_data)
            # screen_size = screen_size_matches[0].value if screen_size_matches else None
            # print(f"Screen size: {screen_size}")

            extracted_values = {}
            for field in self.json_paths.keys():
                extracted_values[field] = self.extract_value(json_data, field, self.json_paths)
           
            for field, value in extracted_values.items():
                print(f"{field}: {value}")
            # Create a directory name for the product using UPC and product name
            upc = extracted_data["UPC"]
            if not upc:
                upc = self.current_date_min 
            product_name = extracted_data["ProductName"].replace(" ", "_")
            sanitized_product_name = self.sanitize_filename(product_name,30) 

            # Retrieve the main product image URL
            image_url = extracted_data.get("ImageURL", "N/A")
            product_name = extracted_data.get("ProductName", "unknown_product").replace(" ", "_")
            description = extracted_data.get("Description", "No description available")
            timestamp = datetime.now().isoformat()
            if product_name and "ProductName" in extracted_data:
                sanitized_json_filename = self.sanitize_filename(product_name,30)
                json_filename = f"{upc}_{sanitized_json_filename}.json"
                base_dir = os.path.join("web_data_prd", "magellan", "walmart", self.current_date)
                max_dir_length = 100  # Adjust as needed based on your OS's maximum path length
                product_data_dir = os.path.join(base_dir, self.sanitize_filename(f"{upc}_{sanitized_product_name}", max_length=max_dir_length))
                os.makedirs(product_data_dir, exist_ok=True)
                json_file_path = os.path.join(product_data_dir, json_filename)
                with open(json_file_path, "w", encoding='utf-8') as json_file:
                    json.dump(extracted_data, json_file, indent=4, ensure_ascii=False)
            log_message(f"Successfully scraped product: {url}")
        return extracted_data

    def process_html(self, html_data: str) -> Dict[str, Any]:
        # Implement Walmart-specific HTML processing
        log_message(f"Implement Walmart-specific HTML processing")
        return {"html_processed": html_data}

    def merge_data(self, json_data: Dict[str, Any], html_data: Dict[str, Any]) -> Dict[str, Any]:
        # Implement Walmart-specific data merging
        log_message(f"Implement Walmart-specific data merging")
        return {**json_data, **html_data}
    
    def extract_product_info_from_html(self,soup):
        script_tags = soup.find_all('script', type='application/ld+json')
        product_info = {}
        for script in script_tags:
            try:
                json_content = json.loads(script.string)
                if 'gtin13' in json_content:
                    product_info['gtin13'] = json_content['gtin13']
                if 'product' in json_content:
                    product_info['product'] = json_content['product']
                if 'idml' in json_content:
                    product_info['idml'] = json_content['idml']
                if 'reviews' in json_content:
                    product_info['reviews'] = json_content['reviews']
                if 'image' in json_content:
                    product_info['image'] = json_content['image']            
            except json.JSONDecodeError:
                continue
        
        return product_info
    
    def extract_json_data(self,soup, script_id):
        try:
            script_tag = soup.find('script', {'id': script_id, 'type': 'application/json'})
            if script_tag and script_tag.string:
                json_content = script_tag.string
                data = json.loads(json_content)
                return data
            else:
                print(f"No script tag found with id '{script_id}' or script content is empty.")
                return None
        except (json.JSONDecodeError, TypeError) as e:
            print(f"Error parsing JSON data: {e}")
            return None
        
    def get_value(self,data, path):
        keys = path.split('/')
        for key in keys:
            if isinstance(data, list):
                try:
                    index = int(key)
                    data = data[index]
                except (ValueError, IndexError):
                    return None
            else:
                data = data.get(key, None)
            if data is None:
                return None
        return data

    def sanitize_filename(self,filename, max_length):
        # Remove illegal characters from filename
        sanitized_filename = re.sub(r'[\\/*?:"<>|,:]', "_", filename)

        # Truncate filename if it exceeds max_length
        if len(sanitized_filename) > max_length:
            sanitized_filename = sanitized_filename[:max_length]

        return sanitized_filename