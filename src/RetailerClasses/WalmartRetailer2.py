import json
import httpx
from parsel import Selector
import jsonpath_ng as jp
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
        

output_dir = r"C:\Users\SudheerRChinthala\circana\Srikanth\Data"
output_file = os.path.join(output_dir, "WalmartListing.html")

with open(output_file, 'r', encoding='utf-8') as f:
    listing_html = f.read()
soup = BeautifulSoup(listing_html, 'html.parser')   
json_data = extract_json_data(soup, '__NEXT_DATA__')
data = json.loads(json_data)