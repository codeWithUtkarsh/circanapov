# it can be used to clean HTML files
from trafilatura import extract,bare_extraction
from scrapfly import ScrapflyClient, ScrapeConfig, ScrapflyError
from lxml import html
import trafilatura
import json
from lxml import etree
from pprint import pprint

def format_data(data):
    # If data is already a Python object, no need for eval
    if isinstance(data, (dict, list)):
        return data
    # If it's a string, then use eval (be cautious with eval though)
    elif isinstance(data, str):
        return eval(data)
    else:
        raise TypeError("data must be a string, dict, or list")

scrapfly = ScrapflyClient(key="scp-live-01fc8534987042f4a2fe553c5cf6a2df", max_concurrency=2)
#url = f"https://www.kohls.com/product/prd-5896243/new-balance-running-phone-pouch.jsp?skuId=888783791953&search=5896243&submit-search=web-regular"
#url = f"https://www.insight.com/en_US/shop/product/40AY0090US/lenovo/40AY0090US/Lenovo-ThinkPad-Universal-USBC-Dock-docking-station-USBC-HDMI-2-x-DP-GigE/"
#url = f"https://www.walmart.com/ip/5164753187"
#url = "https://www.connection.com/product/tp-link-kasa-smart-wifi-plug-mini-2/ep10p2/41297988"
#url = "https://www.usa.canon.com/shop/p/eos-r100-rf-s18-45mm-f4-5-6-3-is-stm-lens-kit"
url = "https://www.crutchfield.com/p_68965C4P/.html"
listing_result = scrapfly.scrape(ScrapeConfig(url=url, render_js=True, country="US", asp=True, retry=False, rendering_wait=10000))
listing_html = listing_result.scrape_result['content']
mytree = html.fromstring(listing_html)
#print(extract(mytree))
data = bare_extraction(mytree)

# Serialize the data
formatted_data = format_data(data)
pprint(formatted_data)

# Convert to JSON string
#json_output = json.dumps(formatted_data, indent=2)

# Print the JSON string
#print(json_output)
