import re
import os
import json
import scrapy
import datetime
from dotenv import load_dotenv

load_dotenv()
class TrinnySpider(scrapy.Spider):
    name = 'trinny_uk_categories'
    start_urls = ['https://trinnylondon.com/uk/']
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    ROTATING_PROXY = os.getenv("ROTATING_PROXY")
    custom_settings={'FEEDS' : {
    f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/trinny_uk/%(name)s_{DATE}.json": {
    "format": "json",
        }
    },
    'ROTATING_PROXY_LIST' : [ROTATING_PROXY]}
    def parse(self, response):
        # blocks = re.findall(r'\"navigation\"\:\{\"categories\"\:(\[[^>]*?\}\]\}\}\])\,',response.text)[0]  
        blocks = re.findall(r'\\\"navigation\\\"\:\{\\\"categories\\\":(\[[^>]*?\}\]\}\}\])\,',response.text)[0]
        blocks = blocks.replace('\\"','\"')  
        blocks = json.loads(blocks)
        
        for block in blocks:
            if block.get('subCategory',{}).get('childItems',{}):
                title = block.get('subCategory',{}).get('childItems',{})[0].get('name')
                if 'Category' in title:
                    item = {}
                    childItems = block.get('subCategory',{}).get('childItems',{})
                    item["name"] = 'skincare'
                    item["url"] = response.urljoin(f"/uk{childItems[0].get('productLinks')[0].get('linkDestination')}")
                    category_crumb = []
                    for productLinks in childItems[0].get('productLinks')[1:]:
                        sub_item = {}
                        sub_item['name'] = productLinks.get('linkText')
                        sub_item['url'] = response.urljoin(f"/uk{productLinks.get('linkDestination')}")
                        category_crumb.append(sub_item)
                            
                    item["category_crumb"] = category_crumb
                    yield item
                if 'Face' in title or 'Makeup Stacks' in title:
                    item = {}
                    childItems = block.get('subCategory',{}).get('childItems',{})
                    item["name"] = block.get('name')
                    item["url"] = response.urljoin(f"/uk{block.get('directLink')}")
                    child_category_crumb = []
                    for childitem in childItems:
                        child_item = {}
                        name = childitem.get('name')
                        if not 'Gifts' in name:
                            child_item['name'] = name
                            child_item["url"] = response.urljoin(f"/uk{childitem.get('productLinks')[0].get('linkDestination')}")
                            temp_list = []
                            for product in childitem.get('productLinks')[1:]:
                                product_item = {}
                                product_item['name'] = product.get('linkText')
                                product_item['url'] = response.urljoin(f"/uk{product.get('linkDestination')}")
                                temp_list.append(product_item)
                            child_item['category_crumb'] = temp_list
                            child_category_crumb.append(child_item)
                    item['category_crumb'] = child_category_crumb
                    yield item
                