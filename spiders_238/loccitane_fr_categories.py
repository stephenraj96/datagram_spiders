import os
import re
import scrapy
import datetime
import requests
from parsel import Selector
from pathlib import Path
from dotenv import load_dotenv


""" load env file """
try:
    load_dotenv()
except:
    env_path = Path(".env")
    load_dotenv(dotenv_path=env_path)

class LoccitaneFRCategiriesSpider(scrapy.Spider):
    name = "loccitane_fr_categories"
    api_token=os.getenv("api_token")

    """ save file in s3"""
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    custom_settings={
        'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/loccitane_fr/%(name)s_{DATE}.json": {"format": "json",}
        }
    }
    
    
    def start_requests(self):
        start_url=[f"https://api.scrape.do/?token={self.api_token}&url=https://fr.loccitane.com/"]
        headers = {
            'accept': 'text/html;charset=UTF-8',
            'accept-language': 'en-GB,en;q=0.9',
            'content-type': 'text/html;charset=UTF-8',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
        }
        for url in start_url:
            yield scrapy.Request(url=url, callback=self.parse,headers=headers)

    def retry_function(self,url):
        retry_count = 0
        while retry_count <=10:
            retry_response=requests.get(url)
            retry_response_text=retry_response.text
            retry_response=Selector(text=retry_response.text)
            if re.search(r'Please\s*enable\s*JS\s*and\s*disable',retry_response_text):
                retry_count+=1
            else:
                break
        return retry_response,retry_response_text

    def parse(self, response):
        if re.search(r'Please\s*enable\s*JS\s*and\s*disable',response.text):
            list_page_response,list_page_response_text = self.retry_function(response.url)
            response = list_page_response
        
        scraped_category_list = ['Soin Visage','Soin Corps','Soin Cheveux','Soin Mains','Parfum','Homme']
        item = {}
        for category in response.xpath('//ul[@class="o-level-1-container"]/li'):
            category_name = category.xpath('./a/text()').get()
            for scraped_category in scraped_category_list:
                if scraped_category.lower() == category_name.lower().strip():
                    category_url =  category.xpath('./a/@href').get()
                    item["name"] = category_name.strip()
                    item["url"] = category_url
                    category_crumb_list = []
                    for sub_menu in category.xpath('./div/ul/li[@class="m-level-2 m-mobile-parent"]/ul/li/a[@class="a-level-3-link"]'):
                        category_crumb_dict = {}
                        category_crumb_dict['name'] = sub_menu.xpath('./text()').get().strip()
                        category_crumb_dict['url'] = sub_menu.xpath('./@href').get()
                        category_crumb_list.append(category_crumb_dict)
                    item['category_crumb'] = category_crumb_list
                    yield item

        