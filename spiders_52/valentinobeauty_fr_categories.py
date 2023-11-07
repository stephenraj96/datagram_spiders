import scrapy
import os
from dotenv import load_dotenv
from pathlib import Path
import datetime

""" load env file """
try:
    load_dotenv()
except:
    env_path = Path(".env")
    load_dotenv(dotenv_path=env_path)

class ValentinobeautySpider(scrapy.Spider):

    name = "valentinobeauty_fr_categories"
    
    """ Get token in env file"""
    api_token=os.getenv("api_token")
    headers = {
  'authority': 'www.valentino-beauty.fr',
  'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
  'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
  'referer': 'https://www.valentino-beauty.fr/',
  'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Linux"',
  'sec-fetch-dest': 'document',
  'sec-fetch-mode': 'navigate',
  'sec-fetch-site': 'same-origin',
  'sec-fetch-user': '?1',
  'upgrade-insecure-requests': '1',
  'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'}

    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    custom_settings={
                    'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/valentinobeauty_fr/%(name)s_{DATE}.json": {"format": "json",}},
                    }
    
    def start_requests(self):
        self.spider_name=self.name
        """Intial main url request"""
        start_url=f"https://api.scrape.do/?token={self.api_token}&url=https://www.valentino-beauty.fr/maquillage-1/&customHeaders=true&device=desktop&render=true"
        
        yield scrapy.Request(start_url,headers=self.headers,callback=self.category)

    def category(self,response):
        # breakpoint()
        main_category=response.xpath("//ul[@class='c-navigation__list m-level-1']/li[@data-category='fragrances']|//ul[@class='c-navigation__list m-level-1']/li[@data-category='makeup']")
        for first in main_category:
            first_name=first.xpath(".//a/text()").get('').strip()
            first_url=first.xpath(".//a/@href").get()
            second_list=[]
            for second in first.xpath(".//*[@data-js-accordion-level='2']"):
                second_name=second.xpath("./span/a/text()").get("").strip()
                second_url=second.xpath("./span/a/@href").get("")
                third_list=[]
                for third in second.xpath("./ul/li")[:-1]:
                    third_name=third.xpath("./span/a/text()").get('').strip()
                    third_url=third.xpath("./span/a/@href").get()
                    third_list.append({"name":third_name,"url":third_url})
                second_list.append({"name":second_name,"url":second_url,"category_crumb":third_list}.copy())
            yield {"name":first_name,"url":first_url,"category_crumb":second_list}.copy()