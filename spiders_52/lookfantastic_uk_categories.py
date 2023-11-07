import scrapy
from pathlib import Path
import datetime
import os
from dotenv import load_dotenv

""" load env file """
try:
    load_dotenv()
except:
    env_path = Path(".env")
    load_dotenv(dotenv_path=env_path)

class LookfantasticSpider(scrapy.Spider):
    name = 'lookfantastic_uk_categories'
    headers = {
  'accept-encoding': 'gzip, deflate, br',
  'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
}
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    ROTATING_PROXY = os.getenv("ROTATING_PROXY")
    custom_settings={
                    'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/lookfantastic_uk/%(name)s_{DATE}.json": {"format": "json",}},
                    'ROTATING_PROXY_LIST' : [ROTATING_PROXY]
                    }


    def start_requests(self):
        url="https://www.lookfantastic.com/"
        yield scrapy.Request(url,headers=self.headers,callback=self.parse)

    def parse(self, response):
        for first in response.xpath("//ul[@class='responsiveFlyoutMenu_levelOne ']/li")[5:11]:
            first_name=first.xpath("./a/text()").get("").strip()
            first_url = response.urljoin(first.xpath("./a/@href").get().strip(""))
            second_list=[]
            for second in first.xpath("./div/div/ul/li")[2:-1]:
                second_name=second.xpath("./a/@data-context|./span/span/text()").get("").strip()
                if second.xpath("./a/@href").get("").strip():
                    second_url= response.urljoin(second.xpath("./a/@href").get("").strip())
                else:
                    second_url=None
                if second_name:
                    third_list=[]
                    for third in second.xpath("./ul/li")[1:]:
                        third_name=third.xpath("./a/span/text()").get('').strip()
                        third_url=response.urljoin(third.xpath("./a/@href").get('').strip())
                        if third_url!=second_url:
                            third_list.append({"name":third_name,"url":third_url}.copy())
                    second_list.append({'name':second_name,"url":second_url,"category_crumb":third_list}.copy())
            yield {"name":first_name,"url":first_url,"category_crumb":second_list}
    
