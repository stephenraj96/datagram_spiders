import scrapy
import os
import datetime
from dotenv import load_dotenv
from pathlib import Path

""" load env file """
try:
    load_dotenv()
except:
    env_path = Path(".env")
    load_dotenv(dotenv_path=env_path)

class GuerlainSpider(scrapy.Spider):
    name = 'guerlain_fr_categories'
    start_urls = ["https://www.guerlain.com/fr/fr-fr/maquillage/"]
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    ROTATING_PROXY_FR = os.getenv("ROTATING_PROXY_FR")
    custom_settings={'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/guerlain_fr/%(name)s_{DATE}.json": {"format": "json",}},
                    'ROTATING_PROXY_LIST' : [ROTATING_PROXY_FR]
                    }


    def parse(self, response):
        first_categories=response.xpath("//ul[@class='nav navbar-nav custom-nav nav-level-1-wrapper']/li")
        for first in first_categories:
            first_name= first.xpath("./button/text()").get('').strip()
            if first_name in ['Parfum','Maquillage','Soin']:
                first_url=response.urljoin(first.xpath("./button/@data-href").get('').strip())
                second_categories=first.xpath("./div/div/div")
                second_list=[]
                for second in second_categories[2:]:
                    for block in second.xpath(".//div[@class='dropdown-column-menu']"):
                        second_name= block.xpath(".//div/a/text()").get('').strip()
                        second_url=response.urljoin(block.xpath(".//div/a/@href").get('').strip())
                        third_categories=block.xpath("./ul/li")
                        third_list=[]
                        for third in third_categories[:-1]:
                            third_name=third.xpath(".//a/text()").get('').strip()
                            third_url=response.urljoin(third.xpath(".//a/@href").get('').strip())
                            if "lart-la-matiere" not in third_url and "customization" not in third_url:
                                third_list.append({"name":third_name,"url":third_url}.copy())
                        second_list.append({"name": second_name,"url": second_url,"category_crumb": third_list}.copy())
                item={"name": first_name,"url": first_url,"category_crumb": second_list}
                yield item