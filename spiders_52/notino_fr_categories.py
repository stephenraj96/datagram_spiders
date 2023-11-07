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

class NotinoSpider(scrapy.Spider):
    name = 'notino_fr_categories'
    start_urls = ["https://www.notino.fr/"]
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    ROTATING_PROXY_FR = os.getenv("ROTATING_PROXY_FR")
    # {'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/notino_fr/%(name)s_{DATE}.json": {"format": "json",}},
    custom_settings= {'ROTATING_PROXY_LIST' : [ROTATING_PROXY_FR],
                      'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/notino_fr/%(name)s_{DATE}.json": {"format": "json",}}
                    }


    def parse(self, response):
        main_rows=response.xpath("//div[@class='zsBLfrsPE21KypJnI_jc']/div")
        for main in main_rows:
            first_name=main.xpath("./div/a/div/div/text()").get('').strip()
            first_url=response.urljoin(main.xpath("./div/a/@href").get('').strip())
            if first_name in ['Parfum','Maquillage',"Cheveux","Visage",'Corps','Dents','Parfums d’ambiance','Homme','Dermo-cosmétique']:
                second_rows=main.xpath("./div/div/div/div")
                second_list=[]
                for second in second_rows[1:]:
                    for third in second.xpath("./div"):
                        second_name=third.xpath("./a/text()").get()
                        if third.xpath("./a/@href").get():
                            second_url=response.urljoin(third.xpath("./a/@href").get())
                        else:
                            second_url=None
                        third_list=[]
                        for fourth in third.xpath("./div/a"):
                            third_name=fourth.xpath('./text()').get('')
                            if third_name=="voir tout":
                                continue
                            third_url=response.urljoin(fourth.xpath('./@href').get())
                            third_list.append({"name":third_name,"url":third_url}.copy())
                        if not third_list:
                            second_list.append({"name": second_name,"url": second_url}.copy())
                        else:
                            second_list.append({"name": second_name,"url": second_url,"category_crumb": third_list}.copy())
                yield {"name": first_name,"url": first_url,"category_crumb": second_list}
                