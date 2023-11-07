import os
import scrapy
import datetime
from pathlib import Path
from dotenv import load_dotenv


""" load env file """
try:
    load_dotenv()
except:
    env_path = Path(".env")
    load_dotenv(dotenv_path=env_path)


class SephoraUkCategoriesSpider(scrapy.Spider):
    name = 'sephora_uk_categories'
    """ save file in s3"""
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    ROTATING_PROXY = os.getenv("ROTATING_PROXY")
    custom_settings={
        'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/sephora_uk/%(name)s_{DATE}.json": {"format": "json",}},
        'ROTATING_PROXY_LIST' : [ROTATING_PROXY]
    }
    start_urls = ['https://www.sephora.co.uk/']
    def parse(self, response):
        item = {}
        scraped_category_list = ['Makeup','Skincare','Fragrance']
        for category in response.xpath('//li[@class="primary li-menu "]'):
            category_name = category.xpath('./a/text()').get()
            if category_name.strip() in scraped_category_list:
                item['name'] = category_name.strip()
                item['url'] = response.urljoin(category.xpath('./a/@href').get())
                category_crumb_list = []
                fragrance_category_crumb_list = []
                if 'Fragrance' == category_name.strip():
                    for sub_category in category.xpath('./ul/li[@class="li-menu cms-menu single"]'):
                        category_crumb_dict = {}
                        for sub_one_category in sub_category.xpath("./ul/li"):
                            sub_category_crumb_dict = {}
                            if sub_one_category.xpath('./a/text()').get('').strip():
                                if not 'Top brands' in sub_one_category.xpath('./a/text()').get('').strip():
                                    sub_category_crumb_dict['name'] = sub_one_category.xpath('./a/text()').get('').strip()
                                    sub_category_crumb_dict['url'] = response.urljoin(sub_one_category.xpath('./a/@href').get())
                                    if 'HOME FRAGRANCE' == sub_category_crumb_dict['name']:
                                        sub_category_crumb_dict['category_crumb'] = []
                                        
                                    if '/home-fragrance/' in response.urljoin(sub_one_category.xpath('./a/@href').get()):
                                        
                                        sub_category_crumb_home_dict = {}
                                        sub_category_crumb_home_dict['name'] = sub_one_category.xpath('./a/text()').get('').strip()
                                        sub_category_crumb_home_dict['url'] = response.urljoin(sub_one_category.xpath('./a/@href').get())
                                        for fragrance_list in fragrance_category_crumb_list:
                                            if 'HOME FRAGRANCE' == fragrance_list['name']: 
                                                
                                                fragrance_list['category_crumb'].append(sub_category_crumb_home_dict)
                                    else:    
                                        fragrance_category_crumb_list.append(sub_category_crumb_dict)
                        
                    item['category_crumb'] = fragrance_category_crumb_list
                    yield item
                else:    
                    for sub_category in category.xpath('./ul/li[@class="li-menu "]'):
                        category_crumb_dict = {}    
                        category_crumb_dict['name'] = sub_category.xpath('./a/text()').get('').strip()
                        category_crumb_dict['url'] = response.urljoin(sub_category.xpath('./a/@href').get())
                        sub_one_category_crumb_list = []
                        for sub_one_category in sub_category.xpath("./ul/li[@class='']"):
                            sub_category_crumb_dict = {}
                            sub_category_crumb_dict['name'] = sub_one_category.xpath('./a/text()').get('').strip()
                            sub_category_crumb_dict['url'] = response.urljoin(sub_one_category.xpath('./a/@href').get())
                            sub_one_category_crumb_list.append(sub_category_crumb_dict)
                        category_crumb_dict['category_crumb'] = sub_one_category_crumb_list
                        category_crumb_list.append(category_crumb_dict)
                        item['category_crumb'] = category_crumb_list
                    yield item
                
