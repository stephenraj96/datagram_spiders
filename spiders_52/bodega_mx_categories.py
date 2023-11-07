import os
import json
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


class BodegaMxCategoriesSpider(scrapy.Spider):
    name = 'bodega_mx_categories'
    """ save file in s3"""
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    ROTATING_PROXY = os.getenv("ROTATING_PROXY")
    custom_settings={
        'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/bodega_mx/%(name)s_{DATE}.json": {"format": "json",}},
        'ROTATING_PROXY_LIST' : [ROTATING_PROXY]
    }
    
    def start_requests(self):
        start_urls = 'https://nextgentheadless.instaleap.io/api/v3'
        payload = json.dumps([
                                {
                                    "operationName": "GetCategoryTree",
                                    "variables": {
                                    "getCategoryInput": {
                                        "clientId": "BODEGA_AURRERA",
                                        "storeReference": "9999"
                                    }
                                    },
                                    "query": "fragment CategoryFields on CategoryModel {\n  active\n  boost\n  hasChildren\n  categoryNamesPath\n  isAvailableInHome\n  level\n  name\n  path\n  reference\n  slug\n  photoUrl\n  imageUrl\n  shortName\n  isFeatured\n  isAssociatedToCatalog\n  __typename\n}\n\nfragment CategoriesRecursive on CategoryModel {\n  subCategories {\n    ...CategoryFields\n    subCategories {\n      ...CategoryFields\n      subCategories {\n        ...CategoryFields\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment CategoryModel on CategoryModel {\n  ...CategoryFields\n  ...CategoriesRecursive\n  __typename\n}\n\nquery GetCategoryTree($getCategoryInput: GetCategoryInput!) {\n  getCategory(getCategoryInput: $getCategoryInput) {\n    ...CategoryModel\n    __typename\n  }\n}"
                                }
                                ])   
        headers = {
                    'authority': 'nextgentheadless.instaleap.io',
                    'accept': '*/*',
                    'accept-language': 'en-US,en;q=0.9',
                    'apollographql-client-name': 'Ecommerce',
                    'apollographql-client-version': '3.24.15',
                    'content-type': 'application/json',
                    'origin': 'https://despensa.bodegaaurrera.com.mx',
                    'referer': 'https://despensa.bodegaaurrera.com.mx/',
                    'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'cross-site',
                    'token': '',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
}
        yield scrapy.Request(start_urls,method='POST',body=payload,headers=headers,callback=self.parse) 
    def parse(self, response):
        scraped_category_list = ['Lácteos y Cremería','Bebidas y Jugos','Artículos para Bebés y Niños']
        for category in response.json()[0].get('data').get('getCategory'):
            category_name = category.get('name','')
            if category_name.strip() in scraped_category_list:
                item = {}
                item['title'] = category_name.strip()
                item['page_url'] = f"https://despensa.bodegaaurrera.com.mx/ca/{category.get('slug','')}"
                item['category_id'] = category.get('reference','')
                category_crumb_list = []
                for sub_category in category.get('subCategories'):
                    sub_category_dict = {}
                    sub_category_dict['name'] = sub_category.get('name','')
                    sub_category_dict['page_url'] = f"https://despensa.bodegaaurrera.com.mx/ca/{sub_category.get('slug','')}"
                    sub_category_dict['category_id'] = sub_category.get('reference','')
                    category_crumb_list.append(sub_category_dict)
                item['category_crumb'] = category_crumb_list
                yield item
                        
                
