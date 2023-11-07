import os
import scrapy
from datetime import datetime

class ShiseidofrCategoriesSpider(scrapy.Spider):
    name = 'shiseido_fr_categories'

    start_urls = ['https://www.shiseido.fr/fr/fr/']
    CURRENT_DATETIME = datetime.now() #today's date
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d") #2023-04-25
    DATE=CURRENT_DATE.replace("-","_") #2023_04_25
    ROTATING_PROXY = os.getenv("ROTATING_PROXY_FR")
    custom_settings={
    'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/shiseido_fr/%(name)s_{DATE}.json": {"format": "json",}},
                     'ROTATING_PROXY_LIST' : [ROTATING_PROXY],
                     }
    def parse(self, response):
        for breadcrumb in response.xpath('//div[@class="menu-list"]//li[contains(@class,"sub-menu")]'):
            text=str(breadcrumb.xpath('./a/@data-link-name').get('').strip()).upper()
            if text in ["SOIN","MAQUILLAGE","HOMME"]:
                categories={}
                categories['name']=text
                url=breadcrumb.xpath('./a/@href').get('').strip()
                if not str(url).startswith('http'):
                    url="https://www.shiseido.fr"+str(url)
                categories['url']=url
                categories['category_crumb']=[]
                if breadcrumb.xpath('.//ul[@class="level-2 sub-menu"]//li[@class="level-2-link"]//following-sibling::li/ul[@class="column"]//li[@class="no-sub-cat-2"]'):
                    for sub_no_inner_category in breadcrumb.xpath('.//ul[@class="level-2 sub-menu"]//li[@class="level-2-link"]//following-sibling::li/ul[@class="column"]//li[@class="no-sub-cat-2"]'):
                        inner_text=sub_no_inner_category.xpath('./a/@data-link-name').get('').strip()
                        no_inner_subt={}
                        no_inner_subt['name']=inner_text.replace('VIEW ALL ','')
                        inner_sub_url=sub_no_inner_category.xpath('./a/@href').get('').strip()
                        if not str(inner_sub_url).startswith('http'):
                            inner_sub_url= "https://www.shiseido.fr"+str(inner_sub_url)
                        no_inner_subt['url']=inner_sub_url
                        categories['category_crumb'].append(no_inner_subt)
                
                for sub_inner_category in breadcrumb.xpath('.//ul[@class="level-2 sub-menu"]//li[@class="level-2-link"]//following-sibling::li/ul[@class="column"]//li[@class="level-3-link"]'):
                    sub_categories={}  
                    name=sub_inner_category.xpath('./a[@class="see-all-link"]/@data-link-name').get('').strip()
                    if name:
                        sub_categories['name']=name.replace('VIEW ALL ','')
                        sub_categories_url=sub_inner_category.xpath('./a[@class="see-all-link"]/@href').get('').strip()
                        if not str(sub_categories_url).startswith('http'):
                            sub_categories_url ="https://www.shiseido.fr"+str(sub_categories_url)
                        sub_categories['url']=sub_categories_url
                        sub_categories['category_crumb']=[]
                        for inner_sub in sub_inner_category.xpath('.//following-sibling::li'):
                            inner_text=inner_sub.xpath('./a/@data-link-name').get('').strip()
                            inner_subt={}
                            inner_subt['name']=inner_text.replace('VIEW ALL ','')
                            inner_sub_url=inner_sub.xpath('./a/@href').get('').strip()
                            if not str(inner_sub_url).startswith('http'):
                                inner_sub_url= "https://www.shiseido.fr"+str(inner_sub_url)
                            inner_subt['url']=inner_sub_url
                            sub_categories['category_crumb'].append(inner_subt)
                        categories['category_crumb'].append(sub_categories)
                yield categories
            if text in ["PARFUM","SOLAIRE"]:
                categories={}
                categories['name']=text.replace('VIEW ALL ','')
                url=breadcrumb.xpath('./a/@href').get('').strip()
                if not str(url).startswith('http'):
                    url="https://www.shiseido.fr"+str(url)
                categories['url']=url
                categories['category_crumb']=[]
                for sub_inner_category in breadcrumb.xpath('.//ul[@class="level-2 sub-menu"]//li[@class="level-2-link"]//following-sibling::li/ul[@class="column"]/li'):
                    inner_text=sub_inner_category.xpath('./a/@data-link-name').get('').strip()
                    inner_subt={}
                    inner_subt['name']=inner_text.replace('VIEW ALL ','')
                    inner_sub_url=sub_inner_category.xpath('./a/@href').get('').strip()
                    if not str(inner_sub_url).startswith('http'):
                        inner_sub_url= "https://www.shiseido.fr"+str(inner_sub_url)
                    inner_subt['url']=inner_sub_url
                    categories['category_crumb'].append(inner_subt)
                yield categories

