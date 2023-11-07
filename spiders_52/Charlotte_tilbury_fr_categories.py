import os
import scrapy
from datetime import datetime

class CharlotteTilburyFRCategoriesSpider(scrapy.Spider):
    name = 'Charlotte_tilbury_fr_categories'
    CURRENT_DATETIME = datetime.now() #today's date
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d") #2023-04-25
    DATE=CURRENT_DATE.replace("-","_") #2023_04_25
    ROTATING_PROXY = os.getenv("ROTATING_PROXY_FR")
    custom_settings={'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/Charlotte_tilbury_fr/%(name)s_{DATE}.json": {"format": "json",}},
                     'ROTATING_PROXY_LIST' : [ROTATING_PROXY]
                     }
    def start_requests(self):
        start_urls = ['https://www.charlottetilbury.com/fr/']
        yield scrapy.Request(start_urls[0],callback=self.parse)
    
    def parse(self, response):
        for menu_xpath in response.xpath('//div[contains(@class,"HeaderMenu__item HeaderMenu__item--parent")]'):
            item = {}
            category_name = menu_xpath.xpath('./a/span/text()').get()
            if category_name in ['Soin','Maquillage']:
                category_url = menu_xpath.xpath('./a/@href').get()
                item['name'] = category_name
                item['url'] = response.urljoin(category_url)
                if category_name in ['Soin']:
                    category_crumb = []
                    for sub_menu_xpath in menu_xpath.xpath('./article//ul/li'):
                        sub_item = {}
                        name = sub_menu_xpath.xpath('./a/text()').get()
                        if not name in [None,'Voir tout le soin','-10% sur les kits beauté à créer soi-même !']:
                            sub_item['name'] = sub_menu_xpath.xpath('./a/text()').get()
                            sub_item['url'] = response.urljoin(sub_menu_xpath.xpath('./a/@href').get())
                            category_crumb.append(sub_item)
                    item['category_crumb'] = category_crumb
                    yield item
                else:
                    category_crumb = []
                    for sub_menu_xpath in menu_xpath.xpath('./article/div/section/div/div'):
                        sub_item = {}
                        name = sub_menu_xpath.xpath('./div/text()').get()
                        sub_item['name'] = name
                        sub_item['url'] = ''
                        sub_crumb = []
                        if not name in ['ENCORE PLUS DE MAGIE !']:
                            for sub_crumb_xpath in sub_menu_xpath.xpath('./a'):
                                sub_crumb_dict = {}
                                sub_crumb_name = sub_crumb_xpath.xpath('./div/text()').get()
                                sub_crumb_url = response.urljoin(sub_crumb_xpath.xpath('./@href').get())
                               
                                if not 'Voir tous' in sub_crumb_name and not 'Trouvez le produit adapté à votre teint ' in sub_crumb_name:
                                    sub_crumb_dict['name'] = sub_crumb_name
                                    sub_crumb_dict['url'] = sub_crumb_url
                                    sub_crumb.append(sub_crumb_dict)
                                else:
                                    sub_item['url'] = sub_crumb_url
                            sub_item['category_crumb'] = sub_crumb
                            category_crumb.append(sub_item)
                    item['category_crumb'] = category_crumb
                    yield item