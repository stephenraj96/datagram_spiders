import os
import scrapy
import datetime


class MarionnaudFrCategoriesSpider(scrapy.Spider):
    name = "marionnaud_fr_categories"
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    
    ROTATING_PROXY = os.getenv("ROTATING_PROXY")
    custom_settings={
        'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/marionnaud_fr/%(name)s_{DATE}.json": {"format": "json",}},
        'ROTATING_PROXY_LIST' : [ROTATING_PROXY],
    }
    def start_requests(self):
        urls = "https://www.marionnaud.fr/"
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
        }
        yield scrapy.Request(urls,headers=headers,callback=self.parse)

    def parse(self,response):
        first_item ={}
        first_item["name"] = response.xpath('//div[@id="menu-main"]//a[@title="Produits"]/text()').get('')
        urls = response.xpath('//div[@id="menu-main"]//a[@title="Produits"]/@href').get('')
        first_item["url"] = response.urljoin(urls)

        blocks = response.xpath('//div[@class="nav-produit sub-nav sub-nav-active"]//li[@class="nav__list__item  auto has-sub js-enquire-has-sub"]')
        given_names = ['Parfum','Soin Visage','Soin Corps','Maquillage','Cheveux','Homme','Parapharmacie','Accessoires','Idées Cadeaux']
        outer_category_crumb =[]
        
        for block in blocks:
            item = {}
            name = block.xpath('./a/text()').get('')
            if name in given_names:
                item["name"] = name
                urls = block.xpath('./a/@href').get('')
                item["url"] = response.urljoin(urls)
                
                category_crumb_list = []
                selector_list = block.xpath('.//div//ul')
                for sel in selector_list:
                    var = {}
                    var["name"] = sel.xpath('./li[@class="yCmsComponent dropdown"]/a/text()').get('')
                    if var["name"] == '':
                        continue
                    url = sel.xpath('./li[@class="yCmsComponent dropdown"]/a/@href').get('')
                    var["url"] = response.urljoin(url)
                    
                    category_crumb_list_2 = []
                    tables = sel.xpath('./li[@class="yCmsComponent leaf-node"]')
                    for blo in tables:
                        product = {}
                        product["name"] = blo.xpath('./a/text()').get('')
                        urls = blo.xpath('./a/@href').get('')
                        product["url"] = response.urljoin(urls)
                        category_crumb_list_2.append(product)
                    
                    var['category_crumb'] = category_crumb_list_2
                    category_crumb_list.append(var)

                item['category_crumb'] = category_crumb_list
                outer_category_crumb.append(item)

        first_item['category_crumb'] = outer_category_crumb
        yield first_item
                    




