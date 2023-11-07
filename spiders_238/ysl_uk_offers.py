import re
import json
import scrapy
import os
import urllib.parse
from dotenv import load_dotenv
from pathlib import Path
import sys
import datetime

""" load env file """
try:
    load_dotenv()
except:
    env_path = Path(".env")
    load_dotenv(dotenv_path=env_path)

class YslukoffersSpider(scrapy.Spider):
    name = 'ysl_uk_offers'

    """ Get token in env file"""
    api_token=os.getenv("api_token")

    """ save file in s3"""
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    custom_settings={
        'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/ysk_uk/%(name)s_{DATE}.json": {"format": "json",}},
        "CONCURRENT_REQUESTS":1
    }

    def start_requests(self):
        """ read ysl url text file"""
        dir_path=os.path.abspath(__file__ + "/../../../")
        supporting_files=os.path.join(dir_path,"supporting_files")
        with open(f'{supporting_files}/ysl_uk_urls.txt') as f:
            urls=f.readlines()
        for url in urls:
            url=f"https://api.scrape.do/?token={self.api_token}&url="+url
            yield scrapy.Request(url, callback=self.parse_product)

    async def parse_product(self, response):
        """ parse product of each url"""
        item={}
        category_crumb_list=[]
        category_crumb_dict={}
        if response.css(".l-plp__breadcrumbs"):
            for block in response.css(".l-plp__breadcrumbs li"):
                name=block.css("span.c-breadcrumbs__text::text").get()
                if name!="Home":
                    category_crumb_dict["name"]=name
                    category_crumb_dict["url"]=block.css("a::attr(href)").get()
                    if category_crumb_dict["url"]==None:
                        category_crumb_dict["url"]=response.url.split("url=")[-1]
                        item["title"]=name
                        item["page_url"]=category_crumb_dict["url"]
                    category_crumb_list.append(category_crumb_dict)
                    category_crumb_dict={}
        elif response.css(".l-pdp__breadcrumbs li"):
            for block in response.css(".l-pdp__breadcrumbs li"):
                name=block.css("span.c-breadcrumbs__text::text").get()
                if name!="Home":
                    category_crumb_dict["name"]=name
                    category_crumb_dict["url"]=block.css("a::attr(href)").get()
                    if category_crumb_dict["url"]==None:
                        category_crumb_dict["url"]=response.url.split("url=")[-1]
                        item["title"]=name
                        item["page_url"]=category_crumb_dict["url"]
                    category_crumb_list.append(category_crumb_dict)
                    category_crumb_dict={}
        else:
            item["title"] = response.css('.c-search-phrase__term::text').get()
            item["page_url"] = response.url.split("url=")[-1]
        item["count"]=""
        if category_crumb_list != []:
            item["category_crumb"]=category_crumb_list
        url=response.url.split("url=")[-1]+"?start=0&sz=100"
        url=urllib.parse.quote(url)
        url=f"https://api.scrape.do/?token={self.api_token}&url="+url
        product_main_response = await self.request(url)
        
        count=0
        if bool(response.css(".l-plp__content .c-product-tile .c-product-tile__thumbnail")):
            for block in product_main_response.css(".l-plp__content .c-product-tile .c-product-tile__thumbnail"):
                url = "https://www.yslbeauty.co.uk"+block.css(".c-product-image::attr(href)").get()
                count=count+1
        elif bool(response.css(".l-search__products .c-product-tile .c-product-tile__thumbnail")):
            url=response.url.split("url=")[-1]
            url=urllib.parse.quote(url)
            url=f"https://api.scrape.do/?token={self.api_token}&url="+url
            product_main_response = await self.request(url)
            for block in product_main_response.css(".l-search__products .c-product-tile .c-product-tile__thumbnail"):
                url = "https://www.yslbeauty.co.uk"+block.css(".c-product-image::attr(href)").get()
                count=count+1
        item["count"]=count
        
        product_list=[]
        if response.css(".l-plp__content .c-product-tile .c-product-tile__thumbnail"):
            for rank,block in enumerate(product_main_response.css(".l-plp__content .c-product-tile .c-product-tile__thumbnail"),1):
                url = "https://www.yslbeauty.co.uk"+block.css(".c-product-image::attr(href)").get()
                url=f"https://api.scrape.do/?token={self.api_token}&url="+url
                product_response = await self.request(url) 
                
                if "discontinued" not in product_response.css("p.c-product-main__recommendation-message::text").get(""):
                    product_dict = {}
                    if response.url.split("url=")[-1] in "https://www.yslbeauty.co.uk/le-vestiaire-des-parfums.html":
                        pass
                    elif response.css('[data-component-context="disabled"]'):
                        product_dict["rank"]=rank
                        product_dict["url"] = response.url.split("url=")[-1]
                        product_dict["image_url"] = response.xpath('//div[@data-component="product/ProductImages"]//div[@class="c-product-detail-image__mosaic"]/div/button/img/@src|//div[@data-component="product/ProductImages"]//div[@class="c-product-detail-image__alternatives c-product-thumbs"]/section//ul/li/button/img/@src|//ul[@class="c-carousel__content m-disabled"]/li[@class="c-carousel__item m-visible m-active"]/img/@src').getall()
                        product_dict["has_video"]=False
                        product_dict["video"]=[]
                        product_dict["master_product_id"] = response.xpath('//div[@class="c-product-main__rating"]//div/@data-bv-productid').get()
                        product_dict["name"] = response.css(".c-product-main__name::text").get()
                        product_dict["brand"] = response.xpath('//meta[@property="product:brand"]/@content').get('').strip()
                        
                        if product_dict["brand"]=="":
                            product_dict["brand"]="YSL"
                        
                        product_dict['in-stock'] = True
                        
                        product_dict["price"]=response.css(".c-product-price.c-product-main__price .c-product-price__value[data-js-saleprice]::text").get()
                        product_dict["price_before"] = None
                        product_dict["promo_label"] = None  
                        product_list.append(product_dict)

                    else:
                        product_dict["rank"]=rank
                        block = product_response.xpath('//div[@class="c-product-main m-v2"]/@data-analytics').get('').strip()
                        if block !="":
                            json_data= json.loads(block)
                        else:
                            block = product_response.xpath('//div[@class="c-product-main m-v1"]/@data-analytics').get('').strip()
                            json_data= json.loads(block)
                            
                        product_dict["url"] = json_data.get("products",'')[0].get("url",'')
                        
                        image_url = product_response.xpath('//div[@data-component="product/ProductImages"]//div[@class="c-product-detail-image__mosaic"]/div/button/img/@src|//div[@data-component="product/ProductImages"]//div[@class="c-product-detail-image__alternatives c-product-thumbs"]/section//ul/li/button/img/@src|//ul[@class="c-carousel__content m-disabled"]/li[@class="c-carousel__item m-visible m-active"]/img/@src').getall()

                        if image_url !=[]:
                            product_dict["image_url"] = image_url
                        else:
                            product_dict["image_url"] = [json_data["products"][0]["imgUrl"]]
                        
                        product_dict["has_video"]=False
                        product_dict["video"]=[]
                        
                        product_dict["master_product_id"] = json_data["products"][0]["id"]
                        
                        product_dict["name"] = json_data["products"][0]["name"]
                    
                        product_dict["brand"] = product_response.xpath('//meta[@property="product:brand"]/@content').get('').strip()

                        if product_dict["brand"]=="":
                            product_dict["brand"]="YSL"
                        price = str(json_data["products"][0]["salePrice"])
                        
                        if re.search(r'[0-9]+',price):
                            if '.' in price:       
                                if len(price.split(".")[-1])==2:
                                    product_dict["price"] = '£'+ price 
                                else:
                                    product_dict["price"] = '£'+ price +"0"
                            else:
                                product_dict["price"] =  '£'+ price+'.00'
                        else:
                            product_dict['price'] = ''
                        
                        in_stock = json_data["products"][0]["stock"]
                        if in_stock =='in stock':
                            product_dict['in-stock'] = True
                        else:
                            product_dict['in-stock'] = False

                        price_before = str(json_data.get("products",'')[0].get("regularPrice",''))
                        if re.search(r'[0-9]+',price_before):
                            if '.' in price_before:     
                                if len(price_before.split(".")[-1])==2:
                                    product_dict["price_before"] = '£'+ price_before 
                                else:
                                    product_dict["price_before"] = '£'+ price_before +"0"       
                            else:
                                product_dict["price_before"] = '£'+ price_before+'.00'
                        else:
                            product_dict["price_before"] = None
                        
                        product_dict["promo_label"] = None
                        product_list.append(product_dict)
                else:
                    pass
        elif response.css(".l-search__products .c-product-tile .c-product-tile__thumbnail"):
            url=response.url.split("url=")[-1]
            url=urllib.parse.quote(url)
            url=f"https://api.scrape.do/?token={self.api_token}&url="+url
            product_main_response = await self.request(url)
            for rank,block in enumerate(product_main_response.css(".l-search__products .c-product-tile .c-product-tile__thumbnail"),1):
                url = "https://www.yslbeauty.co.uk"+block.css(".c-product-image::attr(href)").get()
                url=f"https://api.scrape.do/?token={self.api_token}&url="+url
                product_response = await self.request(url) 
                
                if "discontinued" not in product_response.css("p.c-product-main__recommendation-message::text").get(""):
                    product_dict = {}
                    if response.url.split("url=")[-1] in "https://www.yslbeauty.co.uk/le-vestiaire-des-parfums.html":
                        pass
                    elif response.css('[data-component-context="disabled"]'):
                        product_dict["rank"]=rank

                        product_dict["url"] = response.url.split("url=")[-1]
                        
                        product_dict["image_url"] = response.xpath('//div[@data-component="product/ProductImages"]//div[@class="c-product-detail-image__mosaic"]/div/button/img/@src|//div[@data-component="product/ProductImages"]//div[@class="c-product-detail-image__alternatives c-product-thumbs"]/section//ul/li/button/img/@src|//ul[@class="c-carousel__content m-disabled"]/li[@class="c-carousel__item m-visible m-active"]/img/@src').getall()
                        product_dict["has_video"]=False
                        product_dict["video"]=[]

                        product_dict["master_product_id"] = response.xpath('//div[@class="c-product-main__rating"]//div/@data-bv-productid').get()

                        product_dict["name"] = response.css(".c-product-main__name::text").get()
                        product_dict["brand"] = response.xpath('//meta[@property="product:brand"]/@content').get('').strip()
                        
                        if product_dict["brand"]=="":
                            product_dict["brand"]="YSL"
                        
                        
                        product_dict['in-stock'] = True
                        
                        product_dict["price"]=response.css(".c-product-price.c-product-main__price .c-product-price__value[data-js-saleprice]::text").get()
                        product_dict["price_before"] = None
                        product_dict["promo_label"] = None  
                        product_list.append(product_dict)

                    else:
                        product_dict["rank"]=rank
                    
                        block = product_response.xpath('//div[@class="c-product-main m-v2"]/@data-analytics').get('').strip()
                        if block !="":
                            json_data= json.loads(block)
                        else:
                            block = product_response.xpath('//div[@class="c-product-main m-v1"]/@data-analytics').get('').strip()
                            json_data= json.loads(block)
                            
                        product_dict["url"] = json_data.get("products",'')[0].get("url",'')
                        
                        image_url = product_response.xpath('//div[@data-component="product/ProductImages"]//div[@class="c-product-detail-image__mosaic"]/div/button/img/@src|//div[@data-component="product/ProductImages"]//div[@class="c-product-detail-image__alternatives c-product-thumbs"]/section//ul/li/button/img/@src|//ul[@class="c-carousel__content m-disabled"]/li[@class="c-carousel__item m-visible m-active"]/img/@src').getall()

                        if image_url !=[]:
                            product_dict["image_url"] = image_url
                        else:
                            product_dict["image_url"] = [json_data["products"][0]["imgUrl"]]
                        
                        product_dict["has_video"]=False
                        product_dict["video"]=[]
                        
                        product_dict["master_product_id"] = json_data["products"][0]["id"]
                        
                        product_dict["name"] = json_data["products"][0]["name"]
                    
                        product_dict["brand"] = product_response.xpath('//meta[@property="product:brand"]/@content').get('').strip()
                        if product_dict["brand"]=="":
                            product_dict["brand"]="YSL"
                        price = str(json_data["products"][0]["salePrice"])
                        
                        if re.search(r'[0-9]+',price):
                            if '.' in price:       
                                if len(price.split(".")[-1])==2:
                                    product_dict["price"] = '£'+ price 
                                else:
                                    product_dict["price"] = '£'+ price +"0"
                            else:
                                product_dict["price"] =  '£'+ price+'.00'
                        else:
                            product_dict['price'] = ''
                        
                        in_stock = json_data["products"][0]["stock"]
                        if in_stock =='in stock':
                            product_dict['in-stock'] = True
                        else:
                            product_dict['in-stock'] = False

                        price_before = str(json_data.get("products",'')[0].get("regularPrice",''))
                        if re.search(r'[0-9]+',price_before):
                            if '.' in price_before:     
                                if len(price_before.split(".")[-1])==2:
                                    product_dict["price_before"] = '£'+ price_before 
                                else:
                                    product_dict["price_before"] = '£'+ price_before +"0"       
                            else:
                                product_dict["price_before"] = '£'+ price_before+'.00'
                        else:
                            product_dict["price_before"] = None
                        
                        product_dict["promo_label"] = None
                        product_list.append(product_dict)
                else:
                    pass

        item["products"]=product_list
        if item.get("title","")!="":
            yield item
        
    async def request(self,url):
        """ scrapy request"""
        request = scrapy.Request(url)
        response = await self.crawler.engine.download(request,self)
        return response
    