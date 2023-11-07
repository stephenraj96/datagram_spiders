import re
import json
import scrapy
import os
import urllib.parse
import pandas as pd
import sys
from dotenv import load_dotenv
from pathlib import Path
import datetime

""" load env file """
try:
    load_dotenv()
except:
    env_path = Path(".env")
    load_dotenv(dotenv_path=env_path)

class YslukpdpSpider(scrapy.Spider):
    name = 'ysl_uk_pdp'

    """ Get token in env file"""
    api_token=os.getenv("api_token")

    """ save file in s3"""
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    custom_settings={
        'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/ysk_uk/%(name)s_{DATE}.json": {"format": "json",}
        },
        "CONCURRENT_REQUESTS":1
    }

    def clean(self,text):
        '''remove extra spaces & junk character'''
        if text==None:
            text=""
        text = re.sub(r'\n+',' ',text)
        text = re.sub(r'\s+',' ',text)
        text = re.sub(r'\r+',' ',text)
        return text.strip()
    
    def start_requests(self):
        """ read categories json file"""
        category_spider_name=(YslukpdpSpider.name).replace("pdp","categories")
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        dir_path= os.getcwd()+f"/exports/{category_spider_name}/{category_spider_name}_{current_date}.json"
        with open(os.path.join(dir_path), 'r',encoding="utf-8") as f:
            contents = json.loads(f.read())
    
        category_crumb_dict={}
        category_crumb_list=[]
        for content in contents:
            category_crumb_dict["name"]=content.get("name","")
            category_crumb_dict["url"]=content.get("url","")
            category_crumb_list.append(category_crumb_dict)
            category_crumb_dict={}

            for category in content.get("category_crumb",[]):
                category_crumb_dict["name"]= category.get("name","")
                category_crumb_dict["url"]=category.get("url","")
                category_crumb_list.append(category_crumb_dict)
                category_crumb_dict={}

                if category.get("category_crumb",[]):
                    for sub_category in category.get("category_crumb",[]):
                        category_crumb_list.append(sub_category)
                        url=(sub_category.get("url",""))+"?start=0&sz=100"
                        url=urllib.parse.quote(url)
                        url=f"https://api.scrape.do/?token={self.api_token}&url="+url
                        yield scrapy.Request(url, callback=self.parse_listing,cb_kwargs={"category_crumb":category_crumb_list})
                        category_crumb_list=category_crumb_list[:-1]
                category_crumb_list=category_crumb_list[:-1]
            category_crumb_list=category_crumb_list[:-1] 

    def parse_listing(self, response,category_crumb):
        """ get product url from categories page"""
        if response.css(".l-plp__content .c-product-tile .c-product-tile__thumbnail"):
            for block in response.css(".l-plp__content .c-product-tile .c-product-tile__thumbnail"):
                url = "https://www.yslbeauty.co.uk"+block.css(".c-product-image::attr(href)").get()
                url=f"https://api.scrape.do/?token={self.api_token}&url="+url
                yield scrapy.Request(
                    url,
                    callback=self.parse_product,
                    cb_kwargs={"category_crumb":category_crumb},
                    dont_filter=True
                )
        else:
            url=(urllib.parse.unquote(response.url.split("url=")[-1])).split("?")[0]
            url=f"https://api.scrape.do/?token={self.api_token}&url="+url
            yield scrapy.Request(
                url,
                callback=self.parse_product,
                cb_kwargs={"category_crumb":category_crumb},
                dont_filter=True
            )
           
    async def parse_product(self,response,category_crumb):
        """ parse product page"""

        if "discontinued" not in response.css("p.c-product-main__recommendation-message::text").get(""):    
            if response.url.split("url=")[-1] in "https://www.yslbeauty.co.uk/le-vestiaire-des-parfums.html":
                pass
            elif response.css('[data-component-context="disabled"]'):
                item = {}
                item["url"] = response.url.split("url=")[-1]
                item["image_url"] = response.xpath('//div[@data-component="product/ProductImages"]//div[@class="c-product-detail-image__mosaic"]/div/button/img/@src|//div[@data-component="product/ProductImages"]//div[@class="c-product-detail-image__alternatives c-product-thumbs"]/section//ul/li/button/img/@src|//ul[@class="c-carousel__content m-disabled"]/li[@class="c-carousel__item m-visible m-active"]/img/@src').getall()
                video_url=response.css('.c-video-library__main-video [itemprop="contentURL"]::attr(content)').getall()
                if video_url!=[]:
                    item["has_video"]=True
                    item["video"]=video_url
                else:
                    item["has_video"]=False
                    item["video"]=[]
                
                item["name"] = response.css(".c-product-main__name::text").get()
                item["subtitle"] = response.xpath('//span[@class="c-product-main__subtitle"]//text()').get('').strip()
                if item["subtitle"]=="":
                    item["subtitle"]=None

                description = [self.clean(' '.join(e.strip() for e in response.xpath('//div[@data-tab-hash="description"]//span[@class="subsection_title"]//text()|//div[@data-tab-hash="description"]//p//text()|//div[@data-tab-hash="description-benefits"]//text()').getall()))]
                if description==[""]:
                    item["description"]=""
                else:
                    item["description"]="".join(description)
                item["brand"] = response.xpath('//meta[@property="product:brand"]/@content').get('').strip()
                
                if item["brand"]=="":
                    item["brand"]="YSL"
                
                item["master_product_id"] = response.xpath('//div[@class="c-product-main__rating"]//div/@data-bv-productid').get()
                item['in-stock'] = True
                item["price"]=response.css(".c-product-price.c-product-main__price .c-product-price__value[data-js-saleprice]::text").get()
                item["price_before"] = None
                item["promo_label"] = None  
                item["prices"] = []    
                item["category_crumb"] = category_crumb
                item["reviews"]=[]
                yield item
            else:
                item = {}
                block = response.xpath('//div[@class="c-product-main m-v2"]/@data-analytics').get('').strip()
                if block !="":
                    json_data= json.loads(block)
                else:
                    block = response.xpath('//div[@class="c-product-main m-v1"]/@data-analytics').get('').strip()
                    json_data= json.loads(block)
                json_data = json.loads(block)
                item["url"] = json_data.get("products",'')[0].get("url",'')
                
                image_url = response.xpath('//div[@data-component="product/ProductImages"]//div[@class="c-product-detail-image__mosaic"]/div/button/img/@src|//div[@data-component="product/ProductImages"]//div[@class="c-product-detail-image__alternatives c-product-thumbs"]/section//ul/li/button/img/@src|//ul[@class="c-carousel__content m-disabled"]/li[@class="c-carousel__item m-visible m-active"]/img/@src').getall()

                if image_url !=[]:
                    item["image_url"] = image_url
                else:
                    item["image_url"] = [json_data["products"][0]["imgUrl"]]
                
                video_url=response.css('.c-video-library__main-video [itemprop="contentURL"]::attr(content)').getall()
                if video_url!=[]:
                    item["has_video"]=True
                    item["video"]=video_url
                else:
                    item["has_video"]=False
                    item["video"]=[]

                item["name"] = json_data["products"][0]["name"]
                item["subtitle"] = response.xpath('//span[@class="c-product-main__subtitle"]//text()').get('').strip()
                if item["subtitle"]=="":
                    item["subtitle"]=None

                description= [self.clean(' '.join(e.strip() for e in response.xpath('//div[@data-tab-hash="description"]//span[@class="subsection_title"]//text()|//div[@data-tab-hash="description"]//p//text()|//div[@data-tab-hash="description-benefits"]//text()|//div[@data-tab-hash="description"]//text()').getall()))]
                if description==[""]:
                    item["description"]=""
                else:
                    item["description"]="".join(description)

                item["brand"] = response.xpath('//meta[@property="product:brand"]/@content').get('').strip()
                if item["brand"]=="":
                    item["brand"]="YSL"

                item["master_product_id"] = json_data["products"][0]["id"]
        
                in_stock = json_data["products"][0]["stock"]
                if in_stock =='in stock':
                    item['in-stock'] = True
                else:
                    item['in-stock'] = False

                price = str(json_data["products"][0]["salePrice"])
                if re.search(r'[0-9]+',price):
                    if '.' in price:       
                        if len(price.split(".")[-1])==2:
                            item["price"] = '£'+ price 
                        else:
                            item["price"] = '£'+ price +"0"
                    else:
                        item["price"] =  '£'+ price+'.00'
                else:
                    item['price'] = None

                price_before = str(json_data.get("products",'')[0].get("regularPrice",''))
                if re.search(r'[0-9]+',price_before):
                    if '.' in price_before:     
                        if len(price_before.split(".")[-1])==2:
                            item["price_before"] = '£'+ price_before 
                        else:
                            item["price_before"] = '£'+ price_before +"0"       
                    else:
                        item["price_before"] = '£'+ price_before+'.00'
                else:
                    item["price_before"] = None
                
                item["promo_label"] = None         
                prices = []
                if response.xpath('//div[@class="c-swatches m-active m-pdp"]//li'):
                    for block in response.xpath('//div[@class="c-swatches m-active m-pdp"]//li'):
                        item1 = {}
                        variant_url = block.xpath('.//a/@href').get('').strip()
                        variant_url=f"https://api.scrape.do/?token={self.api_token}&url="+variant_url
                        response_color = await self.request(variant_url) 
                        block_color = response_color.xpath('//div[@class="c-product-main m-v2"]/@data-analytics').get('').strip()
                        if block_color !="":
                            json_color= json.loads(block_color)
                        else:
                            block_color = response_color.xpath('//div[@class="c-product-main m-v1"]/@data-analytics').get('').strip()
                            json_color= json.loads(block_color)

                        item1["id"] = json_color["products"][0]["pid"]
                        item1['variant_url'] = json_color["products"][0]["url"]   

                        variant_image = response_color.xpath('//div[@data-component="product/ProductImages"]//div[@class="c-product-detail-image__mosaic"]/div/button/img/@src|//div[@data-component="product/ProductImages"]//div[@class="c-product-detail-image__alternatives c-product-thumbs"]/section//ul/li/button/img/@src|//ul[@class="c-carousel__content m-disabled"]/li[@class="c-carousel__item m-visible m-active"]/img/@src').getall()
                        if variant_image !=[]:
                            item1["image_url"] = variant_image
                        else:
                            item1["image_url"] = [json_color["products"][0]["imgUrl"]]

                        item1["data_color"] = json_color["products"][0]["color"]
                        if item1["data_color"]=="":
                            item1["data_color"]=None

                        in_stock = json_color["products"][0]["stock"]
                        if in_stock =='in stock':
                            item1['in-stock'] = True
                        else:
                            item1['in-stock'] = False

                        price = str(json_color["products"][0]["salePrice"])
                        if re.search(r'[0-9]+',price):
                            if '.' in price:            
                                if len(price.split(".")[-1])==2:
                                    item1["price"] = '£'+ price 
                                else:
                                    item1["price"] = '£'+ price +"0"
                            else:
                                item1["price"] =  '£'+ price+'.00'
                        else:
                            item1['price'] = None

                        
                        price_before = str(json_color["products"][0]["regularPrice"])
                        if re.search(r'[0-9]+',price_before):
                            if '.' in price_before:            
                                if len(price_before.split(".")[-1])==2:
                                    item1["price_before"] = '£'+ price_before 
                                else:
                                    item1["price_before"] = '£'+ price_before +"0"   
                            else:
                                item1["price_before"] =  '£'+ price_before+'.00'
                        else:
                            item1["price_before"] = None

                        item1["promo_label"] = None 
                    
                        item1["data_size"] = response_color.xpath('//div[@class="c-variation-section__content m-size"]//section//ul//li/a[@class="c-variations-carousel__link m-selected"]/span/text()|//div[@class="c-variation-section__content m-size"]//section//ul//li/a[@class="c-variations-carousel__link m-selected m-disabled"]/span/text()').get('').strip()
                        if item1["data_size"]=="":
                            item1["data_size"]=None
                        prices.append(item1)
                        item1 = {}
                    
                elif response.xpath('//div[@class="c-variation-section__content m-size"]//section//ul//li/a[@class="c-variations-carousel__link m-selected"]/span/text()|//div[@class="c-variation-section__content m-size"]//section//ul//li/a[@class="c-variations-carousel__link m-selected m-disabled"]/span/text()') or json_data["products"][0]["color"]:
                    item1 = {}
                    if response.xpath('//div[@class="c-variation-section__content m-size"]//section//ul//li/a[@class="c-variations-carousel__link m-selected"]/span/text()|//div[@class="c-variation-section__content m-size"]//section//ul/li/a["c-variations-carousel__link m-selected"]|//div[@class="c-variation-section c-product-sticky-bar__item m-variations"]//li[@class="c-carousel__item "]'):
                        for block in response.xpath('//div[@class="c-variation-section__content m-size"]//section//ul//li/a[@class="c-variations-carousel__link m-selected"]/span/text()|//div[@class="c-variation-section__content m-size"]//section//ul/li/a["c-variations-carousel__link m-selected"]|//div[@class="c-variation-section c-product-sticky-bar__item m-variations"]//li[@class="c-carousel__item "]/a'):
                            variant_url=block.xpath(".//@href").get()
                            if variant_url!=None:
                                variant_url=f"https://api.scrape.do/?token={self.api_token}&url="+variant_url
                                response_color = await self.request(variant_url)   
                                block_color = response_color.xpath('//div[@class="c-product-main m-v2"]/@data-analytics').get('').strip()
                            
                                if block_color !="":
                                    json_color= json.loads(block_color)
                                else:
                                    block_color = response_color.xpath('//div[@class="c-product-main m-v1"]/@data-analytics').get('').strip()
                                    json_color= json.loads(block_color)
                                
                                item1["id"] = json_color["products"][0]["pid"]
                                item1['variant_url'] = json_color["products"][0]["url"]
                                
                                variant_image = response_color.xpath('//div[@data-component="product/ProductImages"]//div[@class="c-product-detail-image__mosaic"]/div/button/img/@src|//div[@data-component="product/ProductImages"]//div[@class="c-product-detail-image__alternatives c-product-thumbs"]/section//ul/li/button/img/@src|//ul[@class="c-carousel__content m-disabled"]/li[@class="c-carousel__item m-visible m-active"]/img/@src').getall()
                                if variant_image !=[]:
                                    item1["image_url"] = variant_image
                                else:
                                    item1["image_url"] = [json_color["products"][0]["imgUrl"]]
                                
                                item1["data_color"] = json_color["products"][0]["color"]
                                if item1["data_color"]=="":
                                    item1["data_color"]=None

                                in_stock = json_color["products"][0]["stock"]
                                if in_stock =='in stock':
                                    item1['in-stock'] = True
                                else:
                                    item1['in-stock'] = False
                                
                                price = str(json_color["products"][0]["salePrice"])
                                if re.search(r'[0-9]+',price):
                                    if '.' in price:            
                                        if len(price.split(".")[-1])==2:
                                            item1["price"] = '£'+ price 
                                        else:
                                            item1["price"] = '£'+ price +"0"
                                    else:
                                        item1["price"] =  '£'+ price+'.00'
                                else:
                                    item1['price'] = None

                                price_before = str(json_color["products"][0]["regularPrice"])
                                if re.search(r'[0-9]+',price_before):
                                    if '.' in price_before:            
                                        if len(price_before.split(".")[-1])==2:
                                            item1["price_before"] = '£'+ price_before 
                                        else:
                                            item1["price_before"] = '£'+ price_before +"0"   
                                    else:
                                        item1["price_before"] =  '£'+ price_before+'.00'
                                else:
                                    item1["price_before"] = None

                                item1["promo_label"] = None 
                            
                                item1["data_size"] = response_color.xpath('//div[@class="c-variation-section__content m-size"]//section//ul//li/a[@class="c-variations-carousel__link m-selected"]/span/text()|//div[@class="c-variation-section__content m-size"]//section//ul//li/a[@class="c-variations-carousel__link m-selected m-disabled"]/span/text()').get('').strip()
                                if item1["data_size"]=="":
                                    item1["data_size"]=None
                                prices.append(item1)
                                item1 = {}
                
                    else:
                        item1['id'] = json_data["products"][0]["id"]
                        item1['variant_url'] = response.url.split("url=")[-1]
                        item1['image_url'] = item['image_url']
                        item1["data_color"] = json_data["products"][0]["color"]
                        if item1["data_color"]=="":
                            item1["data_color"]=None
                        in_stock = json_data["products"][0]["stock"]
                        if in_stock =='in stock':
                            item1['in-stock'] = True
                        else:
                            item1['in-stock'] = False

                        item1["price"]=response.css(".c-product-price.c-product-main__price .c-product-price__value[data-js-saleprice]::text").get()
                        
                        price_before = str(json_data["products"][0]["regularPrice"])
                        if price_before != 'None':
                            item1["price_before"] = '£'+price_before
                        else:
                            item1["price_before"] = item['price_before']
                        item1["promo_label"] = None
                    
                        item1["data_size"] = response.xpath('//div[@class="c-variation-section__content m-size"]//section//ul//li/a[@class="c-variations-carousel__link m-selected"]/span/text()|//div[@class="c-variation-section__content m-size"]//section//ul//li/a[@class="c-variations-carousel__link m-selected m-disabled"]/span/text()').get('').strip()
                        if item1["data_size"]=="":
                            item1["data_size"]=None
                        
                        prices.append(item1)
                        item1 = {}
                if prices !=[]:
                    item["prices"] = prices
                else:
                    item["prices"] = []     
                item["category_crumb"] = category_crumb
                
                #review
                review_list=[]
                review_url=f'https://api.bazaarvoice.com/data/reviews.json?resource=reviews&action=REVIEWS_N_STATS&filter=productid%3Aeq%3A{item["master_product_id"]}&filter=contentlocale%3Aeq%3Aen_GB%2Cen_GB&filter=isratingsonly%3Aeq%3Afalse&filter_reviews=contentlocale%3Aeq%3Aen_GB%2Cen_GB&include=authors%2Cproducts&filteredstats=reviews&Stats=Reviews&limit=8&offset=0&sort=relevancy%3Aa1&passkey=tgrj6fashsljqm38we4tct2iu&apiversion=5.5&displaycode=19904-en_gb'
                review_response=await self.request(review_url)
                total_count=json.loads(review_response.text).get('TotalResults','')
                
                if total_count>0:
                    initial_count=0
                    count=8
                    loop_count=round(total_count/30)
                    if total_count<45:
                        review_url=f'https://api.bazaarvoice.com/data/reviews.json?resource=reviews&action=REVIEWS_N_STATS&filter=productid%3Aeq%3A{item["master_product_id"]}&filter=contentlocale%3Aeq%3Aen_GB%2Cen_GB&filter=isratingsonly%3Aeq%3Afalse&filter_reviews=contentlocale%3Aeq%3Aen_GB%2Cen_GB&include=authors%2Cproducts&filteredstats=reviews&Stats=Reviews&limit={total_count}&offset=0&sort=relevancy%3Aa1&passkey=tgrj6fashsljqm38we4tct2iu&apiversion=5.5&displaycode=19904-en_gb'
                        review_response=await self.request(review_url)
                        review_json=json.loads(review_response.text).get("Results","")
                        review_dict={}
                        for review_json1 in review_json:
                            review_dict["review"]=review_json1.get("ReviewText","")
                            if review_dict["review"]=="":
                                review_dict["review"]=None
                            
                            review_dict["stars"]=review_json1.get("Rating","")
                            if review_dict["stars"]=="":
                                review_dict["stars"]=None
                            
                            review_dict["user"]=review_json1.get("UserNickname","")
                            if review_dict["user"]=="":
                                    review_dict["user"]=None

                            date=review_json1.get("SubmissionTime","")
                            if date!="":
                                review_dict["date"]=pd.to_datetime(date).strftime('%m/%d/%Y')
                            else:
                                review_dict["date"]=None
                            
                            review_list.append(review_dict)
                            review_dict={}
                    else:
                        for i in range(1,loop_count+2):
                            if initial_count==0:
                                review_url=f'https://api.bazaarvoice.com/data/reviews.json?resource=reviews&action=REVIEWS_N_STATS&filter=productid%3Aeq%3A{item["master_product_id"]}&filter=contentlocale%3Aeq%3Aen_GB%2Cen_GB&filter=isratingsonly%3Aeq%3Afalse&filter_reviews=contentlocale%3Aeq%3Aen_GB%2Cen_GB&include=authors%2Cproducts&filteredstats=reviews&Stats=Reviews&limit=8&offset=0&sort=relevancy%3Aa1&passkey=tgrj6fashsljqm38we4tct2iu&apiversion=5.5&displaycode=19904-en_gb'
                                initial_count=initial_count+1
                            else:
                                review_url=f'https://api.bazaarvoice.com/data/reviews.json?resource=reviews&action=REVIEWS_N_STATS&filter=productid%3Aeq%3A{item["master_product_id"]}&filter=contentlocale%3Aeq%3Aen_GB%2Cen_GB&filter=isratingsonly%3Aeq%3Afalse&filter_reviews=contentlocale%3Aeq%3Aen_GB%2Cen_GB&include=authors%2Cproducts&filteredstats=reviews&Stats=Reviews&limit=30&offset={count}&sort=relevancy%3Aa1&passkey=tgrj6fashsljqm38we4tct2iu&apiversion=5.5&displaycode=19904-en_gb'
                                count=count+30
                            review_response=await self.request(review_url)
                            review_json=json.loads(review_response.text).get("Results","")
                            review_dict={}
                            for review_json1 in review_json:
                                review_dict["review"]=self.clean(review_json1.get("ReviewText",""))
                                if review_dict["review"]=="":
                                    review_dict["review"]=None

                                review_dict["stars"]=review_json1.get("Rating","")
                                if review_dict["stars"]=="":
                                    review_dict["stars"]=None

                                review_dict["user"]=review_json1.get("UserNickname","")
                                if review_dict["user"]=="":
                                    review_dict["user"]=None
                                    
                                date=review_json1.get("SubmissionTime","")
                                if date!="":
                                    review_dict["date"]=pd.to_datetime(date).strftime('%m/%d/%Y')
                                else:
                                    review_dict["date"]=None
                                
                                review_list.append(review_dict)
                                review_dict={}
                
                item["reviews"]=review_list
                
                yield item
          
    async def request(self,url):
        """ scrapy request"""
        request = scrapy.Request(url)
        response = await self.crawler.engine.download(request,self)
        return response