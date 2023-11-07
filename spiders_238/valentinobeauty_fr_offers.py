import scrapy
import json
import os
import re
import datetime
import urllib.parse
from pathlib import Path
from dotenv import load_dotenv

""" load env file """
try:
    load_dotenv()
except:
    env_path = Path(".env")
    load_dotenv(dotenv_path=env_path)

class ValentinobeautySpider(scrapy.Spider):
    name = 'valentinobeauty_fr_offers'
    api_token=os.getenv("api_token")
    headers = {
  'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
}
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    custom_settings={
                    'CONCURRENT_REQUESTS':2,
                    # "HTTPCACHE_ENABLED" : True,
                    # "HTTPCACHE_DIR" : 'httpcache',
                    # 'HTTPCACHE_EXPIRATION_SECS':72000,
                    # "HTTPCACHE_STORAGE" : "scrapy.extensions.httpcache.FilesystemCacheStorage",
                    'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/valentinobeauty_fr/%(name)s_{DATE}.json": {"format": "json",}},
                    }


    def start_requests(self):
        self.spider_name=self.name
        category_spider_name=(ValentinobeautySpider.name).replace("offers","categories")
        dir_path= os.getcwd()+rf"/exports/{category_spider_name}"
        with open(os.path.join(dir_path,f"{category_spider_name}_{self.CURRENT_DATE}.json"), 'r',encoding="utf-8") as f:
            contents = json.loads(f.read())
        ValentinobeautySpider.name="valentinobeauty_fr_pdp"
        for makeup in contents:
            first_name= makeup.get("name","")
            first_url=makeup.get("url",'')
            parse_url=urllib.parse.quote(first_url.strip()+"?start=0&sz=1000")
            parse_url=f"https://api.scrape.do/?token={self.api_token}&url="+parse_url
            yield scrapy.Request(parse_url,headers=self.headers, callback=self.parse,cb_kwargs={"category":[],"item":{},'count':1,"url":first_url},dont_filter=True)
            for category in makeup.get("category_crumb",[]):
                second_name= category.get("name","")
                second_url=category.get("url","")
                two_category=[{"name":first_name,"url":first_url}.copy(),{"name":second_name,"url":second_url}.copy()]
                parse_url=urllib.parse.quote(second_url.strip()+"?start=0&sz=1000")
                parse_url=f"https://api.scrape.do/?token={self.api_token}&url="+parse_url
                yield scrapy.Request(parse_url,headers=self.headers, callback=self.parse,cb_kwargs={"category":two_category,"item":{},'count':1,"url":second_url},dont_filter=True)
                for sub_category in category.get("category_crumb",[]):
                    third_name= sub_category.get("name","")
                    third_url=sub_category.get("url","")
                    if not  any(chr.isdigit() for chr in third_url):
                        category=[{"name":first_name,"url":first_url}.copy(),{"name":second_name,"url":second_url}.copy(),{"name":third_name,"url":third_url}.copy()]
                        parse_url=urllib.parse.quote(third_url.strip()+'?start=0&sz=1000')
                        parse_url=f"https://api.scrape.do/?token={self.api_token}&url="+third_url
                        yield scrapy.Request(parse_url,headers=self.headers, callback=self.parse,cb_kwargs={"category":category,"item":{},'count':1,"url":third_url},dont_filter=True)
        dir_path=os.path.abspath(__file__ + "/../../../")
        supporting_files=os.path.join(dir_path,"supporting_files")
        with open(f'{supporting_files}/valentinobeauty_offers.txt') as f:urls = f.readlines()
        for url in urls:
            parse_url=url.strip()+"&start=0&sz=1000"
            parse_url=urllib.parse.quote(parse_url.strip())
            parse_url=f"https://api.scrape.do/?token={self.api_token}&url="+parse_url
            yield scrapy.Request(parse_url,headers=self.headers,callback=self.parse,cb_kwargs={"item":{},'count':1,"url":url.strip(),"category":[]})

    async def parse(self, response,item={},count=1,url=None,category=[]):
        async def product(product_url,item,rank):
            product_response = await self.request(product_url)
            data = {}
            data['rank']=rank
            data["name"] = product_response.css(".c-product-main__name::text").get()
            if data['name'] is None:
                product_response = await self.request(product_url+"&geoCode=fr") 
            data["name"] = product_response.css(".c-product-main__name::text").get()
            data["url"] = product_url.split("url=")[-1]
            data["image_url"] = product_response.xpath('//div[@data-component="product/ProductImages"]//div[@class="c-product-detail-image__mosaic"]/div/button/img/@src|//div[@data-component="product/ProductImages"]//div[@class="c-product-detail-image__alternatives c-product-thumbs"]/section//ul/li/button/img/@src|//ul[@class="c-carousel__content m-disabled"]/li[@class="c-carousel__item m-visible m-active"]/img/@src').getall()
            video_url=product_response.css('.c-video-library__main-video [itemprop="contentURL"]::attr(content)').getall()
            if video_url!=[]:
                data["has_video"]=True
                data["video"]=video_url
            else:
                data["has_video"]=False
                data["video"]=[]
            data["name"] = product_response.css(".c-product-main__name::text").get()
            data["subtitle"] = product_response.xpath('//span[@class="c-product-main__subtitle"]//text()').get('').strip()
            if data["subtitle"]=="":
                data["subtitle"]=None
            data["brand"]="Valentino"
            data["master_product_id"] = product_response.xpath('//div[@class="c-product-main__rating"]//div/@data-bv-productid').get()
            data['gtin']=None
            data_id=product_response.xpath("//main[@class='l-pdp__main']/@data-pid").get("").strip()
            stock_url=f"https://www.valentino-beauty.fr/on/demandware.store/Sites-valentino-emea-west-ng-Site/fr_FR/CDSLazyload-product_availability?configid=&data={data_id}&id=availability&section=product&ajax=true"
            stock_url=urllib.parse.quote(stock_url)
            stock_url=f"https://api.scrape.do/?token={self.api_token}&url={stock_url}"
            stock_response = await self.request(stock_url)  
            if 'En rupture de stock' in stock_response.text:
                data['in-stock'] = False
            else:
                data['in-stock'] = True
            data["price"]=product_response.css(".c-product-price.c-product-main__price .c-product-price__value[data-js-saleprice]::text").get()
            data["price_before"] = response.css("span.c-product-price__value.m-old ::text").get('')
            promo = "".join(product_response.xpath('//div[@class="c-product-main__info"]/span/*[@class="Discount"]//text()').getall())
            if promo:
                data["promo_label"] = promo
            else:
                data["promo_label"] = None
            data =self.item_clean(data)
            item['products'].append(data.copy())
        if not  bool(item):
            if "search"  not in url:
                title=url.strip("/").split("/")[-1]
            else:
                title=url.split("=")[1].strip()
            item['title']= re.sub('\s+', ' ',title )
            item['page_url']=url
            item['count']=0
            if category:
                item['category_crumb']=category
            item['products']=[]
        product_count=len(response.css(".l-plp__content .c-product-tile .c-product-tile__thumbnail,.c-product-tile__thumbnail"))
        item['count']=item['count']+product_count
        blocks =  response.css(".l-plp__content .c-product-tile .c-product-tile__thumbnail,.c-product-tile__thumbnail")
        for count,block in enumerate(blocks,1):
            rank=count
            url =  "https://www.valentino-beauty.fr"+block.css(".c-product-image::attr(href)").get('').strip()
            product_url=f"https://api.scrape.do/?token={self.api_token}&url="+url
            await product(product_url,item,rank)
        yield item


    async def request(self,url):
        """ scrapy request"""
        request = scrapy.Request(url,dont_filter=True)
        response = await self.crawler.engine.download(request,self)
        return response
    
    def item_clean(self,item):
        for key, value in item.items():
            if isinstance(value, str):
                item[key]=value.strip()
                if value=="":
                    item[key]=None
        return item