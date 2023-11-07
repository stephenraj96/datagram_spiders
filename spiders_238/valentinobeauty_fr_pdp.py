import re
import os
import json
import scrapy
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
    name = 'valentinobeauty_fr_pdp'

    """ Get token in env file"""
    api_token=os.getenv("api_token")

    review_headers = {
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
        }
    headers = {
  'authority': 'www.valentino-beauty.fr',
  'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
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
    
    def clean(self,text):
        '''remove extra spaces & junk character'''
        if text==None:
            text=""
        text = re.sub(r'\n+',' ',text)
        text = re.sub(r'\s+',' ',text)
        text = re.sub(r'\r+',' ',text)
        return text.strip()
    
    def start_requests(self):
        self.spider_name=self.name
        category_spider_name=(ValentinobeautySpider.name).replace("pdp","categories")
        dir_path= os.getcwd()+rf"/exports/{category_spider_name}"
        with open(os.path.join(dir_path,f"{category_spider_name}_{self.CURRENT_DATE}.json"), 'r',encoding="utf-8") as f:
            contents = json.loads(f.read())
        for makeup in contents:
            first_name= makeup.get("name","")
            first_url=makeup.get("url",'')
            for category in makeup.get("category_crumb",[]):
                second_name= category.get("name","")
                second_url=category.get("url","")
                for sub_category in category.get("category_crumb",[]):
                    third_name= sub_category.get("name","")
                    third_url=sub_category.get("url","")
                    category_crumb=[{"name":first_name,"url":first_url}.copy(),{"name":second_name,"url":second_url}.copy(),{"name":third_name,"url":third_url}.copy()]
                    if not  any(chr.isdigit() for chr in third_url):
                        third_url=urllib.parse.quote(third_url.strip()+'?start=0&sz=1000')
                        third_url=f"https://api.scrape.do/?token={self.api_token}&url={third_url}&customHeaders=true"
                        yield scrapy.Request(third_url,headers=self.headers, callback=self.parse_listing,cb_kwargs={"category_crumb":category_crumb})
                    else:
                        third_url=f"https://api.scrape.do/?token={self.api_token}&url={third_url}&customHeaders=true"
                        yield scrapy.Request(third_url,headers=self.headers, callback=self.parse_product,cb_kwargs={"category_crumb":category_crumb})

    
    def parse_listing(self, response, category_crumb):
        """ get product url from categories page"""
        if response.css(".l-plp__content .c-product-tile .c-product-tile__thumbnail"):
            for block in response.css(".l-plp__content .c-product-tile .c-product-tile__thumbnail"):
                url =  "https://www.valentino-beauty.fr"+block.css(".c-product-image::attr(href)").get()
                url=f"https://api.scrape.do/?token={self.api_token}&url={url}&customHeaders=true"
                yield scrapy.Request(url,callback=self.parse_product,cb_kwargs={"category_crumb":category_crumb},dont_filter=True)
        else:
            url=(urllib.parse.unquote(response.url.split("url=")[-1])).split("?")[0]
            url=f"https://api.scrape.do/?token={self.api_token}&url={url}&customHeaders=true"
            yield scrapy.Request(url,callback=self.parse_product,cb_kwargs={"category_crumb":category_crumb},dont_filter=True)

    async def parse_product(self,response,category_crumb):
        """ parse product page"""
        item = {}
        item["url"] = response.url.split("url=")[-1].replace("&customHeaders=true",'')
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

        item["brand"] = response.xpath('//meta[@property="product:brand"]/@content').get('').strip()
        if item["brand"]=="":
            item["brand"]="Valentino"
        item["master_product_id"] = response.xpath('//div[@class="c-product-main__rating"]//div/@data-bv-productid').get()
        item['gtin']= None
        data_id=response.xpath("//main[@class='l-pdp__main']/@data-pid").get("").strip()
        stock_url=f"https://www.valentino-beauty.fr/on/demandware.store/Sites-valentino-emea-west-ng-Site/fr_FR/CDSLazyload-product_availability?configid=&data={data_id}&id=availability&section=product&ajax=true"
        stock_url=urllib.parse.quote(stock_url)
        stock_url=f"https://api.scrape.do/?token={self.api_token}&url={stock_url}&customHeaders=true"
        stock_response = await self.request(stock_url)  
        if 'En rupture de stock' in stock_response.text:
            item['in-stock'] = False
        else:
            item['in-stock'] = True
        item["price"]=response.css(".c-product-price.c-product-main__price .c-product-price__value[data-js-saleprice]::text").get('')
        item["price_before"] = response.css("span.c-product-price__value.m-old ::text").get('')
        promo = "".join(response.xpath('//div[@class="c-product-main__info"]/span/*[@class="Discount"]//text()').getall())
        if promo:
            item["promo_label"] = promo
        else:
            item["promo_label"] = None  
        item["prices"] = []    
        item["category_crumb"] = category_crumb
        item["description"] = " ".join([self.clean(' '.join(e.strip() for e in response.xpath('//div[@data-tab-hash="description"]//span[@class="subsection_title"]//text()|//div[@data-tab-hash="description"]//p//text()|//div[@data-tab-hash="description-benefits"]//text()|//div[@class="c-content-tile__section c-content-tile__content"]/text()').getall()))]).strip()
        if item["description"]=="":
            item["description"]=None
        item["reviews"]=[]
        if response.xpath('//div[@class="c-variation-section__content m-size"]//section//ul//li/a[@class="c-variations-carousel__link m-selected"]/span/text()|//div[@class="c-variation-section__content m-size"]//section//ul/li/a["c-variations-carousel__link m-selected"]|//div[@class="c-variation-section c-product-sticky-bar__item m-variations"]//li[@class="c-carousel__item "]'):
            for block in response.xpath('//div[@class="c-variation-section__content m-size"]//section//ul//li/a[@class="c-variations-carousel__link m-selected"]/span/text()|//div[@class="c-variation-section__content m-size"]//section//ul/li/a["c-variations-carousel__link m-selected"]|//div[@class="c-variation-section c-product-sticky-bar__item m-variations"]//li[@class="c-carousel__item "]/a'):
                variant_url=block.xpath(".//@href").get()
                item1={}
                if variant_url!=None:
                    variant_url=urllib.parse.quote(variant_url)
                    variant_url=f"https://api.scrape.do/?token={self.api_token}&url={variant_url}&customHeaders=true"
                    response_color = await self.request(variant_url)   
                    block_color = response_color.xpath('//div[@class="c-product-main m-v2"]/@data-analytics').get('').strip()
                    try:
                        if block_color !="":
                            json_color= json.loads(block_color)
                        else:
                            block_color = response_color.xpath('//div[@class="c-product-main m-v1"]/@data-analytics').get('').strip()
                            json_color= json.loads(block_color)
                    except:
                        pass
                    item1["sku_id"] = json_color["products"][0]["pid"]
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
                        if '.' in price :            
                            item1["price"] =  price +' €'
                        else:
                            item1["price"] =   price+',00' +' €'
                    else:
                        item['price'] = None
                    price_before = str(json_color["products"][0]["regularPrice"])
                    if re.search(r'[0-9]+',price_before):
                        if '.' in price_before:            
                            item1["price_before"] =   price_before +' €'
                        else:
                            item1["price_before"] =   price_before+',00'+' €'
                    else:
                        item1["price_before"] = None
                    product_promo = "".join(response_color.xpath('//div[@class="c-product-main__info"]/span/*[@class="Discount"]//text()').getall())
                    if product_promo:
                        item1["promo_label"] = product_promo 
                    else:
                        item1["promo_label"] = None
                    item1["data_size"] = response_color.xpath('//div[@class="c-variation-section__content m-size"]//section//ul//li/a[@class="c-variations-carousel__link m-selected"]/span/text()|//div[@class="c-variation-section__content m-size"]//section//ul//li/a[@class="c-variations-carousel__link m-selected m-disabled"]/span/text()').get('').strip()
                    if item1["data_size"]=="":
                        item1["data_size"]=None
                    item["prices"].append(item1.copy())
        product_id=item['master_product_id']
        review_url=f"https://api.bazaarvoice.com/data/batch.json?passkey=ca9RqGEVCuUwrv2Scm8SZdYmJ54cdO78rEeNQ6pBgWzTc&apiversion=5.5&displaycode=19777-fr_fr&resource.q0=reviews&filter.q0=isratingsonly%3Aeq%3Afalse&filter.q0=productid%3Aeq%3A{product_id}&filter.q0=contentlocale%3Aeq%3Afr*%2Cen_US%2Cfr_FR&sort.q0=rating%3Adesc&stats.q0=reviews&filteredstats.q0=reviews&include.q0=authors%2Cproducts%2Ccomments&filter_reviews.q0=contentlocale%3Aeq%3Afr*%2Cen_US%2Cfr_FR&filter_reviewcomments.q0=contentlocale%3Aeq%3Afr*%2Cen_US%2Cfr_FR&filter_comments.q0=contentlocale%3Aeq%3Afr*%2Cen_US%2Cfr_FR&limit.q0=100&offset.q0=0&limit_comments.q0=3&callback=bv_351_36933"
        yield response.follow(review_url,headers=self.review_headers, callback=self.reviews,cb_kwargs={"item":item,"offset":0,"product_id":product_id},dont_filter=True)

    def reviews(self,response,item,offset,product_id):
        json_data=json.loads(response.text.replace('bv_351_36933(',"")[:-1])
        results=json_data.get("BatchedResults",{})
        total_count=results.get("q0",{}).get('TotalResults','')
        for row in results.get("q0",{}).get("Results",[]):
            data={}
            title=row.get("Title","")
            review_text=row.get("ReviewText","").strip()
            review_text=re.sub('\s+', ' ',review_text )
            data['review']=f'[{title}][{review_text}]'
            data['user']=row.get("UserLocation","")
            data['stars']=row.get("Rating",0)
            data['date']=row.get("LastModificationTime","").strip().split("T")[0]
            item['reviews'].append(data.copy())
        offset=offset+100
        if offset<total_count:
            review_url=f"https://api.bazaarvoice.com/data/batch.json?passkey=ca9RqGEVCuUwrv2Scm8SZdYmJ54cdO78rEeNQ6pBgWzTc&apiversion=5.5&displaycode=19777-fr_fr&resource.q0=reviews&filter.q0=isratingsonly%3Aeq%3Afalse&filter.q0=productid%3Aeq%3A{product_id}&filter.q0=contentlocale%3Aeq%3Afr*%2Cen_US%2Cfr_FR&sort.q0=rating%3Adesc&stats.q0=reviews&filteredstats.q0=reviews&include.q0=authors%2Cproducts%2Ccomments&filter_reviews.q0=contentlocale%3Aeq%3Afr*%2Cen_US%2Cfr_FR&filter_reviewcomments.q0=contentlocale%3Aeq%3Afr*%2Cen_US%2Cfr_FR&filter_comments.q0=contentlocale%3Aeq%3Afr*%2Cen_US%2Cfr_FR&limit.q0=100&offset.q0={offset}&limit_comments.q0=3&callback=bv_351_36933"
            yield response.follow(review_url,headers=self.review_headers, callback=self.reviews,cb_kwargs={"item":item,"offset":offset,"product_id":product_id},dont_filter=True)
        else:
            item =self.item_clean(item)
            if item['name']:
                yield item

    async def request(self,url):
        """ scrapy request"""
        request = scrapy.Request(url,headers=self.headers,dont_filter=True)
        response = await self.crawler.engine.download(request,self)
        return response
    
    def item_clean(self,item):
        for key, value in item.items():
            if isinstance(value, str):
                item[key]=value.strip()
                if value=="":
                    item[key]=None
        return item