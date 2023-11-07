import re
import os
import json
import scrapy
from pathlib import Path
from scrapy import Request
from dotenv import load_dotenv
import datetime as currentdatetime
from inline_requests import inline_requests


""" load env file """
try:
    load_dotenv()
except:
    env_path = Path(".env")
    load_dotenv(dotenv_path=env_path)

class SephoraUkOffersSpider(scrapy.Spider):
    name = 'sephora_uk_offers'
    headers = {
        'accept-encoding': 'gzip, deflate, br',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
            }
    CURRENT_DATETIME = currentdatetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    ROTATING_PROXY = os.getenv("ROTATING_PROXY")
    custom_settings={
        'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/sephora_uk/%(name)s_{DATE}.json": {"format": "json",}},
        "CONCURRENT_REQUESTS":32,
        'ROTATING_PROXY_LIST' : [ROTATING_PROXY],
        # "HTTPCACHE_ENABLED" : True,
        # "HTTPCACHE_DIR" : 'httpcache',
        # 'HTTPCACHE_EXPIRATION_SECS':172800,
        # "HTTPCACHE_IGNORE_HTTP_CODES":[502,504],
        # "HTTPCACHE_STORAGE" : "scrapy.extensions.httpcache.FilesystemCacheStorage",
        }
    
    def start_requests (self):
        self.spider_name=self.name
        SephoraUkOffersSpider.name="sephora_uk_pdp"
        dir_path=os.path.abspath(__file__ + "/../../../")
        supporting_files=os.path.join(dir_path,"supporting_files")  
        with open(f'{supporting_files}/sephora_uk_offer_input.txt') as f:offer_urls = f.readlines()
        for offer_url in offer_urls:
            yield scrapy.Request(offer_url,callback=self.parse,headers=self.headers,cb_kwargs={'offer_url':offer_url,"item":{},"rank":0})
    
    @inline_requests    
    def parse(self,response,offer_url,item={},rank=0):    
        if not bool(item):
            if 'search?q=' in offer_url:
                item['title'] = (offer_url).split('q=')[1].replace('+',' ').capitalize()
                item['page_url'] = offer_url
                item['count'] = 0
            else:
                item['title'] = (offer_url).split('/')[-1].replace('-',' ').capitalize().strip()
                item['page_url'] = offer_url.strip()
                item['count'] = 0
                item['category_crumb']=[]
                category_path = response.xpath('//div[@id="breadcrumbs"]/@data-pwa').get()
                category_names = json.loads(category_path)
                for category in category_names[2:]:
                    url = response.urljoin(category.get('url'))
                    name = category.get('text')
                    item['category_crumb'].append({"name":name,"url":url}.copy())
            item['products']=[]
            
        product_count=len(response.xpath('//div[@class="eba-component eba-product-listing"]/div[contains(@class,"Product")]/a'))
        item['count']=item['count']+product_count
        for link_block in response.xpath('//div[@class="eba-component eba-product-listing"]/div[contains(@class,"Product")]/a'):
            product_url=response.urljoin(link_block.xpath("./@href").get(''))
            product_response = yield Request(product_url,headers=self.headers,dont_filter=True)
            if product_response.xpath('//h1/span[@class="pdp-product-brand-name"]/text()').get('').strip():
                product_dict = {}
                rank=rank+1
                product_dict['rank'] = rank
                product_dict['url'] = product_url
                product_dict['image_url'] = []
                image_url_list = list(set(product_response.xpath("//div[contains(@class,'productpage-gallery')]/div/a/@href|//div[contains(@class,'productpage-image')]/img/@src").getall()))
                image_urls = [ url.replace(' ','%20') for url in image_url_list if not 'youtube' in url ]
                video_url = [ url for url in image_url_list if 'youtube' in url ]
                video_url_player = [ url for url in image_url_list if 'player.vimeo.com' in url ]
                if video_url_player:
                    video_url.extend(video_url_player)
                product_dict['image_url'] = image_urls
                product_dict['has_video'] = False
                product_dict["video"] = []
                if video_url:
                    product_dict['has_video'] = True
                    video_urls = []
                    for video_url_http in video_url:
                        if not 'http' in video_url_http:
                            video_url_http = f'https:{video_url_http}'
                            video_urls.append(video_url_http)
                        else:
                            video_urls.append(video_url_http)
                    product_dict["video"] = video_urls
                product_dict['gtin'] = None
                product_dict['master_product_id'] = None
                if  re.search('\"data\"\:\[\"([^>]*?)\"\]',product_response.text):
                    product_dict['master_product_id'] = re.findall('\"data\"\:\[\"([^>]*?)\"\]',product_response.text)[0]
                if re.search(r'data\-flix\-ean\=\"([^>]*?)\"',product_response.text):
                    gtin = re.findall(r'data\-flix\-ean\=\"([^>]*?)\"',product_response.text)[0]
                    product_dict['gtin'] = gtin if gtin != "" else None
                
                product_dict['name'] = product_response.xpath('//h1/span[@class="pdp-product-brand-name"]/text()').get('').strip()
                product_dict['brand'] = None
                
                if re.search(r'<script\s*type\=\"application\/ld\+json\"[^>]*?>[^>]*?\"brand\"\:\s*\"([^>]*?)\"[^>]*?<\/script>',product_response.text):
                    product_dict['brand'] = re.findall(r'<script\s*type\=\"application\/ld\+json\"[^>]*?>[^>]*?\"brand\"\:\s*\"([^>]*?)\"[^>]*?<\/script>',product_response.text)[0] 
                product_dict['price'] = None
                if product_response.xpath('//p[@class="price-info"]/span/span/text()'):
                    product_dict['price'] = ''.join(product_response.xpath('//p[@class="price-info"]/span/span/text()').getall()).strip()
                in_stock = product_response.xpath('//span[@class="product-options-stock u-error"]/text()|//div[contains(@class,"stock-level")]/text()|//span[@class="product-options-stock"]/text()|//div[@class="sub-products"]/span[@class="option selected"]/span/span[contains(@class,"priceStock")]/span[2]/text()').get("")
                product_dict['in-stock'] = True if 'in stock' in in_stock.lower().strip() else False
                price_before = product_response.xpath('//p[@class="price-info"]/span/span[@class="Price-details"]/span[@class="rrp"]/text()').get('').strip()
                product_dict['price_before'] = price_before if price_before else None
                promo_label = product_response.xpath('//div[@class="discountWrapper"]/span/text()').get('').strip()
                product_dict['promo_label'] = promo_label if promo_label else None
                item['products'].append(product_dict.copy())
        if response.xpath('//div[@class="loadMore loadMoreBottom"]/a/@href').get():
            next_page_url = response.xpath('//div[@class="loadMore loadMoreBottom"]/a/@href').get()
            yield scrapy.Request(response.urljoin(next_page_url),headers=self.headers, callback=self.parse,cb_kwargs={'offer_url':offer_url,"item":item,"rank":rank})
        else:
            yield item
    
        
                
                
     