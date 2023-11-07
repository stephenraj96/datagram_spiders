import re
import os
import json
import scrapy
import datetime as currentdatetime
from datetime import datetime
import urllib.parse
from parsel import Selector
import requests
from dotenv import load_dotenv
from pathlib import Path
from time import sleep 

""" load env file """
try:
    load_dotenv()
except:
    env_path = Path(".env")
    load_dotenv(dotenv_path=env_path)

class LoccitanePdpSpider(scrapy.Spider):
    name = 'loccitane_fr_pdp'
    headers = {
            'accept': 'text/html;charset=UTF-8',
            'accept-language': 'en-GB,en;q=0.9',
            'content-type': 'text/html;charset=UTF-8',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
        }
    """ Get token in env file"""
    api_token=os.getenv("api_token")
    
    CURRENT_DATETIME = currentdatetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    custom_settings={
    'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/loccitane_fr/%(name)s_{DATE}.json": {"format": "json"}},
    "CONCURRENT_REQUESTS":2,
    # 'DOWNLOAD_DELAY' : 1
    }
    
    def start_requests(self):
        start_url=f"https://api.scrape.do/?token={self.api_token}&url=https://fr.loccitane.com/"
        yield scrapy.Request(start_url,headers=self.headers,callback=self.parse)
    

    def retry_function(self,url):
        retry_count = 0
        while retry_count <=10:
            retry_response=requests.get(url)
            sleep(2)
            retry_response_text=retry_response.text
            retry_response=Selector(text=retry_response.text)
            if re.search(r'Please\s*enable\s*JS\s*and\s*disable',retry_response_text):
                retry_count+=1
            else:
                break
        return retry_response,retry_response_text
    
    def parse(self, response):
        spider_name = LoccitanePdpSpider.name.replace('pdp','categories')
        current_date = currentdatetime.datetime.now().strftime("%Y-%m-%d")
        category_path = os.getcwd() + f'/exports/{spider_name}/{spider_name}_{current_date}.json'
        with open(category_path,'r',encoding='utf-8-sig') as f:
            json_file = f.read()
        category_names = json.loads(json_file)
        for main_cat in category_names:
            category_name = main_cat.get('name','').title()
            category_url = main_cat.get('url','')
            for main_sub_cat in main_cat['category_crumb']:
                main_category = main_sub_cat.get('name','').title()
                main_category_url = main_sub_cat.get('url','')
                parse_main_category_url=urllib.parse.quote(main_category_url)
                parse_main_category_url = f'https://api.scrape.do/?token={self.api_token}&url={parse_main_category_url}&super=true&geoCode=gb'
                yield scrapy.Request(url=(parse_main_category_url),callback=self.parse_list,dont_filter=True,cb_kwargs={"category_name":category_name,"category_url":category_url,"main_category":main_category,"main_category_url":main_category_url})
                
    async def parse_list(self,response,category_name,category_url,main_category,main_category_url):
        if re.search(r'Please\s*enable\s*JS\s*and\s*disable',response.text):
            list_page_response,list_page_response_text = self.retry_function(response.url)
            response = list_page_response
        
        for product_list_xpath in response.xpath('//ul[@id="search-result-items"]//a[@class="a-product-link"]'):
            product_url =  product_list_xpath.xpath('./@href').get()
            product_url = f'https://fr.loccitane.com/{product_url}'
            parse_product_url=urllib.parse.quote(product_url)
            parse_product_url = f'https://api.scrape.do/?token={self.api_token}&url={parse_product_url}&super=true&geoCode=gb'           
            yield scrapy.Request(url=parse_product_url,callback=self.product_detail,dont_filter=True,cb_kwargs={"category_name":category_name,"category_url":category_url,"main_category":main_category,"main_category_url":main_category_url,"product_url":product_url})
        
        count = response.xpath('//div[@class="header-title-wrapper"]/span/span/text()').get('')
        count = count if count else 0
        product_links = []    
        for page_count in range(1,round(int(count)/24)+1):
            next_page_url = f'{main_category_url}?srule=Category-default&sz=24&start={page_count*24}'
            next_page_url=urllib.parse.quote(next_page_url)
            next_page_url = f'https://api.scrape.do/?token={self.api_token}&url={next_page_url}&super=true&geoCode=gb'
            next_page_response = await self.requests_process(next_page_url)
            if re.search(r'Please\s*enable\s*JS\s*and\s*disable',next_page_response.text):
                next_page_response,next_page_response_text = self.retry_function(next_page_url)
            
            next_product_link = next_page_response.xpath('//ul[@id="search-result-items"]//a[@class="a-product-link"]/@href').getall()
            product_links.extend(next_product_link)
        
        for product_url in product_links:
            product_url = f'https://fr.loccitane.com/{product_url}'
            parse_product_url=urllib.parse.quote(product_url)
            parse_product_url = f'https://api.scrape.do/?token={self.api_token}&url={parse_product_url}&super=true&geoCode=gb'
            yield scrapy.Request(url=parse_product_url,callback=self.product_detail,dont_filter=True,cb_kwargs={"category_name":category_name,"category_url":category_url,"main_category":main_category,"main_category_url":main_category_url,"product_url":product_url})
 
        
    async def product_detail(self,response,category_name,category_url,main_category,main_category_url,product_url):
        item = {}
        item['url'] = product_url
        category_crumb = []
        main_cat = {}
        main_cat['name'] = category_name
        main_cat['url'] = category_url
        category_crumb.append(main_cat)
        sub_main_cat = {}
        sub_main_cat['name'] = main_category
        sub_main_cat['url'] = main_category_url
        category_crumb.append(sub_main_cat)
        item['category_crumb'] = category_crumb
        if re.search(r'Please\s*enable\s*JS\s*and\s*disable',response.text):
            response,response_text = self.retry_function(response.url)
        else:
            response_text = response.text
        if response.xpath('//h1/div/text()').get('').strip():
            item['name'] = response.xpath('//h1/div/text()').get('').strip()
            item['subtitle'] = None
            item['image_url'] = []
            if re.search(r'<button\s*class\=\"a\-button\s*a\-button\-\-text\s*product\-image\s*main\-image\"[^>]*?>\s*<img[^>]*?src\=\"([^>]*?)\"',response_text):
                image_urls = re.findall(r'<button\s*class\=\"a\-button\s*a\-button\-\-text\s*product\-image\s*main\-image\"[^>]*?>\s*<img[^>]*?src\=\"([^>]*?)\"',response_text)
                image_urls_data_frz = re.findall(r'<button\s*class\=\"a\-button\s*a\-button\-\-text\s*product\-image\s*main\-image\"[^>]*?>\s*<img[^>]*?data\-frz\-src\=\"([^>]*?)\"',response_text)
                if image_urls_data_frz:
                    image_urls.extend(image_urls_data_frz)
                image_urls = [image_url for image_url in image_urls if not 'data:image/gif' in image_url]
                
                item['image_url'] = [image_url.replace(' ','%20') for image_url in image_urls]
            video_url = response.xpath('//div[@class="video-wrapper"]/@video-id').get('')
            item['has_video'] = False
            item['video'] = []
            if video_url:
                item['has_video'] = True
                item['video'] = [f'https://www.youtube.com/embed/{video_url}']
            item['brand'] = None
            if re.search('\"brand\"\s*\:\"([^>]*?)\"',response_text):        
                item['brand'] = re.findall('\"brand\"\s*\:\"([^>]*?)\"',response_text)[0]
                if item['brand'] == "":
                    item['brand'] = None
            item['master_product_id'] = None
            item['gtin'] = None
            item['master_product_id'] = response.xpath('//div[@class="m-product-zoom-sku"]/p/span/text()').get()
            if re.search('\"gtin13\"\:\"([^>]*?)\"',response_text):
                item['gtin'] = re.findall('\"gtin13\"\:\"([^>]*?)\"',response_text)[0]
                if item['gtin'] == "":
                    item['gtin'] = None
            in_stack = response.xpath('//div[@class="m-product-add-to-cart"]/button[contains(text(),"Ajouter au panier")]').get("")
            item['in-stock'] = False
            if in_stack:
                item['in-stock'] = True
            price = response.xpath('//div[@class="m-product-price"]/p[@class="a-price-sales"]/text()').get("").strip()
            price = price if not 'N/A' in str(price) else None
            item["price"] =price if price else None
            item['price_before'] = None
            price_before = ' '.join(response.xpath('//p[@class="a-price-standard"]/text()').getall()).strip()
            item['price_before'] = price_before if price_before else None
            item['promo_label'] = None
            description = re.sub(r'\s+',' ', ' '.join(response.xpath('//div[contains(@class,"product-desc-content")]//text()').getall()))
            item['description'] = None
            if description:
                item['description'] = description.strip()
            prices = []
            variant_urls = response.xpath('//div[@class="product-variations"]//li//option/@value|//div[@class="product-variations"]//li/a/@href').getall()
            variant_product_url = response.xpath('//div[@class="product-variations"]/@data-attributes').get()
            if not variant_product_url == '{}':
                variant_urls.append(product_url)
            for variant_url in list(set(variant_urls)):
                parse_variant_url=urllib.parse.quote(variant_url)
                parse_variant_url = f'https://api.scrape.do/?token={self.api_token}&url={parse_variant_url}&super=true&geoCode=gb'
                variant_response = await self.requests_process(parse_variant_url)
                if re.search(r'Please\s*enable\s*JS\s*and\s*disable',variant_response.text):
                    variant_response,variant_response_text = self.retry_function(parse_variant_url)
                else:
                    variant_response_text = variant_response.text
                variant_dict = {}
                variant_dict['sku_id'] = variant_response.xpath('//div[@class="m-product-zoom-sku"]/p/span/text()').get()
                variant_dict['gtin']=None
                if re.search('\"gtin13\"\:\"([^>]*?)\"',variant_response_text):
                    variant_dict['gtin'] = re.findall('\"gtin13\"\:\"([^>]*?)\"',variant_response_text)[0]
                    if variant_dict['gtin'] == "":
                        variant_dict['gtin'] = None
                variant_dict['variant_url'] = variant_url
                variant_dict['image_url'] = []
                video_url = variant_response.xpath('//div[@class="video-wrapper"]/@video-id').get('')
                variant_dict['has_video'] = False
                variant_dict['video'] = []
                if video_url:
                    variant_dict['has_video'] = True
                    variant_dict['video'] = [f'https://www.youtube.com/embed/{video_url}']
                if re.search(r'<button\s*class\=\"a\-button\s*a\-button\-\-text\s*product\-image\s*main\-image\"[^>]*?>\s*<img[^>]*?src\=\"([^>]*?)\"',variant_response_text):
                    variant_image_urls = re.findall(r'<button\s*class\=\"a\-button\s*a\-button\-\-text\s*product\-image\s*main\-image\"[^>]*?>\s*<img[^>]*?src\=\"([^>]*?)\"',variant_response_text)
                    variantimage_urls_data_frz = re.findall(r'<button\s*class\=\"a\-button\s*a\-button\-\-text\s*product\-image\s*main\-image\"[^>]*?>\s*<img[^>]*?data\-frz\-src\=\"([^>]*?)\"',variant_response_text)
                    if variantimage_urls_data_frz:
                        variant_image_urls.extend(variantimage_urls_data_frz)
                    variant_image_urls = [variant_image_url for variant_image_url in variant_image_urls if not 'data:image/gif' in variant_image_url]
                    
                    variant_dict['image_url'] = [variant_image_url.replace(' ','%20') for variant_image_url in variant_image_urls]
                variant_price = variant_response.xpath('//div[@class="m-product-price"]/p[@class="a-price-sales"]/text()').get("").strip()
                variant_price = variant_price if not 'N/A' in str(variant_price) else None
                variant_dict["price"] =variant_price if variant_price else None
                in_stack = variant_response.xpath('//div[@class="m-product-add-to-cart"]/button[contains(text(),"Ajouter au panier")]').get("")
                variant_dict['in-stock'] = False
                if in_stack:
                    variant_dict['in-stock'] = True
                variant_dict['data_size'] = None
                variant_dict['data_color'] = None
                data_variant= variant_response.xpath('//div[@class="product-variations"]/@data-attributes').get()
                data_size = None
                data_color = None
                if data_variant:
                    data_variant_json = json.loads(data_variant)
                    variant_format = data_variant_json.get('format',{}).get('displayValue')
                    variant_volume = data_variant_json.get('volume',{}).get('displayValue')
                    variant_weight = data_variant_json.get('weight',{}).get('displayValue')
                    variant_shade = data_variant_json.get('shade',{}).get('displayValue')
                    data_scent = data_variant_json.get('scent',{}).get('displayValue')
                    if variant_format and variant_volume:
                        data_size = f'{variant_format} - {variant_volume}'
                    elif variant_volume:
                        data_size = variant_volume
                    elif variant_format and variant_weight:
                        data_size = f'{variant_format} - {variant_weight}'
                    elif variant_weight:
                        data_size = variant_weight
                    if data_scent:
                        data_color = data_scent
                    elif variant_shade:
                        data_color = variant_shade
                if data_size or data_color:
                    variant_dict['data_size'] = data_size
                    variant_dict['data_color'] = data_color
                    variant_video_url = variant_response.xpath('//div[@class="video-wrapper"]/@video-id').get('')
                    variant_dict['has_video'] = False
                    variant_dict['video'] = []
                    if variant_video_url:
                        variant_dict['has_video'] = True
                        variant_dict['video'] = [f'https://www.youtube.com/embed/{variant_video_url}']
                    variant_dict['price_before'] = None
                    varoant_price_before = ''.join(variant_response.xpath('//p[@class="a-price-standard"]/text()').getall()).strip()
                    variant_dict['price_before'] = varoant_price_before if varoant_price_before else None
                    variant_dict['promo_label'] = None
                    prices.append(variant_dict)
            item['prices'] = prices
            
            merchant_id = response.xpath('//input[@id="merchant_id"]/@value').get('')
            api_key = response.xpath('//input[@id="api_key"]/@value').get('')
            review_post_url = f'https://display.powerreviews.com/m/{merchant_id}/l/fr_FR/product/{item["master_product_id"]}/reviews?apikey={api_key}&_noconfig=true'
            post_response = await self.requests_process(review_post_url)
            review_list = []
            review_list = self.review_write(post_response,review_list)
            page_total = post_response.json().get('paging',{}).get('pages_total','')
            nextpage_url =  post_response.json().get('paging',{}).get('next_page_url','')
            if page_total:
                for _ in range(2,page_total+1):
                    nextpage_url = f"https://display.powerreviews.com{nextpage_url}&apikey={api_key}"
                    next_review_resp = await self.requests_process(nextpage_url)
                    nextpage_url =  next_review_resp.json().get('paging',{}).get('next_page_url','')
                    review_list = self.review_write(next_review_resp,review_list)
            item["reviews"] = review_list
            yield item
        
            
    
    def review_write(self,review_response,review_list):
        if review_response.json().get('results'):
            for reviews in review_response.json().get('results')[0]['reviews']:
                review_dict = {}
                headline = reviews.get('details',{}).get('headline')
                comments = reviews.get('details',{}).get('comments')
                review_dict['review'] = f"{headline} {comments}"
                review_dict['stars'] = reviews.get('metrics',{}).get('rating','')
                review_dict['user'] = reviews.get('details',{}).get('nickname')
                timestamp = reviews.get('details',{}).get('created_date')
                review_dict['date'] = datetime.fromtimestamp(timestamp/1000).strftime('%Y-%m-%d')
                review_list.append(review_dict)
        return review_list
    
    async def requests_process(self,url):
        request = scrapy.Request(url,dont_filter=True)
        response = await self.crawler.engine.download(request, self)
        return response