import re
import os
import json
import scrapy
import datetime as currentdatetime
import urllib.parse
import requests
from parsel import Selector
import traceback2 as traceback
from dotenv import load_dotenv
from pathlib import Path

""" load env file """
try:
    load_dotenv()
except:
    env_path = Path(".env")
    load_dotenv(dotenv_path=env_path)

class LoccitaneOffersSpider(scrapy.Spider):
    name = 'loccitane_fr_offers'
    
    """ Get token in env file"""
    api_token=os.getenv("api_token")
    CURRENT_DATETIME = currentdatetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    dup_list = []
    custom_settings={
        'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/loccitane_fr/%(name)s_{DATE}.json": {"format": "json",}},
        "CONCURRENT_REQUESTS":1}
    
    def retry_function(self,url):
        try:
            retry_count = 0
            while retry_count <=10:
                retry_response=requests.get(url)
                retry_response_text=retry_response.text
                retry_response=Selector(text=retry_response.text)
                if re.search(r'Please\s*enable\s*JS\s*and\s*disable',retry_response_text):
                    retry_count+=1
                else:
                    break
            return retry_response,retry_response_text
        except:
            return None,None

    async def product_collection(self,products,product_links):
        for rank,product_url in enumerate(product_links,1):
            product_url = f'https://fr.loccitane.com/{product_url}'
            parse_product_url=urllib.parse.quote(product_url)
            parse_product_url = f'https://api.scrape.do/?token={self.api_token}&url={parse_product_url}&super=true&geoCode=gb'
            product_response = await self.requests_process((parse_product_url))
            product_response_text = product_response.text
            if re.search(r'Please\s*enable\s*JS\s*and\s*disable',product_response.text):
                product_response,product_response_text = self.retry_function(parse_product_url)
            if product_response is None:
                continue
            product_dict = {}
            product_dict['rank'] = rank
            product_dict['url'] = product_url
            
            product_dict['image_url'] = []
            if re.search(r'<button\s*class\=\"a\-button\s*a\-button\-\-text\s*product\-image\s*main\-image\"[^>]*?>\s*<img[^>]*?src\=\"([^>]*?)\"',product_response_text):
                image_urls = re.findall(r'<button\s*class\=\"a\-button\s*a\-button\-\-text\s*product\-image\s*main\-image\"[^>]*?>\s*<img[^>]*?src\=\"([^>]*?)\"',product_response_text)
                image_urls_data_frz = re.findall(r'<button\s*class\=\"a\-button\s*a\-button\-\-text\s*product\-image\s*main\-image\"[^>]*?>\s*<img[^>]*?data\-frz\-src\=\"([^>]*?)\"',product_response_text)
                if image_urls_data_frz:
                    image_urls.extend(image_urls_data_frz)
                image_urls = [image_url for image_url in image_urls if not 'data:image/gif' in image_url]
            
                product_dict['image_url'] = [image_url.replace(' ','%20') for image_url in image_urls]
            product_dict['master_product_id'] = product_response.xpath('//div[@class="m-product-zoom-sku"]/p/span/text()').get()
            if product_response.xpath('//h1/div/text()').get('').strip():
                product_dict['name'] = product_response.xpath('//h1/div/text()').get('').strip()
                product_dict['brand'] = None
                if re.search('\"brand\"\s*\:\"([^>]*?)\"',product_response_text):        
                    product_dict['brand'] = re.findall('\"brand\"\s*\:\"([^>]*?)\"',product_response_text)[0]
                    if product_dict['brand'] == "":
                        product_dict['brand'] = None
                price = product_response.xpath('//div[@class="m-product-price"]/p[@class="a-price-sales"]/text()').get("").strip()
                video_url = product_response.xpath('//div[@class="video-wrapper"]/@video-id').get('')
                product_dict['has_video'] = False
                product_dict['video'] = []
                if video_url:
                    product_dict['has_video'] = True
                    product_dict['video'] = [f'https://www.youtube.com/embed/{video_url}']
                price = price if not 'N/A' in str(price) else None
                product_dict["price"] =price if price else None
                in_stack = product_response.xpath('//div[@class="m-product-add-to-cart"]/button[contains(text(),"Ajouter au panier")]').get("")
                product_dict['in-stock'] = False
                if in_stack:
                    product_dict['in-stock'] = True
                product_dict['price_before'] = None
                price_before = ' '.join(product_response.xpath('//p[@class="a-price-standard"]/text()').getall()).strip()
                product_dict['price_before'] = price_before if price_before else None
                product_dict['promo_label'] = None
                products.append(product_dict)
        return products
    
    
    def start_requests(self):
        start_url=f"https://api.scrape.do/?token={self.api_token}&url=https://fr.loccitane.com/"
        headers = {
            'accept': 'text/html;charset=UTF-8',
            'accept-language': 'en-GB,en;q=0.9',
            'content-type': 'text/html;charset=UTF-8',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
        }
        yield scrapy.Request(start_url,headers=headers,callback=self.parse)

    async def parse(self, response):
        spider_name = LoccitaneOffersSpider.name.replace('offers','categories')
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
                item = {}
                item['title'] = main_category
                item['page_url'] = main_category_url
                
                parse_main_category_url=urllib.parse.quote(main_category_url)
                parse_main_category_url = f'https://api.scrape.do/?token={self.api_token}&url={parse_main_category_url}&super=true&geoCode=gb'
                page_response = await self.requests_process(parse_main_category_url)
                if re.search(r'Please\s*enable\s*JS\s*and\s*disable',page_response.text):
                    page_response,page_response_text = self.retry_function(parse_main_category_url)
                if page_response is None:
                    continue
                count = page_response.xpath('//div[@class="header-title-wrapper"]/span/span/text()').get('')
                product_links = page_response.xpath('//ul[@id="search-result-items"]//a[@class="a-product-link"]/@href').getall()
                count = count if count else 0
                item['count'] = count
                for page_count in range(1,round(int(count)/24)+1):
                    next_page_url = f'{main_category_url}?srule=Category-default&sz=24&start={page_count*24}'
                    next_page_url=urllib.parse.quote(next_page_url)
                    next_page_url = f'https://api.scrape.do/?token={self.api_token}&url={next_page_url}&super=true&geoCode=gb'
                    next_page_response = await self.requests_process(next_page_url)
                    if re.search(r'Please\s*enable\s*JS\s*and\s*disable',next_page_response.text):
                        next_page_response,next_page_response_text = self.retry_function(next_page_url)
                    if next_page_response is None:
                        continue
                    next_product_link = next_page_response.xpath('//ul[@id="search-result-items"]//a[@class="a-product-link"]/@href').getall()
                    product_links.extend(next_product_link)
                category_crumb_list = []
                category_crumb_list.append({"name":category_name,"url":category_url})
                category_crumb_list.append({"name":main_category,"url":main_category_url})
                item['category_crumb'] = category_crumb_list
                products = []
                products = await self.product_collection(products,product_links)
                item['products'] = products
                yield item
                    
            
        
        """ read loccitane search url text file"""
        dir_path=os.path.abspath(__file__ + "/../../../")
        supporting_files=os.path.join(dir_path,"supporting_files")
        with open(f'{supporting_files}/loccitane_fr_offer_input.txt') as f:
            offers_urls=f.readlines()
        
        for offer_url in offers_urls:
            try:
                offer_url = offer_url.strip()
                parse_offer_url=urllib.parse.quote(offer_url)
                parse_offer_url = f'https://api.scrape.do/?token={self.api_token}&url={parse_offer_url}&super=true&geoCode=gb'
                page_offer_response = await self.requests_process(parse_offer_url)
                if re.search(r'Please\s*enable\s*JS\s*and\s*disable',page_offer_response.text):
                    page_offer_response,page_offer_response_text = self.retry_function(parse_offer_url)
                if page_offer_response is None:
                    continue
                item = {}
                item['title'] = offer_url.split('q=')[1].replace('+',' ').capitalize()
                item['page_url'] = offer_url
                count = page_offer_response.xpath('//h1[@class="o-search-results-title"]/text()').re_first('^\s*([\d]+)\s+')
                count = count if count else 0
                item['count'] = count
                product_links = page_offer_response.xpath('//ul[@id="search-result-items"]//a[@class="a-product-link"]/@href').getall()
                for page_count in range(1,round(int(count)/24)+1):
                    next_page_url = f'{offer_url}&srule=Default-BestSeller&sz=24&start={page_count*24}&format=ajax&type=view-more&context=products-list'
                    next_page_url=urllib.parse.quote(next_page_url)
                    next_page_url = f'https://api.scrape.do/?token={self.api_token}&url={next_page_url}&super=true&geoCode=gb'
                    next_page_response = await self.requests_process(next_page_url)
                    if re.search(r'Please\s*enable\s*JS\s*and\s*disable',next_page_response.text):
                        next_page_response,next_page_response_text = self.retry_function(next_page_url)
                    if next_page_response is None:
                        continue
                    next_product_link = next_page_response.xpath('//ul[@id="search-result-items"]//a[@class="a-product-link"]/@href').getall()
                    product_links.extend(next_product_link)
                products = []
                products = await self.product_collection(products,product_links)
                item['products'] = products
                yield item
            except:
                print(traceback.format_exc())

    async def requests_process(self,url):
        request = scrapy.Request(url)
        response = await self.crawler.engine.download(request, self)
        return response
        

            
