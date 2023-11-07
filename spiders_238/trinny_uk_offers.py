import re
import os
import json
import scrapy
import codecs
import datetime
from parsel import Selector
from dotenv import load_dotenv

load_dotenv()

class TrinnyOfferSpider(scrapy.Spider):
    name = 'trinny_uk_offers'
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    ROTATING_PROXY = os.getenv("ROTATING_PROXY")
    custom_settings={
        'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/trinny_uk/%(name)s_{DATE}.json": {"format": "json",}},
    'ROTATING_PROXY_LIST' : [ROTATING_PROXY]
    }
    start_urls = ['http://www.trinnylondon.com/']
    
    def product_details(self,product_url,product_response,response_selector,response,product_item,rank):
        product_response_text = (str(product_response.text)).replace('\\"','\"')
        product_item['rank'] = rank
        product_item['url'] = response.urljoin(product_url)
        product_item['image_url'] = list(set(response_selector.xpath('//div[contains(@class,"swiper-slide")]/div/img/@src').getall()))
        video_url = ""
        if re.search('\"desktopVideo\"[^>]*?\"file\"\:\{\"url\"\:\"([^>]*?)\"\,',product_response_text):
            video_url = re.findall('\"desktopVideo\"[^>]*?\"file\"\:\{\"url\"\:\"([^>]*?)\"\,',product_response_text)[0]
        elif re.search('\"trinnyTipVideo\"\:\{\"url\"\:\"([^>]*?)\"\,',product_response_text):
            video_url = re.findall('\"trinnyTipVideo\"\:\{\"url\"\:\"([^>]*?)\"\,',product_response_text)[0]
        product_item['has_video'] = False
        product_item['video'] = []
        if video_url:
            product_item['has_video'] = True
            video_url = codecs.decode(video_url,'unicode-escape')
            if 'http' in video_url:
                product_item['video'] = [video_url]
            else:
                product_item['video'] = [f'https:{video_url}']
        product_item['master_product_id'] = None
        product_item['variant_id'] = None
        if  re.search('\"image\"\:\"[^>]*?\"sku\"\:\"([^>]*?)\"',product_response_text):
            product_item['master_product_id'] = re.findall('\"image\"\:\"[^>]*?\"sku\"\:\"([^>]*?)\"',product_response_text)[0]
        elif re.search('\"Product\"\,\"name\"\:[^>]*?\"sku\"\:\"([^>]*?)\"',product_response_text):
            product_item['master_product_id'] = re.findall('\"Product\"\,\"name\"\:[^>]*?\"sku\"\:\"([^>]*?)\"',product_response_text)[0]
        elif re.search('\"collectionGroupDescription\"\:\"[^>]*?\"\,\"sku\"\:\"([^>]*?)\"\,',product_response_text):
            product_item['master_product_id'] = re.findall('\"collectionGroupDescription\"\:\"[^>]*?\"\,\"sku\"\:\"([^>]*?)\"\,',product_response_text)[0]
        product_item['variant_id'] = product_item['master_product_id']
        product_item['name'] = response_selector.xpath('//h1//text()').get()
        product_item['brand'] = response_selector.xpath('//meta[@property="og:site_name"]/@content').get()
        if response_selector.xpath('//div[contains(@class,"ProductInfo_")]/div/div[@data-test-id="text-pill"]/text()'):
            product_item['price'] = response_selector.xpath('//div[contains(@class,"ProductInfo_")]/div/div[@data-test-id="text-pill"]/text()').get('').replace('Worth ','').replace('NORMAL/DRY','').replace('NEW SHADES','').replace('NEW','')
            if product_item['price'] == ""or not re.search(r'\d',product_item['price']):
                product_item['price'] = response_selector.xpath('//button[@data-test-id="buy-button"]//span[@data-test-id="price"]/text()').get()
        else:
            product_item['price'] = response_selector.xpath('//button[@data-test-id="buy-button"]//span[@data-test-id="price"]/text()').get()
        in_stack = response_selector.xpath('//div[@data-test-id="add-to-bag-container"]/button[@disabled]').get("")
        product_item['in-stock'] = True
        if in_stack:
            product_item['in-stock'] = False
        
        
        product_item["price_before"] = None
        product_item["promo_label"] = None
        return product_item
    
    def parse(self, response):
        dir_path=os.path.abspath(__file__ + "/../../../")
        supporting_files=os.path.join(dir_path,"supporting_files")
        with open(f'{supporting_files}/trinny_uk_offer_input.txt') as f:offer_urls = f.readlines()
        for offer_url in offer_urls:
            yield scrapy.Request(offer_url,callback=self.parse_list)
    async def parse_list(self,response):
        item = {}
        if '?search=' in response.url:
            item['title'] = (response.url).split('?search=')[-1].replace('%20',' ')
            item['page_url'] = response.url
            item['count'] = len(response.xpath('//div[@class="SearchTile_variant_BLtrZ"]/a'))
            product_xpath = '//div[@class="SearchTile_variant_BLtrZ"]/a'
        else:
            item['title'] = (response.url).split('/')[-1].replace('%20',' ')
            item['page_url'] = response.url
            item['count'] = len(response.xpath('//h6/a|//article[@data-test-id="product-tile"]//h3/a'))
            spider_name = TrinnyOfferSpider.name.replace('offers','categories')
            current_date = datetime.datetime.now().strftime("%Y-%m-%d")
            category_path = os.getcwd() + f'/exports/{spider_name}/{spider_name}_{current_date}.json'
            with open(category_path,'r',encoding='utf-8-sig') as f:
                json_file = f.read()
            category_names = json.loads(json_file)
            category_crumb = []
            matching_word = (response.url).split('/')[-1]
            break_flag = False
            for main_cat in category_names:
                category_name = main_cat.get('name','').title()
                category_url = main_cat.get('url','')
                for main_sub_cat in main_cat['category_crumb']:
                    if 'category_crumb' in (main_sub_cat).keys():
                        main_category = main_sub_cat.get('name','').title()
                        main_category_url = main_sub_cat.get('url','')
                        for sub_cat in main_sub_cat['category_crumb']:
                            sub_category = sub_cat.get('name','').title()
                            sub_cat_url = sub_cat.get('url','')
                            if matching_word in sub_cat_url:
                                category_crumb.append({'name':category_name,"url":category_url})
                                category_crumb.append({'name':main_category,"url":main_category_url})
                                category_crumb.append({'name':sub_category,"url":sub_cat_url})
                                break_flag = True
                                break
                            
                    else:
                        main_category = main_sub_cat.get('name','').title()
                        main_category_url = main_sub_cat.get('url','')
                        if matching_word in main_category_url:
                            category_crumb.append({'name':category_name,"url":category_url})
                            category_crumb.append({'name':main_category,"url":main_category_url})
                            break_flag = True
                            break
                    if break_flag is True:
                        break
                if break_flag is True:
                    break
            item['category_crumb'] = category_crumb
            product_xpath = '//h6/a|//div[@data-test-id="product-list-all"]//h3/a'
        products = []
        for rank,product_xpath in enumerate(response.xpath(product_xpath),1):
            product_item = {}
            product_url = product_xpath.xpath('./@href').get()
            product_response = await self.requests_process(response.urljoin(product_url))
            response_selector = Selector(text=product_response.text)
            product_item = self.product_details(product_url,product_response,response_selector,response,product_item,rank)
            products.append(product_item)
        if item['count'] == 0 and response.xpath('//button[@data-test-id="buy-button"]//span[@data-test-id="price"]/text()'):
            product_item = {}
            item['count'] = 1
            rank = 1
            product_item = self.product_details(response.url,response,response,response,product_item,rank)
            products.append(product_item)
        item["products"] = products
        yield item
            
    async def requests_process(self,url):
        request = scrapy.Request(url)
        response = await self.crawler.engine.download(request, self)
        return response

