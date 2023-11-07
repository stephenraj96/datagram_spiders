import re
import os
import json
import codecs
import scrapy
import datetime
import dateparser
from parsel import Selector
from dotenv import load_dotenv

load_dotenv()
class TrinnyProductSpider(scrapy.Spider):
    name = 'trinny_uk_pdp'
    start_urls = ['https://trinnylondon.com/uk/collections/face']
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    dup_list = []
    ROTATING_PROXY = os.getenv("ROTATING_PROXY")
    custom_settings={
        'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/trinny_uk/%(name)s_{DATE}.json": {"format": "json",}},
    'ROTATING_PROXY_LIST' : [ROTATING_PROXY],
    'DOWNLOAD_DELAY' : 3
    }
    def parse(self, response):
        spider_name = TrinnyProductSpider.name.replace('pdp','categories')
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        category_path = os.getcwd() + f'/exports/{spider_name}/{spider_name}_{current_date}.json'
        with open(category_path,'r',encoding='utf-8-sig') as f:
            json_file = f.read()
        category_names = json.loads(json_file)
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
                        yield scrapy.Request(url=(sub_cat_url),callback=self.parse_list,dont_filter=True,cb_kwargs={"category_name":category_name,"category_url":category_url,"main_category":main_category,"main_category_url":main_category_url,"sub_category":sub_category,"sub_cat_url":sub_cat_url})
                else:
                    main_category = main_sub_cat.get('name','').title()
                    main_category_url = main_sub_cat.get('url','')
                    sub_category = ''
                    sub_cat_url = ""
                    yield scrapy.Request(url=(main_category_url),callback=self.parse_list,dont_filter=True,cb_kwargs={"category_name":category_name,"category_url":category_url,"main_category":main_category,"main_category_url":main_category_url,"sub_category":sub_category,"sub_cat_url":sub_cat_url})
                
    def parse_list(self,response,category_name,category_url,main_category,main_category_url,sub_category,sub_cat_url):
        if response.xpath('//h6/a|//article[@data-test-id="product-tile"]//h3/a'):
            for product_list_xpath in response.xpath('//h6/a|//article[@data-test-id="product-tile"]//h3/a'):
                product_url =  product_list_xpath.xpath('./@href').get()
                if not product_url in self.dup_list:
                    self.dup_list.append(product_url)
                    yield scrapy.Request(url=response.urljoin(product_url),callback=self.product_detail,dont_filter=True,cb_kwargs={"category_name":category_name,"category_url":category_url,"main_category":main_category,"main_category_url":main_category_url,"sub_category":sub_category,"sub_cat_url":sub_cat_url})
        else:
            if not response.url in self.dup_list:
                self.dup_list.append(response.url)
                yield scrapy.Request(url=response.url,callback=self.product_detail,dont_filter=True,cb_kwargs={"category_name":category_name,"category_url":category_url,"main_category":main_category,"main_category_url":main_category_url,"sub_category":sub_category,"sub_cat_url":sub_cat_url})
    async def product_detail(self,response,category_name,category_url,main_category,main_category_url,sub_category,sub_cat_url):
        item = {}
        item['url'] = response.url
        response_text = (str(response.text)).replace('\\"','\"')
        category_crumb = []
        main_cat = {}
        main_cat['name'] = category_name
        main_cat['url'] = category_url
        category_crumb.append(main_cat)
        sub_main_cat = {}
        sub_main_cat['name'] = main_category
        sub_main_cat['url'] = main_category_url
        category_crumb.append(sub_main_cat)
        if sub_category and sub_cat_url:
            sub_cat = {}
            sub_cat['name'] = sub_category
            sub_cat['url'] = sub_cat_url
            category_crumb.append(sub_cat)
        item['category_crumb'] = category_crumb
        item['name'] = response.xpath('//h1//text()').get()
        item['subtitle'] = None
        if response.xpath('//div[@data-test-id="featured-text"]//text()|//div[@data-test-id="description"]//text()'):
            item['subtitle']  = ' '.join([temp.strip() for temp in response.xpath('//div[@data-test-id="featured-text"]//text()|//div[@data-test-id="description"]//text()').getall()]) 
        item['image_url'] = list(set(response.xpath('//div[contains(@class,"swiper-slide")]/div/img/@src').getall()))
        video_url = ""
        if re.search('\"desktopVideo\":[^>]*?\"file\"\:\{\"url\"\:\"([^>]*?)\"\,',response_text):
            video_url = re.findall('\"desktopVideo\":[^>]*?\"file\"\:\{\"url\"\:\"([^>]*?)\"\,',response_text)[0]
        elif re.search('\"trinnyTipVideo\"\:\{\"url\"\:\"([^>]*?)\"\,',response_text):
            video_url = re.findall('\"trinnyTipVideo\"\:\{\"url\"\:\"([^>]*?)\"\,',response_text)[0]
        item['has_video'] = False
        item['video'] = []
        if video_url:
            item['has_video'] = True
            video_url = codecs.decode(video_url,'unicode-escape')
            if 'http' in video_url:
                item['video'] = [video_url]
            else:
                item['video'] = [f'https:{video_url}']
                
        item['brand'] = response.xpath('//meta[@property="og:site_name"]/@content').get()
        item['master_product_id'] = None
        item['gtin'] = None
        if  re.search('\"image\"\:\"[^>]*?\"sku\"\:\"([^>]*?)\"',response_text):
            item['master_product_id'] = re.findall('\"image\"\:\"[^>]*?\"sku\"\:\"([^>]*?)\"',response_text)[0]
        elif re.search('\"Product\"\,\"name\"\:[^>]*?\"sku\"\:\"([^>]*?)\"',response_text):
            item['master_product_id'] = re.findall('\"Product\"\,\"name\"\:[^>]*?\"sku\"\:\"([^>]*?)\"',response_text)[0]
        elif re.search('\"collectionGroupDescription\"\:\"[^>]*?\"\,\"sku\"\:\"([^>]*?)\"\,',response_text):
            item['master_product_id'] = re.findall('\"collectionGroupDescription\"\:\"[^>]*?\"\,\"sku\"\:\"([^>]*?)\"\,',response_text)[0]
        if re.search('\"image\"\:\"[^>]*?\"sku\"\:\"[^>]*?\"\,\"gtin13"\:\"([^>]*?)\"',response_text):
            item['gtin'] = re.findall('\"image\"\:\"[^>]*?\"sku\"\:\"[^>]*?\"\,\"gtin13"\:\"([^>]*?)\"',response_text)[0]
        in_stock = response.xpath('//button[@disabled]/span/text()').get('')
        item['in-stock'] = True
        if 'Out of Stock' in in_stock:
            item['in-stock'] = False
        
        if response.xpath('//div[contains(@class,"ProductInfo_")]/div/div[@data-test-id="text-pill"]/text()'):
            item['price'] = response.xpath('//div[contains(@class,"ProductInfo_")]/div/div[@data-test-id="text-pill"]/text()').get('').replace('Worth ','').replace('NORMAL/DRY','').replace('NEW SHADES','').replace('NEW','')
            if item['price'] == "" or not re.search(r'\d',item['price']):
                item['price'] = response.xpath('//button[@data-test-id="buy-button"]//span[@data-test-id="price"]/text()').get()
        else:
            item['price'] = response.xpath('//button[@data-test-id="buy-button"]//span[@data-test-id="price"]/text()').get()
        
        item['price_before'] = None
        item['promo_label'] = None
        prices = []
        if re.search(r'"offers"\:([^>]*?\])',response_text):
            variant_block =  re.findall(r'"offers"\:([^>]*?\])',response_text)[0]
            variant_json = json.loads(variant_block)
            for variant in variant_json:
                variant_dict = {}
                variant_url = variant.get('url')
                variant_dict['variant_url'] = variant.get('url')
                variant_dict['sku_id'] = variant.get('sku')
                variant_dict['gtin']  = variant.get('gtin13') if variant.get('gtin13') else None 
                variant_response = await self.requests_process(variant_url)
                variant_response_selector = Selector(text=variant_response.text)
                variant_dict['price'] =  variant_response_selector.xpath('//button[@data-test-id="buy-button"]//span[@data-test-id="price"]/text()').get()
                variant_dict['image_url'] = list(set(variant_response.xpath('//div[contains(@class,"swiper-slide")]/div/img/@src').getall()))
                vatiant_video_url = ""
                variant_response_text = (str(variant_response.text)).replace('\\"','\"')
                if re.search('\"desktopVideo\":[^>]*?\"file\"\:\{\"url\"\:\"([^>]*?)\"\,',variant_response_text):
                    vatiant_video_url = re.findall('\"desktopVideo\":[^>]*?\"file\"\:\{\"url\"\:\"([^>]*?)\"\,',variant_response_text)[0]
                elif re.search('\"trinnyTipVideo\"\:\{\"url\"\:\"([^>]*?)\"\,',variant_response_text):
                    vatiant_video_url = re.findall('\"trinnyTipVideo\"\:\{\"url\"\:\"([^>]*?)\"\,',variant_response_text)[0]
                variant_dict['has_video'] = False
                variant_dict['video'] = []
                if vatiant_video_url:
                    variant_dict['has_video'] = True
                    vatiant_video_url = codecs.decode(vatiant_video_url,'unicode-escape')
                    if 'http' in vatiant_video_url:
                        variant_dict['video'] = [vatiant_video_url]
                    else:
                        variant_dict['video'] = [f'https:{vatiant_video_url}']
                variant_dict['data_color'] = variant.get('name').split('-')[1].strip()
                variant_dict['data_size'] = None
                in_stock = variant_response_selector.xpath('//button[@disabled]/span/text()').get('')
                variant_dict['in-stock'] = True
                if 'Out of Stock' in in_stock:
                    variant_dict['in-stock'] = False
                variant_dict['price_before'] = None
                variant_dict['promo_label'] = None
                if variant_dict['price'] and variant_dict['image_url']:
                    prices.append(variant_dict)
        elif re.search(r'\"product\"\:\{\"__typename\"\:[^>]*?\"variants\"\:([^>]*?\]\}\])\,',response_text):
            variant_block =  re.findall(r'\"product\"\:\{\"__typename\"\:[^>]*?\"variants\"\:([^>]*?\]\}\])\,',response_text)[0]
            variant_json = json.loads(variant_block)
            for variant in variant_json:
                variant_dict = {}
                title = variant.get('title').lower()
                title = re.sub(r'\+','',title)
                title = re.sub(r'\s+',' ',title)
                title = re.sub(r'\s+','-',title)
                variant_url = f'{response.url}?variant={title}'
                variant_dict['variant_url'] = variant_url
                variant_dict['sku_id'] = variant.get('sku')
                variant_dict['gtin']  = variant.get('gtin') if variant.get('gtin13') else None
                variant_response = await self.requests_process(variant_url)
                variant_response_text = (str(variant_response.text)).replace('\\"','\"')
                variant_response_selector = Selector(text=variant_response.text)
                variant_dict['price'] =  variant_response_selector.xpath('//button[@data-test-id="buy-button"]//span[@data-test-id="price"]/text()').get()
                variant_dict['image_url'] = list(set(variant_response_selector.xpath('//div[contains(@class,"swiper-slide")]/div/img/@src').getall()))
                vatiant_video_url = ""
                if re.search('\"desktopVideo\":[^>]*?\"file\"\:\{\"url\"\:\"([^>]*?)\"\,',variant_response_text):
                    vatiant_video_url = re.findall('\"desktopVideo\":[^>]*?\"file\"\:\{\"url\"\:\"([^>]*?)\"\,',variant_response_text)[0]
                elif re.search('\"trinnyTipVideo\"\:\{\"url\"\:\"([^>]*?)\"\,',variant_response_text):
                    vatiant_video_url = re.findall('\"trinnyTipVideo\"\:\{\"url\"\:\"([^>]*?)\"\,',variant_response_text)[0]
                variant_dict['has_video'] = False
                variant_dict['video'] = []
                if vatiant_video_url:
                    variant_dict['has_video'] = True
                    vatiant_video_url = codecs.decode(vatiant_video_url,'unicode-escape')
                    if 'http' in vatiant_video_url:
                        variant_dict['video'] = [vatiant_video_url]
                    else:
                        variant_dict['video'] = [f'https{vatiant_video_url}']
                variant_dict['data_color'] = title
                variant_dict['data_size'] = None
                in_stock = variant_response_selector.xpath('//button[@data-test-id="buy-button"]/@disabled')
                variant_dict['in-stock'] = True
                if 'Out of Stock' in in_stock:
                    variant_dict['in-stock'] = False
                variant_dict['price_before'] = None
                variant_dict['promo_label'] = None
                if variant_dict['price'] and variant_dict['image_url']:
                    prices.append(variant_dict)
        
        item['prices'] = prices
        
        item["description"] = None
        if '?variant' in response.url:
            review_split = (response.url).split('?variant')[0].split('/')[-1]    
        else:
            review_split = (response.url).split('/')[-1]
        review_post_url = f"https://staticw2.yotpo.com/batch/bPYeSBCthuAglEkGKVJOkg2ytGAqvExKfsK1822f/{review_split}"
        payload=f'methods=%5B%7B%22method%22%3A%22main_widget%22%2C%22params%22%3A%7B%22pid%22%3A%22{review_split}%22%2C%22order_metadata_fields%22%3A%7B%7D%2C%22widget_product_id%22%3A%22{review_split}%22%7D%7D%5D&app_key=bPYeSBCthuAglEkGKVJOkg2ytGAqvExKfsK1822f&is_mobile=false&widget_version=2022-10-31_10-11-02'
        post_response = await self.requests_process_post(review_post_url,payload)
        response_text = re.sub(r'\\"',"",post_response.text) 
        response = Selector(text=response_text)
        review_list = []
        review_list = self.review(response,review_list)
        count_total,per_page='',''
        if re.search(r'data\-total\=([\d]+)\s+',response_text):
            count_total =  re.findall(r'data\-total\=([\d]+)\s+',response_text)[0]
        if re.search(r'data\-per\-page\=([\d]+)\s+',response_text):
            per_page = re.findall(r'data\-per\-page\=([\d]+)\s+',response_text)[0]
        if count_total and per_page:
            for count in range(2,round(int(count_total)/int(per_page))+1):
                payload = f'methods=%5B%7B%22method%22%3A%22reviews%22%2C%22params%22%3A%7B%22pid%22%3A%22{review_split}%22%2C%22order_metadata_fields%22%3A%7B%7D%2C%22widget_product_id%22%3A%22{review_split}%22%2C%22data_source%22%3A%22default%22%2C%22page%22%3A{count}%2C%22host-widget%22%3A%22main_widget%22%7D%7D%5D&app_key=bPYeSBCthuAglEkGKVJOkg2ytGAqvExKfsK1822f&is_mobile=false&widget_version=2022-10-31_10-11-02'
                post_response = await self.requests_process_post(review_post_url,payload)
                response_text = re.sub(r'\\"',"",post_response.text) 
                response = Selector(text=response_text)
                review_list = self.review(response,review_list)
        item["reviews"] = review_list
        yield item
    
    def review(self,response,review_list):
        for review_xpath in response.xpath("//div[@class='yotpo-review']"):
            review_dict = {}
            review_dict['user'] = review_xpath.xpath(".//div[@class='yotpo-header-element'][1]/span[@class='y-label']/text()").get()
            date = review_xpath.xpath(".//div[@class='yotpo-header-element'][2]/span[@class='y-label']/text()").get()
            review_dict['date']=dateparser.parse(str(date)).strftime('%Y-%m-%d')
            review_dict['stars'] = review_xpath.xpath(".//div[@class='yotpo-review-stars']/span[@class='sr-only']/text()").get("").replace(' star rating','')
            review_title = review_xpath.xpath(".//div[@class='content-title']//text()").get("").strip()
            review_body = review_xpath.xpath(".//div[@class='content-review']/text()").get("").strip()
            review_dict['review'] = f"{review_title} {review_body}"
            if review_dict['user'] is not None:
                review_list.append(review_dict)
        return review_list
    async def requests_process(self,url):
        request = scrapy.Request(url)
        response = await self.crawler.engine.download(request, self)
        return response
    async def requests_process_post(self,url,payload):
        request = scrapy.Request(url,method='POST',body=payload)
        response = await self.crawler.engine.download(request, self)
        return response


            
