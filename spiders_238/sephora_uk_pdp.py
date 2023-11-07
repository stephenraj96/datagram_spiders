import re
import os
import json
import scrapy
import datetime
from pathlib import Path
from dotenv import load_dotenv

try:
    load_dotenv()
except:
    env_path = Path(".env")
    load_dotenv(dotenv_path=env_path)
class SephoraUKPdpSpider(scrapy.Spider):
    name = 'sephora_uk_pdp'
    start_urls = ['https://www.sephora.co.uk/']
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    
    ROTATING_PROXY = os.getenv("ROTATING_PROXY")
    custom_settings={
        'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/sephora_uk/%(name)s_{DATE}.json": {"format": "json",}},
    
    "CONCURRENT_REQUESTS":100,
    'ROTATING_PROXY_LIST' : [ROTATING_PROXY],
    "HTTPCACHE_ENABLED" : True,
    "HTTPCACHE_DIR" : 'httpcache',
    'HTTPCACHE_EXPIRATION_SECS':172800,
    "HTTPCACHE_IGNORE_HTTP_CODES":[502],
    "HTTPCACHE_STORAGE" : "scrapy.extensions.httpcache.FilesystemCacheStorage",
    }
    def parse(self, response):
        spider_name = SephoraUKPdpSpider.name.replace('pdp','categories')
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
        if response.xpath('//div[@class="eba-component eba-product-listing"]/div[contains(@class,"Product")]/a/@href'):
            for product_list_xpath in response.xpath('//div[@class="eba-component eba-product-listing"]/div[contains(@class,"Product")]/a'):
                product_url =  product_list_xpath.xpath('./@href').get()
                yield scrapy.Request(url=response.urljoin(product_url),callback=self.product_detail,dont_filter=True,cb_kwargs={"category_name":category_name,"category_url":category_url,"main_category":main_category,"main_category_url":main_category_url,"sub_category":sub_category,"sub_cat_url":sub_cat_url})
        nexpage_url = response.xpath('//div[@class="loadMore loadMoreBottom"]/a/@href').get()
        if nexpage_url:
           yield scrapy.Request(url=response.urljoin(nexpage_url),callback=self.parse_list,dont_filter=True,cb_kwargs={"category_name":category_name,"category_url":category_url,"main_category":main_category,"main_category_url":main_category_url,"sub_category":sub_category,"sub_cat_url":sub_cat_url})
    async def product_detail(self,response,category_name,category_url,main_category,main_category_url,sub_category,sub_cat_url):
        item = {}
        item['url'] = response.url
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
        if response.xpath('//h1/span[@class="pdp-product-brand-name"]/text()').get('').strip():
            item['name'] = response.xpath('//h1/span[@class="pdp-product-brand-name"]/text()').get('').strip()
            item['subtitle'] = None
            image_url_list = list(set(response.xpath("//div[contains(@class,'productpage-gallery')]/div/a/@href|//div[contains(@class,'productpage-image')]/img/@src").getall()))
            image_urls = [ url.replace(' ','%20') for url in image_url_list if not 'youtube' in url ]
            video_url = [ url for url in image_url_list if 'youtube' in url ]
            video_url_player = [ url for url in image_url_list if 'player.vimeo.com' in url ]
            if video_url_player:
                video_url.extend(video_url_player)
            item['image_url'] = image_urls
            item['has_video'] = False
            item["video"] = []
            if video_url:
                item['has_video'] = True
                video_urls = []
                for video_url_http in video_url:
                    if not 'http' in video_url_http:
                        video_url_http = f'https:{video_url_http}'
                        video_urls.append(video_url_http)
                    else:
                        video_urls.append(video_url_http)
                item["video"] = video_urls
            item['brand'] = None
            if re.search(r'<script\s*type\=\"application\/ld\+json\"[^>]*?>[^>]*?\"brand\"\:\s*\"([^>]*?)\"[^>]*?<\/script>',response.text):
                item['brand'] = re.findall(r'<script\s*type\=\"application\/ld\+json\"[^>]*?>[^>]*?\"brand\"\:\s*\"([^>]*?)\"[^>]*?<\/script>',response.text)[0] 
            item['master_product_id'] = None
            item['gtin'] = None
            if  re.search('\"data\"\:\[\"([^>]*?)\"\]',response.text):
                item['master_product_id'] = re.findall('\"data\"\:\[\"([^>]*?)\"\]',response.text)[0]
            if re.search(r'data\-flix\-ean\=\"([^>]*?)\"',response.text):
                gtin = re.findall(r'data\-flix\-ean\=\"([^>]*?)\"',response.text)[0]
                item['gtin'] = gtin if gtin != "" else None
                
            in_stock = response.xpath('//span[@class="product-options-stock u-error"]/text()|//div[contains(@class,"stock-level")]/text()|//span[@class="product-options-stock"]/text()|//div[@class="sub-products"]/span[@class="option selected"]/span/span[contains(@class,"priceStock")]/span[2]/text()').get("")
            item['in-stock'] = True if 'in stock' in in_stock.lower().strip() else False
            
            item['price'] = None
            if response.xpath('//p[@class="price-info"]/span/span/text()'):
                item['price'] = ''.join(response.xpath('//p[@class="price-info"]/span/span/text()').getall()).strip()
            price_before = response.xpath('//p[@class="price-info"]/span/span[@class="Price-details"]/span[@class="rrp"]/text()').get('').strip()
            item['price_before'] = price_before if price_before else None
            promo_label = response.xpath('//div[@class="discountWrapper"]/span/text()').get('').strip()
            item['promo_label'] = promo_label if promo_label else None
            prices = []
            for variant_block in response.xpath('//div[@class="product-detail-information productpage-buy"]//div[@class="sub-products"]/span'):
                variant_dict = {}
                variant_dict['sku_id'] = variant_block.xpath('./@data-sku').get()
                variant_dict['gtin'] = item['gtin']
                in_stock = variant_block.xpath('./span/span[@class="priceStock "]/span[2]/text()').get('')
                variant_dict['in-stock'] = True if 'in stock' in in_stock.lower().strip() else False
                variant_dict['image_url'] = image_urls
                variant_dict['has_video'] = item['has_video']
                variant_dict['video'] = item["video"]
                variant_price = variant_block.xpath('./span/span[contains(@class,"priceStock")]/span[1]/text()').get()
                variant_dict['price'] = variant_price.strip() if variant_price else None
                variant_dict['price_before'] = item['price_before']
                variant_dict['promo_label'] = item['promo_label']
                variant_dict['variant_url'] = response.url
                variant_dict['data_color'] = variant_block.xpath('./span/span[@class="name"]/text()').get()
                variant_dict['data_size'] = None
                prices.append(variant_dict)
            item['prices'] = prices
            description = ' '.join(response.xpath('//section[@id="information"]/div//text()').getall())
            description = re.sub(r'\s+',' ',description)
            item['description'] = description.strip() if description else None
            yield item
            
    async def requests_process(self,url):
        request = scrapy.Request(url)
        response = await self.crawler.engine.download(request, self)
        return response
    

            
