import re
import os
import json
import scrapy
import urllib.parse
import datetime as currentdatetime
from datetime import datetime
import traceback2 as traceback
from dotenv import load_dotenv

load_dotenv()
class BobbieProductUKSpider(scrapy.Spider):
    name = "bobbi_brown_uk_pdp"
    CURRENT_DATETIME = currentdatetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    ROTATING_PROXY = os.getenv("ROTATING_PROXY")
    custom_settings={
                    'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/bobbi_brown_uk/%(name)s_{DATE}.json": {"format": "json",}},
                     'ROTATING_PROXY_LIST' : [ROTATING_PROXY]
                     }
    async def request_process(self, url):
        request = scrapy.Request(url)
        response = await self.crawler.engine.download(request, self)
        return response

    def regex_parse(self,pattern,text):
        if re.search(pattern,text,re.I):
            data = re.findall(pattern,text,re.I)
            return data[0]
        else:
            return ''

    def clean_text(self, text):
        text = re.sub(r'<.*?>','',str(text))
        return text
    
    def review_write(self,review_response,review_list):
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
        
    async def sub_data_extraction(self,category_name,category_url,main_category,main_category_url,sub_category,sub_cat_url,response,product_url,product_list):
        
        try:
            item = {}
            parse_resp = await self.request_process(product_url)
            if re.search(r'\"Product\"\,\"name\"\:\"(.*?)\"',parse_resp.text):
                sku_id = self.regex_parse(r'\"sku\"\:\"(.*?)\"',parse_resp.text)
                item['name'] = self.regex_parse(r'\"Product\"\,\"name\"\:\"(.*?)\"',parse_resp.text)
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
                if sub_category and sub_cat_url:
                    sub_cat = {}
                    sub_cat['name'] = sub_category
                    sub_cat['url'] = sub_cat_url
                    category_crumb.append(sub_cat)
                item['category_crumb'] = category_crumb
                item['master_product_id'] = self.regex_parse(r'product\_base\_id\"\:\[(.*?)\]',parse_resp.text)
                item['gtin'] = None
                if re.search(r'\"UPC_CODE\"\:\"([^>]*?)\"',parse_resp.text):
                    gtin = re.findall(r'\"UPC_CODE\"\:\"([^>]*?)\"',parse_resp.text)[0]
                    item["gtin"] = gtin if gtin else None
                item['brand'] = 'Bobbi Brown'
                sub_title = self.regex_parse(r'product\_short\_desc\"\:\[\"(.*?)\"\]',parse_resp.text)
                item['subtitle'] = None
                if sub_title:
                    item['subtitle'] = self.clean_text(sub_title.encode('raw_unicode_escape').decode('unicode_escape'))
                
                item['image_url'] = [response.urljoin(e) for e in parse_resp.xpath('//img[@data-swap-image-field="IMAGE_LARGE_COMBINED"]/@src').extract()]
                video_url_id = parse_resp.xpath('//div[contains(@class,"product-how-to-video__container")]/@data-youtube-id').get("")
                item['has_video'] = False
                item['video'] = []
                if video_url_id:
                    item['has_video'] = True
                    video_url_id = video_url_id.split('&')[0]
                    item['video'] = [f'https://www.youtube.com/embed/{video_url_id}']
                
                stock = ''
                if re.findall(r'\"availability\"\:\"http\:\/\/schema\.org\/(.*?)\"\,\"sku\"\:\"(.*?)\"',parse_resp.text,re.DOTALL):
                    stock_dump = re.findall(r'\"availability\"\:\"http\:\/\/schema\.org\/(.*?)\"\,\"sku\"\:\"(.*?)\"',parse_resp.text,re.DOTALL)
                    for stock in stock_dump:
                        if stock[1] == sku_id:
                            stock = stock[0]
                            break
                        else:
                            continue
                if stock == 'InStock':
                    item['in-stock'] = True
                else:
                    item['in-stock'] = False
                item['price'] = self.regex_parse(r'product\_price\"\:\[\"(.*?)\"\]',parse_resp.text)
                if item['price']:
                    item['price'] = '£' + "%.2f" %float(item['price'])
                else:
                    item['price'] = None
                item['price_before'] = self.regex_parse(r'product\_was\_price\"\:\[\"(.*?)\"]',parse_resp.text)
                if item['price_before']:
                    item['price_before'] = '£' + "%.2f" %float(item['price_before'])
                else:
                    item['price_before'] = None
                if item['price_before']:
                    item['promo_label'] = parse_resp.xpath('//div[@class="product-full__offer-text"]/p/span/text()').get()
                else:
                    item['promo_label'] = None
                json_block = re.findall(r'application\/json\"\s*id\=\"page_data\">([\w\W]*?)<\/script>',parse_resp.text,re.DOTALL)[0]
                json_data = json.loads(json_block)
                variation_lt = []
                if json_data['catalog-spp']['products'][0]['skus'][0]['shadeMenuLabel']:
                    for block in json_data['catalog-spp']['products'][0]['skus']:
                        variation_dict = {}
                        product_id = block.get('SKU_ID','')
                        variation_dict['gtin'] = block.get('UPC_CODE')
                        variation_dict['sku_id'] = int(re.findall(r'\d+',product_id,re.DOTALL)[0])
                        data_size = (block.get('PRODUCT_SIZE',''))
                        variation_dict['data_size'] = None
                        if data_size:
                            data_size = data_size.strip()
                            if data_size:
                                if '\\/' in data_size:
                                    data_size = data_size.split('\\/')
                                else:
                                    data_size = data_size.split('/')
                                if len(data_size) > 1:
                                    for size in data_size:
                                        if 'g' in size or 'ml' in size:
                                            variation_dict['data_size'] = re.sub(r'\s+','',size)
                                            if re.search(r'^\.\d+',variation_dict['data_size'],re.DOTALL):
                                                variation_dict['data_size'] = '0' + variation_dict['data_size']
                                            else:
                                                variation_dict['data_size'] = variation_dict['data_size']
                                        else:
                                            continue
                                else:
                                    variation_dict['data_size'] = re.sub(r'\s+','',data_size[0])
                                    if re.search(r'^\.\d+',variation_dict['data_size'],re.DOTALL):
                                        variation_dict['data_size'] = '0' + variation_dict['data_size']
                                    else:
                                        variation_dict['data_size'] = variation_dict['data_size']
                            else:
                                variation_dict['data_size'] = None
                        else:
                            variation_dict['data_size'] = None        
                        data_color = block.get('shadeMenuLabel','')
                        if not re.search(r'\d+ml|\d+g|\d+\s*ml|\d+\s*g',str(data_color),re.DOTALL):
                            variation_dict['data_color'] = data_color
                        else:
                            variation_dict['data_color'] = None
                        
                        variant_url = block.get('shadeMenuLabel','')
                        variant_url = re.sub(r'\s','_',str(variant_url))
                        if '(' in variant_url:
                            if '/' in variant_url:
                                variant_url = variant_url.split('/')[0] + '/'
                                variant_url = urllib.parse.quote(variant_url, safe="")
                            else:
                                variant_url = variant_url.split('-')[0] + '-'
                                variant_url = urllib.parse.quote(variant_url, safe="")
                        else:
                            variant_url = variant_url
                        variation_dict['variant_url'] = item['url'] + '#/shade/' + variant_url
                        variation_dict['image_url'] = [response.urljoin(e) for e in block.get('IMAGE_LARGE_COMBINED','')]
                        price = block.get('formattedTaxedPrice','')
                        if item['price_before']:
                            price_before = block.get('formattedPrice2','')
                            if price != price_before:
                                variation_dict['price'] = price.strip()
                            else:
                                variation_dict['price'] = price.strip()
                                
                        else:
                            variation_dict['price'] = price.strip()
                        sku_ids = block.get('PRODUCT_CODE','')
                        for stocks in stock_dump:
                            if stocks[1] == sku_ids:
                                stock = stocks[0]
                                break
                            else:
                                continue
                        if stock == 'InStock':
                            variation_dict['in-stock'] = True
                        else:
                            variation_dict['in-stock'] = False
                        variation_dict['price_before'] = None
                        variation_dict['promo_label'] = None
                        variation_lt.append(variation_dict)
                    item['prices'] = variation_lt
                elif re.search(r'product\_size\"\:\[\"(.*?)\"\]',parse_resp.text,re.I):
                    variant_dict = {}
                    sku_id = self.regex_parse(r'product_sku\"\:\[\"SKU(.*?)\"\]',parse_resp.text)
                    variant_dict['sku_id'] = int(sku_id)
                    variant_dict['gtin'] = None
                    if re.search(r'\"UPC_CODE\"\:\"([^>]*?)\"',parse_resp.text):
                        variant_gtin = re.findall(r'\"UPC_CODE\"\:\"([^>]*?)\"',parse_resp.text)[0]
                        variant_dict["gtin"] = variant_gtin if variant_gtin else None
                    variant_dict['data_size'] = None
                    if not re.search(r'\"product\_size\"\:\[\"Valeur',parse_resp.text,re.I):
                        data_size = (self.regex_parse(r'product\_size\"\:\[\"(.*?)\"\]',parse_resp.text)).encode('raw_unicode_escape').decode('unicode_escape').strip()
                        data_size = data_size.split('\\/')
                        for size in data_size:
                            if 'g' in size or 'ml' in size:
                                variant_dict['data_size'] = re.sub(r'\s+','',size)
                                if re.search(r'^\.\d+',variant_dict['data_size'],re.DOTALL):
                                    variant_dict['data_size'] = '0' + variant_dict['data_size']
                                else:
                                    variant_dict['data_size'] = variant_dict['data_size']
                            else:
                                continue
                        
                    else:
                        variant_dict['data_size'] = None
                    variant_dict['data_color'] = None
                    variant_dict['variant_url'] = product_url
                    variant_dict['image_url'] = item['image_url']
                    variant_dict['price'] = item['price']
                    variant_dict['in-stock'] = item['in-stock']
                    variant_dict['price_before'] = None
                    variant_dict['promo_label'] = None
                    variation_lt.append(variant_dict)
                    item['prices'] = variation_lt
                else:
                    item['prices'] = []
                description = ' '.join(parse_resp.xpath('//div[@class="product-full__data-details"]/section//text()').getall()).strip()
                description = re.sub(r'\s+',' ',description)
                item['description'] = self.clean_text(description.encode('raw_unicode_escape').decode('unicode_escape')).replace('\\/','/')
                if re.search(r'\"power_reviews\"\:\{\"api_key\"\:\"([^>]*?)\"',parse_resp.text):
                    api_key = re.findall(r'\"power_reviews\"\:\{\"api_key\"\:\"([^>]*?)\"',parse_resp.text)[0]
                if re.search(r'\"power_reviews\"[^>]*?\"merchant_id\"\:\"([^>]*?)\"',parse_resp.text):
                    merchant_id = re.findall(r'\"power_reviews\"[^>]*?\"merchant_id\"\:\"([^>]*?)\"',parse_resp.text)[0]
                if re.search('\"product_base_id\"\:\[([^>]*?)\]\,',parse_resp.text):
                    product_base_id = re.findall('\"product_base_id\"\:\[([^>]*?)\]\,',parse_resp.text)[0]
                review_url = f'https://display.powerreviews.com/m/{merchant_id}/l/all/product/{product_base_id}/reviews?apikey={api_key}&_noconfig=true&page_locale=en_GB'
                review_resp = await self.request_process(review_url)
                review_list = []
                review_list = self.review_write(review_resp,review_list)
                page_total = review_resp.json().get('paging',{}).get('pages_total','')
                for _ in range(2,page_total+1):
                    nextpage_url =  review_resp.json().get('paging',{}).get('next_page_url','')
                    nextpage_url = f"https://display.powerreviews.com{nextpage_url}&apikey={api_key}"
                    next_review_resp = await self.request_process(nextpage_url)
                    review_list = self.review_write(next_review_resp,review_list)
                item['reviews'] = review_list
                product_list.append(item)
            else:
                with open('error_product_bobbie_brown_uk.txt','a') as f: f.write(product_url+'\n')
        except Exception as e:
            print(traceback.format_exc())


    async def data_extraction(self,sub_cat_url,category_name,category_url,main_category,main_category_url,sub_category,response):
        try:
            sub_cat_parse = await self.request_process(sub_cat_url)
            if not re.search(r'\"Product\"\,\"name\"\:\"(.*?)\"',sub_cat_parse.text):
                product_urls = sub_cat_parse.xpath('//div[@class="product-brief__headline"]/a/@href|//div[@class="product-brief__image-container-top js-product-brief-image-container-top"]/a[1]/@href').getall()
                product_list = []
                for product_url in product_urls:
                    product_url = response.urljoin(product_url)
                    sub_data = await self.sub_data_extraction(category_name,category_url,main_category,main_category_url,sub_category,sub_cat_url,response,product_url,product_list)                    
                return product_list
            else:
                product_list = []
                product_url = sub_cat_url
                sub_data = await self.sub_data_extraction(category_name,category_url,main_category,main_category_url,sub_category,sub_cat_url,response,product_url,product_list)
                return product_list
        except Exception as e:
            print(e)

    def start_requests(self):
        urls = ['https://www.bobbibrown.co.uk/products/2321/makeup']
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    async def parse(self, response):
        try:
            spider_name = BobbieProductUKSpider.name.replace('pdp','categories')
            current_date = datetime.now().strftime("%Y-%m-%d")
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
                            data_list = await self.data_extraction(sub_cat_url,category_name,category_url,main_category,main_category_url,sub_category,response)
                            for data in data_list:
                                yield data
                    else:
                        main_category = main_sub_cat.get('name','').title()
                        main_category_url = main_sub_cat.get('url','')
                        sub_category = ''
                        sub_cat_url = main_sub_cat.get('url','')  
                        data_list = await self.data_extraction(sub_cat_url,category_name,category_url,main_category,main_category_url,sub_category,response)
                        for data in data_list:
                            yield data
        except Exception as e:
            print(traceback.format_exc())
            
