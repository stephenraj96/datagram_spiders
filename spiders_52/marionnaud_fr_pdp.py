import scrapy
import json
import re
import datetime
import os
import redis
redis_connection = redis.Redis(host='localhost', port=6379, db=0)

class MarionnaudFrPdpSpider(scrapy.Spider):
    name = "marionnaud_fr_pdp"
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    redis_connection = redis.Redis(host='localhost', port=6379, db=0)
    ROTATING_PROXY = os.getenv("ROTATING_PROXY_FR")
    custom_settings={
    # 'DOWNLOADER_CLIENTCONTEXTFACTORY':'datagram.context.CustomContextFactory', 
    # 'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/marionnaud_fr/%(name)s_{DATE}.json": {"format": "json",}},
    'ROTATING_PROXY_LIST' : [ROTATING_PROXY],
    'CONCURRENT_REQUESTS' : 4,
    # "HTTPCACHE_ENABLED" : True,
    # "HTTPCACHE_DIR" : 'httpcache',
    # 'HTTPCACHE_EXPIRATION_SECS':86400,
    # "HTTPCACHE_IGNORE_HTTP_CODES":[502],
    # "HTTPCACHE_STORAGE" : "scrapy.extensions.httpcache.FilesystemCacheStorage",
    # "DUPEFILTER_DEBUG": True,
    # "DUPEFILTER_CLASS": "scrapy.dupefilters.BaseDupeFilter",
    # 'handle_httpstatus_all': True
    # 'HTTPCACHE_EXPIRATION_SECS':72000,
    }
    headers = {
  'authority': 'www.marionnaud.fr',
  'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
  'accept-language': 'en-US,en;q=0.9',
  'cache-control': 'no-cache',
  'pragma': 'no-cache',
  'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Windows"',
  'sec-fetch-dest': 'document',
  'sec-fetch-mode': 'navigate',
  'sec-fetch-site': 'none',
  'sec-fetch-user': '?1',
  'upgrade-insecure-requests': '1',
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
}
    async def request_process(self, url ,headers , payload,method):
        
        if method == "POST":
            request = scrapy.Request(url,headers=headers,body=payload,method="POST",dont_filter=True,meta={'proxy': self.ROTATING_PROXY})
        elif method == "GET" and headers:
            request = scrapy.Request(url,headers=headers,body=payload,method="GET",dont_filter=True,meta={'proxy': self.ROTATING_PROXY})
        else:
            request = scrapy.Request(url,dont_filter=True,meta={'proxy': self.ROTATING_PROXY})
        response = await self.crawler.engine.download(request, self)
        return response 
    
    def start_requests(self):
        spider_name = MarionnaudFrPdpSpider.name.replace('pdp','categories')
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        category_path = os.getcwd() + f'/exports/{spider_name}/{spider_name}_{current_date}.json'
        with open(category_path,'r',encoding='utf-8-sig') as f:
            json_file = f.read()
        
        category_names = json.loads(json_file)
        category_name = category_names[0].get('name','').title()
        category_url = category_names[0].get('url','')
        for main_sub_cat in category_names[0]['category_crumb']:
            main_category = main_sub_cat.get('name','').title()
            main_category_url = main_sub_cat.get('url','')
            for sub_cat in main_sub_cat['category_crumb']:
                sub_category = sub_cat.get('name','').title()
                sub_cat_url = sub_cat.get('url','')
                for sub_sub_cat in sub_cat.get('category_crumb',''):
                    sub_sub_category = sub_sub_cat.get('name','').title()
                    sub_sub_cat_url = sub_sub_cat.get('url','')
                    # api_token = f"http://api.scrape.do?token=99ea2d2dc2e94fe193ced5d6fece0493bd9b9ff2b47&url={sub_sub_cat_url}"
                    yield scrapy.Request(sub_sub_cat_url,callback=self.parse,dont_filter=True,meta={'proxy': self.ROTATING_PROXY},cb_kwargs={"category_name":category_name,"category_url":category_url,"main_category":main_category,"main_category_url":main_category_url,"sub_cat_url":sub_cat_url,"sub_category":sub_category,"sub_sub_category":sub_sub_category,"sub_sub_cat_url":sub_sub_cat_url})

    def parse(self,response,category_name,category_url,main_category,main_category_url,sub_category,sub_cat_url,sub_sub_cat_url,sub_sub_category):
        try:
            product_urls = response.xpath('//div[@class=" productMainLink "]/a/@href').getall()
            list_comp = []
            count = 0
            for product_url in product_urls:
                product_url1 = response.urljoin(product_url)
                sub_cat_url =product_url1
                item={}
                category_crumb = []
                main_cat = {}
                main_cat['name'] = category_name
                main_cat['url'] = category_url
                category_crumb.append(main_cat)
                sub_main_cat = {}
                sub_main_cat['name'] = main_category
                sub_main_cat['url'] = main_category_url
                category_crumb.append(sub_main_cat)
                sub_sub_main_cat = {}
                sub_sub_main_cat['name'] = sub_category
                sub_sub_main_cat['url'] = sub_cat_url
                category_crumb.append(sub_sub_main_cat)
                if sub_sub_category and sub_sub_cat_url:
                    sub_sub_cat = {}
                    sub_sub_cat['name'] = sub_sub_category
                    sub_sub_cat['url'] = sub_sub_cat_url
                    category_crumb.append(sub_sub_cat)
                item['category_crumb'] = category_crumb
                item["url"] = product_url1
                print(f'{count}{product_url1}')
                list_comp.append(item)
                json_data = json.dumps(list_comp)
                count+=1
                print(f"{count}{json_data}")
                # # yield item
                # breakpoint()
                # redis_connection.set("mari_link_collection_total", json_data)
                # print(item)
                # with open("sample_1234.json", "a+") as json_file:json.dump(list_comp, json_file)
                # file_value = self.redis_connection.lpush('link_collection',sub_cat_url)
                
                # yield scrapy.Request(sub_cat_url,callback=self.sub_data_extraction,dont_filter=True,meta={'proxy': self.ROTATING_PROXY},cb_kwargs={"sub_cat_url":sub_cat_url,"category_name":category_name,"category_url":category_url,"main_category":main_category,"main_category_url":main_category_url,"sub_category":sub_category,"sub_sub_category":sub_sub_category,"sub_sub_cat_url":sub_sub_cat_url})
            next_page = response.xpath('//link[@rel="next"]/@href').get('')
            if next_page:
                yield scrapy.Request(next_page,callback=self.parse,meta={'proxy': self.ROTATING_PROXY},cb_kwargs={"category_name":category_name,"category_url":category_url,"main_category":main_category,"main_category_url":main_category_url,"sub_cat_url":sub_cat_url,"sub_category":sub_category,"sub_sub_category":sub_sub_category,"sub_sub_cat_url":sub_sub_cat_url})
        except Exception as e:
            print(e)
        # json_string_from_redis = redis_connection.get("mari_tesing_json")
    # async def sub_data_extraction(self,response,category_name,category_url,main_category,main_category_url,sub_category,sub_cat_url,sub_sub_category,sub_sub_cat_url):
    # # async def sub_data_extraction(self,response,category_name,category_url,main_category,main_category_url,sub_sub_category,sub_sub_cat_url):
    #     try: 
           
    #         item = {}
    #         item['url'] = response.url
    #         category_crumb = []
    #         main_cat = {}
    #         main_cat['name'] = category_name
    #         main_cat['url'] = category_url
    #         category_crumb.append(main_cat)
    #         sub_main_cat = {}
    #         sub_main_cat['name'] = main_category
    #         sub_main_cat['url'] = main_category_url
    #         category_crumb.append(sub_main_cat)
    #         sub_sub_main_cat = {}
    #         sub_sub_main_cat['name'] = sub_category
    #         sub_sub_main_cat['url'] = sub_cat_url
    #         category_crumb.append(sub_sub_main_cat)
    #         if sub_sub_category and sub_sub_cat_url:
    #             sub_sub_cat = {}
    #             sub_sub_cat['name'] = sub_sub_category
    #             sub_sub_cat['url'] = sub_sub_cat_url
    #             category_crumb.append(sub_sub_cat)
    #         item['category_crumb'] = category_crumb
    #         product_name = response.xpath('//span[@class="producRangeName"]/text()').get('')
    #         if product_name:
    #             item["name"] =  product_name
    #         else:
    #             item["name"] = None
    #         sub_title = response.xpath('//span[@class="productName"]/text()').get('')
    #         item['subtitle'] = None
    #         if sub_title:
    #             item['subtitle'] = sub_title
    #         image_url =  response.xpath('//div[@class="gallery__container"]//@data-zoom-image').getall() 
    #         item['image_url'] = [f'https://www.marionnaud.fr/{image_url}' for image_url in image_url] 
    #         item['has_video'] = ''
    #         item["video"] = response.xpath("//div[@class='productInformationSection']//iframe/@src").getall()
    #         if item["video"] != []:
    #             item['has_video'] = True
    #         else:
    #             item['has_video'] = False
    #         item['brand'] = response.xpath('//span[@class="productBrandName"]/text()').get('')
    #         item['master_product_id'] = response.xpath('//input[@class="GTMbaseProdudct"]/@value').get('')
    #         if re.search(r'gtin_ean\]\=(.*?)&',response.text):
    #             item['gtin'] = re.findall(r'gtin_ean\]\=(.*?)&',response.text)[0]
    #         elif re.search(r'mpn\"\:\s*\"(.*?)\"',response.text):
    #             item['gtin'] = re.findall(r'mpn\"\:\s*\"(.*?)\"',response.text)[0]
    #         else:
    #             item['gtin'] = None
    #         add_to_cart = response.xpath('//div[@class="addtoCart_Onload"]/button[@id="addToCartButton"]')
    #         response.xpath("//div[@class='productInformationSection']//iframe/@src").getall()
    #         if add_to_cart:
    #             item["in-stock"] = True
    #         else:
    #             item["in-stock"] = False
    #         link = response.url
    #         link_promo = link.split("/p/")[-1]
    #         promo = f"https://www.marionnaud.fr/p/productVariantPromotions?code={link_promo}"
    #         payload = None
    #         method = 'GET'
    #         promo_response = await self.request_process(promo,self.headers,payload,method)
    #         price_before = ''.join(response.xpath('//div[@class="markdownPrice priceformat"]//text()').getall()).strip()
    #         after_price = ''.join(response.xpath('//div[@class="finalPrice"]//text()').getall()).strip()
    #         json_method = json.loads(promo_response.text)
    #         promo_image_link = json_method.get('normalPromotion','')
    #         if promo_image_link:
    #             promo_image_link = promo_image_link.get("imageUrl","")
    #             if "3P2" in promo_image_link:
    #                 price_replace = float(after_price.replace("€","."))
    #                 item["pirce"] = "€"+str(price_replace*2)
    #                 item["price_before"] = "€"+str(price_replace*3)
    #                 item["promo_label"] = "2=3"
    #             else:    
    #                 promo_label_check = promo_image_link.get("description","")
    #                 # if promo_label_check:
    #                 #     promo_labels_product = re.findall('\:\s*(.*?%)',promo_label_check)[0]
    #                 if price_before != '':
    #                     price = after_price.replace('€','.')
    #                     item['price'] = '€'+ price
    #                     price_before= price_before.replace('€','.')
    #                     item['price_before'] = '€'+ price_before
    #                     item["promo_label"] = promo_label_check
    #                 elif after_price:
    #                     price = after_price.replace('€','.')
    #                     item['price'] = '€'+ price
    #                     item["price_before"] = None
    #                     item["promo_label"] = promo_label_check
    #                 else:
    #                     item["price_before"] = None
    #                     item["promo_label"] = None
    #         else:
    #             if price_before != '':
    #                     price = after_price.replace('€','.')
    #                     item['price'] = '€'+ price
    #                     price_before= price_before.replace('€','.')
    #                     item['price_before'] = '€'+ price_before
    #                     item["promo_label"] = None
    #             else:
    #                 price = after_price.replace('€','.')
    #                 item['price'] = '€'+ price
    #                 item["price_before"] = None
    #                 item["promo_label"] = None
    #         variant_links= response.xpath('//div[@id="buttonClickshow"]/ul/li/a/@href').getall()
    #         color_data = response.xpath('//span[@class="singleTextColor"]/text()').get('').strip()
    #         if '' != color_data:
    #             data_colour_link= response.xpath('//div[@class="grid__brick_outer"]/a/@href').getall()
    #             variant_links.extend(data_colour_link)
    #         no_text = response.xpath('//div[@id="productVariantDisplay"]//a/span/text()').get('').strip()
    #         if '' != no_text:
    #             data_size_link = response.xpath('//div[@id="productVariantDisplay"]//a/@href').getall()
    #             variant_links.extend(data_size_link) 
    #         variation_lt = [] 
    #         if variant_links:
    #             variant_links = ['https://www.marionnaud.fr/'+ items for items in variant_links]
    #             for variant_link in variant_links:
                
    #                 headers = {
    #                 "Authority":"www.marionnaud.fr",
    #                 "Method":"GET",
    #                 "Path":f"{variant_link}",
    #                 "Scheme":"https",
    #                 "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    #                 "Accept-Encoding":"gzip, deflate, br",
    #                 "Accept-Language":"en-US,en;q=0.9",
    #                 "Cache-Control":"no-cache",
    #                 "Pragma":"no-cache",
    #                 "Sec-Ch-Ua":'"Google Chrome";v="113", "Chromium";v="113", "Not-A.Brand";v="24"',
    #                 "Sec-Ch-Ua-Mobile":"?0",
    #                 "Sec-Ch-Ua-Platform":"Windows",
    #                 "Sec-Fetch-Dest":"document",
    #                 "Sec-Fetch-Mode":"navigate",
    #                 "Sec-Fetch-Site":"none",
    #                 "Sec-Fetch-User":"?1",
    #                 "Upgrade-Insecure-Requests":"1",
    #                 "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"

    #             }
    #                 payload = None
    #                 method = 'GET'
    #                 variant_response = await self.request_process(variant_link,headers,payload,method)
    #                 promo_label_varint = variant_link.split('/p/')[-1]
    #                 variation_dict = {}
    #                 if re.search(r'gtin_ean\]\=(.*?)&',variant_response.text):
    #                     variation_dict['gtin'] = re.findall(r'gtin_ean\]\=(.*?)&',variant_response.text)[0]
    #                 elif re.search(r'mpn\"\:\s*\"(.*?)\"',variant_response.text):
    #                     variation_dict['gtin'] = re.findall(r'mpn\"\:\s*\"(.*?)\"',variant_response.text)[0]
    #                 else:
    #                     variation_dict['gtin'] = None
    #                 variation_dict["sku_id"] = variant_response.url.split('/')[-1]
    #                 variation_dict["variant_url"] = variant_response.url
    #                 image_url =  variant_response.xpath('//div[@class="gallery__container"]//@data-zoom-image').getall() 
    #                 variation_dict['image_url'] = [f'https://www.marionnaud.fr/{images_urls}' for images_urls in image_url]
    #                 variation_dict['has_video'] = ''
    #                 variation_dict["video"] = variant_response.xpath("//div[@class='productInformationSection']//iframe/@src").getall()
    #                 if variation_dict["video"] != []:
    #                     variation_dict['has_video'] = True
    #                 else:
    #                     variation_dict['has_video'] = False 
    #                 variation_dict['brand'] = variant_response.xpath('//span[@class="productBrandName"]/text()').get('')
                    
    #                 price_before_variant = ''.join(variant_response.xpath('//div[@class="markdownPrice priceformat"]//text()').getall()).strip()
    #                 after_price_variant = ''.join(variant_response.xpath('//div[@class="finalPrice"]//text()').getall()).strip()
    #                 promo_second = f"https://www.marionnaud.fr/p/productVariantPromotions?code={promo_label_varint}"
    #                 payload = None
    #                 method = 'GET'
    #                 promo_response_second = await self.request_process(promo_second,self.headers,payload,method)
    #                 after_price = ''.join(response.xpath('//div[@class="finalPrice"]//text()').getall()).strip()
    #                 promo_lableb_json_method_variant = json.loads(promo_response_second.text)
    #                 promo_image_link_vaiant = promo_lableb_json_method_variant.get("normalPromotion","")
    #                 if promo_image_link_vaiant:
    #                     promo_image_link_vaiant = promo_image_link_vaiant.get("imageUrl","")
    #                     if "3P2" in promo_image_link_vaiant:
    #                         price_replace = float(after_price.replace("€","."))
    #                         variation_dict["pirce"] = "€"+str(price_replace*2)
    #                         variation_dict["price_before"] = "€"+str(price_replace*3)
    #                         variation_dict["promo_label"] = "2=3"
    #                     else:
    #                         promo_label_check_variant = promo_image_link_vaiant.get("description","")
    #                         # promo_labels = re.findall('\:\s*(.*?%)',promo_label_check_variant)[0]
    #                         if price_before_variant != '':
    #                             price = after_price_variant.replace('€','.')
    #                             variation_dict['price'] = '€'+ price
    #                             price_before= price_before_variant.replace('€','.')
    #                             variation_dict['price_before'] = '€'+ price_before_variant
    #                             variation_dict["promo_label"] = promo_label_check_variant
    #                         elif after_price_variant:
    #                             price = after_price_variant.replace('€','.')
    #                             variation_dict['price'] = '€'+ price
    #                             variation_dict["price_before"] = None
    #                             variation_dict["promo_label"] = promo_label_check_variant
    #                         else:
    #                             variation_dict["price_before"] = None
    #                             variation_dict["promo_label"] = None
    #                 else:
    #                     if price_before_variant != '':
    #                         price = after_price_variant.replace('€','.')
    #                         variation_dict['price'] = '€'+ price
    #                         price_before= price_before_variant.replace('€','.')
    #                         variation_dict['price_before'] = '€'+ price_before_variant
    #                         variation_dict["promo_label"] = None
    #                     else:
    #                         price = after_price_variant.replace('€','.')
    #                         variation_dict['price'] = '€'+ price
    #                         variation_dict["price_before"] = None
    #                         variation_dict["promo_label"] = None
    #                 # if variation_dict["price_before"] != None:
    #                 #     variation_dict["promo_label"] = re.findall(r"discount\_rate\'\:\'(.*?)'",variant_response.text)[0]
    #                 # else:
    #                 #     variation_dict["promo_label"] = None
    #                 data_size = variant_response.xpath('//span[@class="size-box selected "]/text()|//span[@class="size-box selected"]/text()').get('').strip() 
    #                 if data_size != '':
    #                     variation_dict["data_size"] = data_size
    #                 else:
    #                     variation_dict["data_size"] = None
                    
                    
    #                 data_key = variant_response.xpath('//div[@class="variant-color-selected"]//select/option/@data-code').getall()
    #                 data_value = variant_response.xpath('//div[@class="variant-color-selected"]//select/option/text()').getall()
    #                 if data_key and data_value:
                    
    #                     color_dic = dict(zip(data_key, data_value))
    #                     for color, value in color_dic.items():
    #                         if variation_dict["sku_id"] == color:
    #                             data_color = value
    #                             break
    #                 else:
    #                     data_color = None
    #                 if data_color:
    #                     variation_dict["data_color"] = data_color 
    #                 elif "" != color_data:
    #                     variation_dict["data_color"] = color_data 
    #                 else:
    #                     variation_dict["data_color"] =  None                   
    #                 add_to_cart = variant_response.xpath('//div[@class="addtoCart_Onload"]/button[@id="addToCartButton"]')
    #                 if add_to_cart:
    #                     variation_dict["in-stock"] = True
    #                 else:
    #                     variation_dict["in-stock"] = False
    #                 variation_lt.append(variation_dict)
    #                 if variation_dict["data_color"] or variation_dict["data_size"]:
    #                     item["prices"] = variation_lt   
    #                 else:
    #                     item["prices"] = []        
    #         else:
    #             item["prices"] = []
    #         description = ''.join(response.xpath('//div[@class="productInformationSection"]//h3[contains(text(),"DESCRIPTION")]/parent::div/parent::div//p/text()').getall()).replace('\n','').strip()
    #         if description != '':
    #             item["description"] = re.sub(r"\s+", " ", description)
    #         else:
    #             item["description"] = None
            
    #         review_links = response.xpath("//div[@class='prodReview pull-right']/a/@href").get('')
    #         review_link = response.urljoin(review_links)
            
    #         if 'login' not in review_link:
    #             review_api_id = review_link.split('/')[-1].replace('#reviews','')
    #             review_api_link = f'https://www.marionnaud.fr/api/product-reviews/{review_api_id}'
                
    #             path = review_api_link.split('fr')[-1]
                
    #             csrf_token = response.xpath('//input[@name="CSRFToken"]/@value').get()
    #             headers = {
    #                 'content-type': 'application/json;charset=UTF-8',
    #                 'csrftoken': csrf_token,         
    #                 'referer': f'{response.url}',
    #             }
                
    #             method = "POST"
    #             payload = json.dumps({"facets":[{"facet":"skinTone","values":[]},{"facet":"skinType","values":[]},{"facet":"rating","values":[]}],"sort":"PUBLICATION_DATE_NEWEST","startIndex":0,"amount":6})
                
    #             review_response = await self.request_process(review_api_link,headers,payload,method)
                
    #             total_page = review_response.json().get('totalAmount','')
                
    #             page_index = total_page//6
    #             review_list = []
    #             if page_index == 0:
    #                 json_datas = json.loads(review_response.text)
    #                 for data in json_datas["reviews"]:
    #                     review_dict = {}
    #                     headline = data.get('headline','')
    #                     comment = data.get('comment','')
    #                     review_dict['review'] = f'[{headline}] [{comment}]'
    #                     review_dict['stars'] = data.get('rating','')
    #                     review_dict['user'] = data.get('alias','')
    #                     dates = data.get('date')
    #                     timestamp = dates / 1000 
    #                     date1 = datetime.datetime.fromtimestamp(timestamp)
    #                     review_dict['date'] = date1.strftime("%d/%m/%Y")
    #                     review_list.append(review_dict)
                        

    #             else:

    #                 for page in range(0, page_index + 1):
    #                     start_review = page * 6  
    #                     next_page_url = f'https://www.marionnaud.fr/api/product-reviews/{review_api_id}'
    #                     payloads = {"facets":[{"facet":"skinTone","values":[]},{"facet":"skinType","values":[]},{"facet":"rating","values":[]}],"sort":"PUBLICATION_DATE_NEWEST","startIndex":(start_review),"amount":6}
                        
    #                     payload = json.dumps(payloads)
    #                     headers = {
    #                         'content-type': 'application/json',
    #                         'csrftoken': csrf_token,   
    #                         'referer': f'{response.url}',
    #                     }
                        
    #                     next_review_response = await self.request_process(next_page_url,headers,payload,method)
                        
    #                     json_datas = json.loads(next_review_response.text)
                        
    #                     for data in json_datas["reviews"]:
    #                         review_dict = {}
    #                         headline = data.get('headline','')
    #                         comment = data.get('comment','')
    #                         review_dict['review'] = f'[{headline}] [{comment}]'
    #                         review_dict['stars'] = data.get('rating','')
    #                         review_dict['user'] = data.get('alias','')
    #                         dates = data.get('date')
    #                         timestamp = dates / 1000 
    #                         date1 = datetime.datetime.fromtimestamp(timestamp)
    #                         review_dict['date'] = date1.strftime("%d/%m/%Y")
    #                         review_list.append(review_dict)     
                                    
    #             unique_lst = []
    #             for d in review_list:
    #                 if d not in unique_lst:
    #                     unique_lst.append(d)
    #             item['reviews'] = unique_lst    
    #             yield item
    #         else:
    #             item['reviews'] = []
    #         yield item  
    #     except Exception as e:
    #         with open('error.txt','a+')as f:f.write(str(e)+'\t'+str(response.url) +'\n')
   
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
        