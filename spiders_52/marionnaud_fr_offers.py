import os
import re
import json
import scrapy
import datetime

class MarionnaudFrOfferSpider(scrapy.Spider):
    name = "marionnaud_fr_offers"
    headers = {
            'authority': 'www.marionnaud.fr',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
            }
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    
    ROTATING_PROXY = os.getenv("ROTATING_PROXY_FR")
    custom_settings={
        'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/marionnaud_fr/%(name)s_{DATE}.json": {"format": "json",}},
        'ROTATING_PROXY_LIST' : [ROTATING_PROXY],
        'CONCURRENT_REQUESTS' : 8,
        "HTTPCACHE_ENABLED" : True,
        "HTTPCACHE_DIR" : 'httpcache',
        'HTTPCACHE_EXPIRATION_SECS':72000,
        "HTTPCACHE_IGNORE_HTTP_CODES":[502,504],
        "HTTPCACHE_STORAGE" : "scrapy.extensions.httpcache.FilesystemCacheStorage",
        }
    def start_requests(self): 
        self.spider_name=self.name
        MarionnaudFrOfferSpider.name="marionnaud_fr_pdp"
        url = 'https://www.marionnaud.fr/'
        yield scrapy.Request(url,callback=self.parse)

    async def product_details(self,product_url,product_response,response,product_item,rank):
        if not product_response.xpath('//div[@class="banner-with-error__right"]'):
            product_item['rank'] = rank
            product_item['url'] = response.urljoin(product_url)
            image_url =  product_response.xpath('//div[@class="gallery__container"]//@data-zoom-image').getall() 
            product_item['image_url'] = ['https://www.marionnaud.fr/'+ item for item in image_url]
            product_item['has_video'] = ''     
            product_item["video"] = product_response.xpath("//div[@class='productInformationSection']//iframe/@src").getall()
            if product_item["video"] != []:
                product_item['has_video'] = True
            else:
                product_item['has_video'] = False 
            product_item['master_product_id'] = product_response.xpath('//input[@class="GTMbaseProdudct"]/@value').get('')
            product_item['variant_id'] = product_url.split('/')[-1]
            product_item['name'] = product_response.xpath('//span[@class="producRangeName"]/text()').get('')
            product_item['brand'] = product_response.xpath('//span[@class="productBrandName"]/text()').get('')
            price_before = ''.join(product_response.xpath('//div[@class="markdownPrice priceformat"]//text()').getall()).strip()
            after_price = ''.join(product_response.xpath('//div[@class="finalPrice"]//text()').getall()).strip()
            link = response.url
            link_promo = link.split("/p/")[-1]
            promo = f"https://www.marionnaud.fr/p/productVariantPromotions?code={link_promo}"
            payload = None
            method = 'GET'
            promo_response = await self.request_process(promo,self.headers,payload,method)
            json_method = json.loads(promo_response.text)
            promo_image_link = json_method.get('normalPromotion','')
            if promo_image_link:
                promo_image_link = promo_image_link.get("imageUrl","")
                if "3P2" in promo_image_link:
                    price_replace = float(after_price.replace("€","."))
                    product_item["pirce"] = "€"+str(price_replace*2)
                    product_item["price_before"] = "€"+str(price_replace*3)
                    product_item["promo_label"] = "2=3"
                else:    
                    promo_label_check = promo_image_link.get("description","")
                    if price_before != '':
                        price = after_price.replace('€','.')
                        product_item['price'] = '€'+ price
                        price_before= price_before.replace('€','.')
                        product_item['price_before'] = '€'+ price_before
                        product_item["promo_label"] = promo_label_check
                    elif after_price:
                        price = after_price.replace('€','.')
                        product_item['price'] = '€'+ price
                        product_item["price_before"] = None
                        product_item["promo_label"] = promo_label_check
                    else:
                        product_item["price_before"] = None
                        product_item["promo_label"] = None
            else:
                if price_before != '':
                        price = after_price.replace('€','.')
                        product_item['price'] = '€'+ price
                        price_before= price_before.replace('€','.')
                        product_item['price_before'] = '€'+ price_before
                        product_item["promo_label"] = None
                else:
                    price = after_price.replace('€','.')
                    product_item['price'] = '€'+ price
                    product_item["price_before"] = None
                    product_item["promo_label"] = None
            # if price_before != '':
            #     price = after_price.replace('€','.')
            #     product_item['price'] = '€'+ price
            #     price_before= price_before.replace('€','.')
            #     product_item['price_before'] = '€'+ price_before
            # else:
            #     price = after_price.replace('€','.')
            #     product_item['price'] = '€'+ price
            #     product_item["price_before"] = None

            add_to_cart = product_response.xpath('//div[@class="addtoCart_Onload"]')
            if add_to_cart:
                product_item["in-stock"] = True
            else:
                product_item["in-stock"] = False
            return product_item
        
    def parse(self,response):
        dir_path=os.path.abspath(__file__ + "/../../../")
        supporting_files=os.path.join(dir_path,"supporting_files")
        with open(f'{supporting_files}/marionnaud_fr_offer_input.txt') as f:offer_urls = f.readlines()
        for offer_url in offer_urls:
            offer_url = offer_url.replace('\n','')
            offer_url = response.urljoin(offer_url)
            
                
            yield scrapy.Request(offer_url,callback=self.parse_list)

    async def parse_list(self,response):
        item = {}
        product_links = []
        if 'search/?' in response.url:
            item['title'] = response.url.split('search/?text=')[-1].replace('+',' ').capitalize()
            item['page_url'] = response.url
            count = response.xpath('//label[@class="totalResults pull-left"]/span/text()').get('').replace('résultats trouvés','').replace('/xa0','').replace('\xa0','').strip()
            
            if count == '':
                item['count'] = 0
            else:
                item['count'] = count
            
            query = response.url.split('text=')[-1]
            if int(item['count']) > 100:
                for page_count in range(0,round(int(item['count'])/100)+1):
                    next_page_url = f'https://www.marionnaud.fr/search?q={query}%3Arank-desc&sort=&page={page_count}&pageSize=100'  
                    payload = None
                    method = None
                    next_page_response = await self.requests_process(next_page_url,self.headers,payload,method)
                
                    next_product_link = next_page_response.xpath('//ul[@class="product-listing product-grid"]/li//div[@class="infoTextCarousel"]/a/@href').getall()
                    product_links.extend(next_product_link)
                    
            elif range(0,round(int(item['count'])/100)) != range(0, 0):
                for page_count in range(0,round(int(item['count'])/100)):
                    next_page_url = f'https://www.marionnaud.fr/search?q={query}%3Arank-desc&sort=&page={page_count}&pageSize=100'  
                    payload = None
                    method = None
                    next_page_response = await self.requests_process(next_page_url,self.headers,payload,method)
                    if next_page_response is None:
                        continue
                    next_product_link = next_page_response.xpath('//ul[@class="product-listing product-grid"]/li//div[@class="infoTextCarousel"]/a/@href').getall()
                    product_links.extend(next_product_link)
            else:
                next_product_link = response.xpath('//ul[@class="product-listing product-grid"]/li//div[@class="infoTextCarousel"]/a/@href').getall()
                product_links.extend(next_product_link)
        
        else:
            item['title'] = response.url.split('/')[-3].replace('-',' ').title()
            item['page_url'] = response.url
            item['count'] = response.xpath('//label[@class="totalResults pull-left"]/text()').get('').strip().replace(' articles','').replace('\xa0','').replace('/xa0','').strip()
            product_links = []
            
            spider_name = MarionnaudFrOfferSpider.name.replace('offers','categories')
            current_date = datetime.datetime.now().strftime("%Y-%m-%d")
            category_path = os.getcwd() + f'/exports/{spider_name}/{spider_name}_{current_date}.json'
            with open(category_path,'r',encoding='utf-8-sig') as f:
                json_file = f.read()
            category_names = json.loads(json_file)
            category_crumb = []
        
            matching_word = (response.url).split('/')[-1]
            category_name = category_names[0].get('name','').title()
            category_url = category_names[0].get('url','')
            for main_sub_cat in category_names[0]['category_crumb']:
                main_category = main_sub_cat.get('name','').title()
                main_category_url = main_sub_cat.get('url','')
                for sub_cat in main_sub_cat['category_crumb']:
                    sub_category = sub_cat.get('name','').title()
                    sub_cat_url = sub_cat.get('url','')
                   
                    if 'category_crumb' in sub_cat:
                        for sub_sub_cat in sub_cat['category_crumb']:
                            sub_sub_category = sub_sub_cat.get('name','').title()
                            sub_sub_cat_url = sub_sub_cat.get('url','')
                            if matching_word in sub_sub_cat_url:
                                category_crumb.append({'name':main_category,"url":main_category_url})
                                category_crumb.append({'name':sub_category,"url":sub_cat_url})
                                category_crumb.append({'name':sub_sub_category,"url":sub_sub_cat_url})
                                break

                    if matching_word in sub_cat_url:
                        category_crumb.append({'name': main_category, "url": main_category_url})
                        category_crumb.append({'name': sub_category, "url": sub_cat_url})
                        break
                                    
            item['category_crumb'] = category_crumb
            if int(item['count']) > 100:
                for page_count in range(0,round(int(item['count'])/100)+1):
                    
                    next_page_url = f'{response.url}?q=%3Arank-desc&page={page_count}&pageSize=100'
                    payload = None
                    method = None
                    next_page_response = await self.requests_process(next_page_url,self.headers,payload,method)
                
                    if next_page_response is None:
                        continue
                    next_product_link = next_page_response.xpath('//ul[@class="product-listing product-grid"]/li//div[@class="infoTextCarousel"]/a/@href').getall()
                    product_links.extend(next_product_link)
                
            elif range(0,round(int(item['count'])/100)) != range(0, 0):
                for page_count in range(0,round(int(item['count'])/100)):
                    next_page_url = f'{response.url}?q=%3Arank-desc&page={page_count}&pageSize=100'
                    payload = None
                    method = None
                    next_page_response = await self.requests_process(next_page_url,self.headers,payload,method)
                    if next_page_response is None:
                        continue
                    next_product_link = next_page_response.xpath('//ul[@class="product-listing product-grid"]/li//div[@class="infoTextCarousel"]/a/@href').getall()
                    product_links.extend(next_product_link)
            else:
                next_product_link = response.xpath('//ul[@class="product-listing product-grid"]/li//div[@class="infoTextCarousel"]/a/@href').getall()
                product_links.extend(next_product_link)
            product_xpath = '//ul[@class="product-listing product-grid"]/li//div[@class="infoTextCarousel"]/a'

        unique_links = []
        for link in product_links:
            if link not in unique_links:
                unique_links.append(link)
        
        products = []
        for rank,product_xpath in enumerate(unique_links,1):
            product_item = {}
            # product_url = 'https://www.marionnaud.fr/maquillage/teint/blush/making-you-blush-pinceau-blush-morphe/p/102345376'
            product_url = 'https://www.marionnaud.fr/' + product_xpath
            headers = {
                "Authority":"www.marionnaud.fr",
                "Method":"GET",
                "Path":f"{product_url}",
                "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Accept-Encoding":"gzip, deflate, br",
                "Accept-Language":"en-US,en;q=0.9",
                "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
            }

            payload = None
            method = 'GET'
            product_response = await self.requests_process(product_url,headers,payload,method)
            no_response_data = product_response.xpath('//span[@class="titre"]//text()').get('').strip()
            if 'Oups' == no_response_data:
                continue
            product_item = self.product_details(product_url,product_response,response,product_item,rank)
            products.append(product_item)
           
        if item['count'] == 0 and response.xpath('//div[@class="finalPrice"]//text()'):
            product_item = {}
            item['count'] = 1
            rank = 1
            product_item = self.product_details(response.url,response,response,response,product_item,rank)
            products.append(product_item)
        
        item["products"] = products
        yield item
            

    async def requests_process(self,url,headers,payload,method):
        if method == "POST":
            request = scrapy.Request(url,headers=headers,body=payload,method="POST")
        elif method == "GET" and headers:
            request = scrapy.Request(url,headers=headers,body=payload,method="GET")
        else:
            request = scrapy.Request(url)
       
        response = await self.crawler.engine.download(request, self)
        return response