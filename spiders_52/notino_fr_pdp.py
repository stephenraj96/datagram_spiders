import scrapy
import os
import json
import datetime
import re
from dotenv import load_dotenv
from pathlib import Path
from scrapy import Request
import html


""" load env file """
try:
    load_dotenv()
except:
    env_path = Path(".env")
    load_dotenv(dotenv_path=env_path)

class NotinoSpider(scrapy.Spider):
    name = 'notino_fr_pdp'
    headers = {
            'accept-encoding': 'gzip',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}
    review_header_headers = {
                        'accept-encoding': 'gzip',
                        'authority': 'www.notino.fr',
                        'accept': '*/*',
                        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                        'content-type': 'application/json',
                        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
                        }
    
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    ROTATING_PROXY = os.getenv("ROTATING_PROXY_FR")
    custom_settings={
                    "CONCURRENT_REQUESTS":16,
                    "HTTPCACHE_ENABLED" : True,
                    "HTTPCACHE_DIR" : 'httpcache',
                    'HTTPCACHE_EXPIRATION_SECS':82800,
                    "HTTPCACHE_STORAGE" : "scrapy.extensions.httpcache.FilesystemCacheStorage",
                    'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/notino_fr/%(name)s_{DATE}.json": {"format": "json",}},
                    'ROTATING_PROXY_LIST' : [ROTATING_PROXY]
                    }

    def start_requests(self):
        category_spider_name=(NotinoSpider.name).replace("pdp","categories")
        dir_path= os.getcwd()+rf"/exports/{category_spider_name}"
        with open(os.path.join(dir_path,f"{category_spider_name}_{self.CURRENT_DATE}.json"), 'r',encoding="utf-8") as f:
            contents = json.loads(f.read())
        for makeup in contents:
                first_name= makeup.get("name","")
                first_url=makeup.get("url",'')
                for category in makeup.get("category_crumb",[]):
                    second_name= category.get("name","")
                    second_url=category.get("url","")
                    if "category_crumb" in category:
                        for sub_category in category.get("category_crumb",[]):
                            third_name= sub_category.get("name","")
                            third_url=sub_category.get("url","")
                            category_crumb=[{"name":first_name,"url":first_url}.copy(),{"name":second_name,"url":second_url}.copy(),{"name":third_name,"url":third_url}.copy()]
                            yield scrapy.Request(url=third_url,headers=self.headers, callback=self.parse,cb_kwargs={'page_num':2,"category":category_crumb})
                    else:
                        category_crumb=[{"name":first_name,"url":first_url}.copy(),{"name":second_name,"url":second_url}.copy()]
                        yield scrapy.Request(url=second_url,headers=self.headers, callback=self.parse,cb_kwargs={'page_num':2,"category":category_crumb})

    def parse(self, response,page_num=None,category=[]):
        for block in response.xpath('//div/div[@data-testid="product-container"]/a'):
            product_url=response.urljoin(block.xpath("./@href").get(""))
            yield scrapy.Request(product_url,headers=self.headers, callback=self.parse_product,cb_kwargs={"category":category},dont_filter=True)
            
        if re.search(r'\"pageUrl\":\"([^>]*?)\"',str(response.text)):        
            next_page_url = re.findall(r'\"pageUrl\":\"([^>]*?)\"',str(response.text))[0]
            if not response.xpath('//div[contains(@class,"nextPage")]/@hidden'):
                next_page_url = next_page_url.replace('{sortOption}','1')
                next_page_url = next_page_url.replace('{pageNumber}',f'{page_num}')
                page_num += 1
                yield scrapy.Request(response.urljoin(next_page_url),callback=self.parse,cb_kwargs={'page_num':page_num,'category':category})
        
    async def parse_product(self, response,category=[]):
        item={}
        item["url"] = response.url
        if response.xpath("//script[@type='application/ld+json']/text()").get():
            json_data=json.loads(response.xpath("//script[@type='application/ld+json']/text()").get('{}'))
            if response.xpath("//span[@class='sc-3sotvb-4 kSRNEJ']/text()").get():
                item['name'] = response.xpath("//span[@class='sc-3sotvb-4 kSRNEJ']/text()").get()
            else:
                item['name'] = None
            item['gtin']=json_data.get("gtin13",None)
            item['image_url']=[re.sub("\s+|\[|\]","",image_url.replace("order_2k",'detail_zoom').replace("`","%60")) for image_url in json_data.get("image",[])]
            item['video']= response.xpath("//div[@class='sc-1s81hkh-1 cPDPWg']/iframe/@src").getall()
            if item['video']:
                item['has_video']= True
            else:
                item['has_video']= False
            item['brand']= html.unescape(json_data.get("brand",{}).get("name",""))
            item['master_product_id']=json_data.get("sku",None)
            master_id=item['master_product_id']
            item['subtitle']=response.xpath("//span[@class='sc-3sotvb-5 bBQCfG']/text()").get('')
            if response.xpath("//div[@id='pd-price']//text()").getall():
                item['price'] = ' '.join(response.xpath("//div[@id='pd-price']//text()").getall())
            else:
                item['price'] = None
            if response.xpath("//span[@class='sc-wpdhab-6 kPjZwv']//text()").getall():
                item['price_before'] = ' '.join(response.xpath("//span[@class='sc-wpdhab-6 kPjZwv']//text()").getall())
            else:
                item['price_before'] = None
            if response.xpath("//span[@class='sc-wpdhab-5 wDBvY']//text()"):
                item['promo_label']=response.xpath("//span[@class='sc-wpdhab-5 wDBvY']//text()").get()
            elif response.xpath('//div[@data-testid="voucher-discount-icon"]/div/text()|//div[@data-testid="voucher-discount-icon"]/div/span/text()'):
                promo = response.xpath('//div[@data-testid="voucher-discount-icon"]/div/text()|//div[@data-testid="voucher-discount-icon"]/div/span/text()').getall()
                item['promo_label'] = ''.join(promo)
            else:
                item['promo_label'] = None
            description=re.sub('<.*?>','',html.unescape(json_data.get("description","").strip()))
            item['description'] = description if description else None

            item['category_crumb']=category
            if 'en stock' in response.xpath("//span[@class='sc-mu8uqe-4 firsAk']//text()|//span[@class='sc-mu8uqe-4 frURDV']/text()").get('').lower().strip():
                item['in-stock']=True
            else:
                item['in-stock']=False
            item['prices']=[]
            
            for variant in json_data.get("offers",[]):
                if  variant.get("url",None):
                    data={}
                    data['variant_url']=  response.urljoin( variant.get("url",''))
                    variant_response=yield Request(data['variant_url'],headers=self.headers)
                    variant_response= await self.request(data['variant_url'])
                    if response.xpath("//script[@type='application/ld+json']/text()").get():
                        variant_json_data=json.loads(variant_response.xpath("//script[@type='application/ld+json']/text()").get('{}'))
                        data['gtin']=variant_json_data.get("gtin13",None)
                        data['sku_id']= variant.get("sku",None)
                        data['image_url']=[re.sub("\s+|\[|\]","",image_url.replace("order_2k",'detail_zoom').replace("`","%60")) for image_url in variant_json_data.get("image",[])]
                        data['video']= variant_response.xpath("//div[@class='sc-1s81hkh-1 cPDPWg']/iframe/@src").getall()
                        if data['video']:
                            data['has_video']= True
                        else:
                            data['has_video']= False
                        if variant_response.xpath("//div[@id='pd-price']//text()").getall():
                            data['price'] = ' '.join(variant_response.xpath("//div[@id='pd-price']//text()").getall())
                        else:
                            data['price'] = None
                        if variant_response.xpath("//span[@class='sc-wpdhab-6 kPjZwv']//text()").getall():
                            data['price_before']=' '.join(variant_response.xpath("//span[@class='sc-wpdhab-6 kPjZwv']//text()").getall())
                        else:
                            data['price_before']= None
                        if variant_response.xpath("//span[@class='sc-wpdhab-5 wDBvY']//text()"):
                            data['promo_label']=variant_response.xpath("//span[@class='sc-wpdhab-5 wDBvY']//text()").get()
                        elif variant_response.xpath('//div[@data-testid="voucher-discount-icon"]/div/text()|//div[@data-testid="voucher-discount-icon"]/div/span/text()'):
                            variant_promo = variant_response.xpath('//div[@data-testid="voucher-discount-icon"]/div/text()|//div[@data-testid="voucher-discount-icon"]/div/span/text()').getall()
                            data['promo_label'] = ''.join(variant_promo)
                        else:
                            data['promo_label'] = None
                        # 
                        if 'en stock' in variant_response.xpath("//span[@class='sc-mu8uqe-4 firsAk']//text()|//span[@class='sc-mu8uqe-4 frURDV']/text()").get('').lower().strip():
                            data['in-stock']=True
                        else:
                            data['in-stock']=False
                        data_variant = variant_response.xpath("//div[@class='sc-h83s98-4 fqSniA']/span/text()|//span[@class='sc-8ca8nt-4 gyBAuw']/text()").getall()
                        if len(data_variant) == 1:
                            data_variant = data_variant[0]
                            if not data_variant:
                                continue
                            if data_variant.lower().endswith("cm"):
                                data['data_size']=data_variant
                                data['data_color']=None
                            elif re.findall('\d+\s*ml|\d+x\d+\s*ml',data_variant):
                                data['data_size']=re.findall('\d+\s*ml|\d+x\d+\s*ml',data_variant)[0]
                                data['data_color']=data_variant.replace(data['data_size'],'')          
                            elif re.findall('\d+\s*g|\d+\s*pcs',data_variant):
                                data['data_size']=re.findall('\d+\s*g|\d+\s*pcs',data_variant)[0]
                                data['data_color']=data_variant.replace(data['data_size'],'')
                            elif re.search(r'additionalInfo\"\:\"(.*?)\"\,',response.text):
                                data["data_size"] = re.findall(r'additionalInfo\"\:\"(.*?)\"\,',response.text)[0]
                                data["data_color"] = None
                            else:
                                data['data_size']=None
                                data['data_color']=data_variant
                            if data['data_color'] == '':
                                data['data_color'] = None
                            item['prices'].append(data.copy()) 
                        else:
                            data_variant = ''.join(data_variant).strip()
                            if not data_variant:
                                continue
                            if data_variant.lower().endswith("cm"):
                                data['data_size']=data_variant
                                data['data_color']=None
                            elif re.findall('\d+\s*ml|\d+x\d+\s*ml',data_variant):
                                data['data_size']=re.findall('\d+\s*ml|\d+x\d+\s*ml',data_variant)[0]
                                data['data_color']=data_variant.replace(data['data_size'],'')         
                            elif re.findall('\d+\s*g|\d+\s*pcs',data_variant):
                                data['data_size']=re.findall('\d+\s*g|\d+\s*pcs',data_variant)[0]
                                data['data_color']=data_variant.replace(data['data_size'],'')
                            elif re.search(r'additionalInfo\"\:\"(.*?)\"\,',response.text):
                                data["data_size"] = re.findall(r'additionalInfo\"\:\"(.*?)\"\,',response.text)[0]
                                data["data_color"] = None
                            else:
                                data['data_size']=None
                                data['data_color']=data_variant
                            if data['data_color'] == '':
                                data['data_color'] = None   
                            item['prices'].append(data.copy()) 
            item['reviews']=[]
            review_count=int(response.xpath("//span[@class='sc-dmctIk kgesg']/text()").get(0))
            review_count= json_data.get("aggregateRating",{}).get("ratingCount",0)
            if review_count>0:
                review_url = f"https://www.notino.fr/api/product/?operationName=getReviews&variables=%7B%22code%22%3A%22{master_id}%22%2C%22orderBy%22%3A%22DateTime%22%2C%22orderDesc%22%3Atrue%2C%22page%22%3A1%2C%22pageSize%22%3A20%2C%22hideTranslated%22%3Afalse%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%22a2b79caed3a17075b1263985b020025ee983e4d2e4cd31b9cb3ffe1cda868847%22%7D%7D"
                yield scrapy.Request(review_url,headers=self.review_header_headers,method='GET', callback=self.reviews,cb_kwargs={"item":item,"review_count":review_count,"page_count":1},dont_filter=True)
            else:
                yield item

    def reviews(self,response,item,review_count=0,page_count=1):
        review_json_data=json.loads(response.text)
        for review_block in review_json_data.get("data",{}).get("reviews",[]):
            data={}
            data['review']=f'[{review_block.get("text","")}]'
            data['user']=review_block.get("userName",None)
            data['stars']=review_block.get("score",0)
            data['date']=review_block.get("createdDate","").strip().split("T")[0]
            item['reviews'].append(data.copy())
            
        review_collected_count=len(review_json_data.get("data",{}).get("reviews",[]))
        if review_count>review_collected_count and len(review_json_data.get("data",{}).get("reviews",[]))!=0:
            current_page=f'page%22%3A{page_count}'
            page_count=page_count+1
            next_page=f'page%22%3A{page_count}'
            review_url=response.url.replace(current_page,next_page)
            yield scrapy.Request(review_url,headers=self.review_header_headers,method='GET', callback=self.reviews,cb_kwargs={"item":item,"review_count":review_count,"page_count":page_count},dont_filter=True)
        else:
            yield item

    async def request(self,url):
        """ scrapy request"""
        request = scrapy.Request(url,headers=self.headers,dont_filter=True)
        response = await self.crawler.engine.download(request,self)
        return response