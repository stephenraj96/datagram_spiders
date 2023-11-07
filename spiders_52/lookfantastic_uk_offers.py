import scrapy
import json
import os
import re
from inline_requests import inline_requests
from scrapy import Request
from dotenv import load_dotenv
from pathlib import Path
import datetime

class LookfantasticSpider(scrapy.Spider):
    name = 'lookfantastic_uk_offers'
    headers = {
        'accept-encoding': 'gzip, deflate, br',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
            }
    """ load env file """
    try:
        load_dotenv()
    except:
        env_path = Path(".env")
        load_dotenv(dotenv_path=env_path)

    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    ROTATING_PROXY = os.getenv("ROTATING_PROXY")
    custom_settings={'CONCURRENT_REQUESTS':4,
                    # "HTTPCACHE_ENABLED" : True,
                    # "HTTPCACHE_DIR" : 'httpcache',
                    # 'HTTPCACHE_EXPIRATION_SECS':79200,
                    # "HTTPCACHE_IGNORE_HTTP_CODES":[502],
                    # "HTTPCACHE_STORAGE" : "scrapy.extensions.httpcache.FilesystemCacheStorage",
                    'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/lookfantastic_uk/%(name)s_{DATE}.json": {"format": "json",}},
                    'ROTATING_PROXY_LIST' : [ROTATING_PROXY]
                    }

    def start_requests(self):
        self.spider_name=self.name
        LookfantasticSpider.name="lookfantastic_uk_pdp"
        dir_path=os.path.abspath(__file__ + "/../../../")
        supporting_files=os.path.join(dir_path,"supporting_files")
        with open(f'{supporting_files}/lookfantastic_offers_urls.txt') as f:urls = f.readlines()
        for url in urls:
            yield scrapy.Request(url.strip(),headers=self.headers,callback=self.parse,cb_kwargs={"item":{},'rank':0})

    @inline_requests
    def parse(self, response,item={},rank=0):
        if not  bool(item):
            title=response.xpath("//*[@id='responsive-product-list-title']/text()").get('').replace("Results for","").replace("“","").replace("”","").strip()
            if not title:
                title=response.xpath("//li[@class='breadcrumbs_item breadcrumbs_item-active']/text()").get("")
            title = title.lower()
            item['title']= re.sub('\s+', ' ',title )
            item['page_url']=response.url
            item['count']=0
            if "search" not in response.url:
                item['category_crumb']=[]
                for category in  response.xpath("//ul[@class='breadcrumbs_container']/li")[1:]:
                    name=category.xpath("./a/text()").get("").strip()
                    url=category.xpath("./a/@href").get("").strip()
                    if not url:
                        name=category.xpath("./text()").get()
                        url=response.url
                    item['category_crumb'].append({"name":name,"url":url}.copy())
            item['products']=[]
        product_count=len(response.xpath("//li[contains(@class,'productListProducts_product')]"))
        item['count']=item['count']+product_count
        for block in  response.xpath("//li[contains(@class,'productListProducts_product')]"):
            product_url=response.urljoin(block.xpath(".//a[@class='productBlock_link']/@href").get(''))
            product_response = yield Request(product_url,headers=self.headers,dont_filter=True)
            if product_response.xpath("//*[@class='productName_title']/text()").get() is not None:
                rank=rank+1
                data = {}
                data['rank']=rank
                data["url"] = product_url
                data['image_url']=product_response.xpath("//*[@data-size='1600']/@data-path").getall()
                if  response.xpath("//*[@type='video/mp4']/@src").getall():
                    data['has_video']=True
                    data['video']= product_response.xpath("//*[@type='video/mp4']/@src").getall()
                else:
                    data['has_video']=False
                    data['video']= []
                data["master_product_id"]= product_response.xpath("//div[@class='productPrice']/@data-product-id").get()
                if product_response.xpath("//*[@id='productSchema']/text()").get():
                    json_data=json.loads( product_response.xpath("//*[@id='productSchema']/text()").get("{}"))
                    data["mpn"]= json_data.get("mpn",None)
                else:
                    data["mpn"]=None
                data["name"] = product_response.xpath("//*[@class='productName_title']/text()").get()
                data["brand"]= product_response.xpath("//div[@class='productBrandLogo']/img/@title").get()
                if data['brand'] is None:
                    if re.findall("productBrand: \"(.*)\"",product_response.text):
                        data['brand']=re.findall("productBrand:\s*\"(.*)\"",product_response.text)[0]
                data['price']=  product_response.xpath("//*[@class='productPrice_price  ']/text()").get()
                if "InStock" in product_response.xpath("//*[@id='productSchema']/text()").get(""):
                    data["in-stock"]= True
                else:
                    data["in-stock"]= False
                if product_response.xpath("//*[@class='productPrice_rrp']/text()").get(''):
                    data["price_before"]=product_response.xpath("//*[@class='productPrice_rrp']/text()").get('').replace("RRP:","").strip()
                else:
                    data["price_before"]=None
                data["promo_label"]= product_response.xpath("//*[@class='papDescription_title']/text()").get()
                for key, value in data.items():
                    if isinstance(value, str):
                        data[key]=value.strip()
                        if value=="":
                            data[key]=None
                item['products'].append(data.copy())
        next_page=response.xpath("//a[@aria-current='true']/parent::li/following-sibling::li[1]/a/@href").get()
        if next_page is not None:
            next_page=response.urljoin(next_page)
            yield scrapy.Request(next_page,headers=self.headers, callback=self.parse,cb_kwargs={"item":item,"rank":rank})
        else:
            yield item
