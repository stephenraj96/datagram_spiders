import re
import os
import scrapy
import shutil
import hashlib
import datetime
from parsel import Selector
from pathlib import Path
from scrapy import signals
import requests
import json
from dotenv import load_dotenv
import logging



class MarionnaudFrMediaSpider(scrapy.Spider):
    name = "marionnaud_fr_media"
    dir_path=os.path.abspath(__file__ + "/../../../")
    try:
        load_dotenv()
    except:
        env_path = Path(".env")
        load_dotenv(dotenv_path=env_path)


    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    ROTATING_PROXY = os.getenv("ROTATING_PROXY_FR")
    proxies={'http':ROTATING_PROXY,'https':ROTATING_PROXY}
    custom_settings={
        'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/marionnaud_fr/%(name)s_{DATE}.json": {"format": "json",}},
        "CONCURRENT_REQUESTS":100,
        'ROTATING_PROXY_LIST' : [ROTATING_PROXY],
        # 'DOWNLOAD_HANDLERS' : {}
        # 'DOWNLOADER_CLIENT_TLS_METHOD':'TLSv1.2',
        "HTTPCACHE_ENABLED" : True,
        "HTTPCACHE_DIR" : 'httpcache',
        'ROTATING_PROXY_PAGE_RETRY_TIMES':1,
        "HTTPCACHE_STORAGE" : "scrapy.extensions.httpcache.FilesystemCacheStorage",
        # 'AUTOTHROTTLE_START_DELAY':5,
        # 'AUTOTHROTTLE_MAX_DELAY':60,
        # 'DOWNLOAD_DELAY':2,
        'DOWNLOAD_TIMEOUT':10,
        # 'DOWNLOADER_CLIENT_TLS_METHOD':'TLSv1.2'
        }
    dir_path=os.path.abspath(__file__ + "/../../../")
    supporting_files=os.path.join(dir_path,"supporting_files")
    # with open(f'{supporting_files}/user-agents.txt') as f:
    #     user_agents=f.readlines()       
    screenshot_folder=os.getcwd()+rf"/exports/{name}/screenshots_{DATE}"
    if not os.path.exists(screenshot_folder):
        os.makedirs(screenshot_folder)
    headers = {
    'accept-encoding': 'gzip, deflate, br',
  'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
}

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(MarionnaudFrMediaSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signals.spider_closed)
        return spider


    def spider_closed(self, spider):
        if os.path.exists(self.screenshot_folder):
            self.spider_name=self.name
            shutil.make_archive(os.getcwd()+rf"/exports/{self.spider_name}/"+f"screenshots_{self.DATE}", 'zip', self.screenshot_folder)
            screenshot_zip_file=os.getcwd()+rf"/exports/{self.spider_name}/"+f"screenshots_{self.DATE}.zip"
            if os.path.exists(screenshot_zip_file):
                os.system(f"usr/local/bin/aws s3 cp {screenshot_zip_file}  s3://scraping-external-feeds-lapis-data/{self.CURRENT_DATE}/marionnaud_fr/")
                logging.warning(f"Completed aws s3 cp {screenshot_zip_file}  s3://scraping-external-feeds-lapis-data/{self.CURRENT_DATE}/marionnaud_fr/")
            else:
                logging.warning(f" File Not Found {screenshot_zip_file}")
            

    def start_requests(self):
        homepage_url="https://www.marionnaud.fr/"
        yield scrapy.Request(homepage_url,callback=self.home_page,headers=self.headers,meta={'proxy': self.ROTATING_PROXY})

    def home_page(self,response):
        dir_path=os.path.abspath(__file__ + "/../../../")
        supporting_files=os.path.join(dir_path,"supporting_files")
        with open(f'{supporting_files}/marionnaud_fr_media.txt') as f:
            start_ulrs=f.readlines()            
        for url in start_ulrs:
            url=url.strip()
            yield scrapy.Request(url,callback=self.parse,headers=self.headers,cb_kwargs={"url":url}.copy(),errback=self.errback_yield,meta={'proxy': self.ROTATING_PROXY},dont_filter=True)
        for block in response.xpath('//div[@class="carousel-inner"]/div'):
            redirection_url=(block.xpath('./a/@href').get(''))
            if block.xpath('.//img/@src').get(""):
                image_url = response.urljoin(block.xpath('.//img/@src').get(""))
            elif json.loads(block.xpath('.//img/@data-media').get({})).get('992',''):
                image_url=response.urljoin(json.loads(block.xpath('.//img/@data-media').get({})).get('992',''))
            hash_obj = hashlib.md5(image_url.encode("utf-8"))
            filename = hash_obj.hexdigest()+".png"
            if not os.path.exists(f"{self.screenshot_folder}/{filename}"):
                self.image_download(image_url,filename)
            main_slider={"format": "Main Slider",
            "page_url": response.url,
            "image_url": image_url,
            "screenshot": filename,
            "redirection_url": redirection_url,
            "products_count":0,
            "master_products": []
            }
            yield scrapy.Request(redirection_url,callback=self.homepage_product_listing,cb_kwargs={"item":main_slider.copy()},meta={'proxy': self.ROTATING_PROXY},dont_filter=True)
                # # # 3 Banners in the middle 
        for block in response.xpath("//div[@class='wrapper_SliderTrioBig']/div/a"):
            redirection_url=(block.xpath('.//@href').get(''))
            image_url = response.urljoin(block.xpath('.//img/@src').get(""))
            hash_obj = hashlib.md5(image_url.encode("utf-8"))
            filename = hash_obj.hexdigest()+".png"
            if not os.path.exists(f"{self.screenshot_folder}/{filename}"):
                self.image_download(image_url,filename)
            middle_banner={"format": "Banner",
                "page_url": response.url,
                "image_url": image_url,
                "screenshot": filename,
                "redirection_url": redirection_url,
                "products_count":0,
                "master_products": []
                }
            yield scrapy.Request(redirection_url,callback=self.homepage_product_listing,cb_kwargs={"item":middle_banner.copy()},meta={'proxy': self.ROTATING_PROXY},dont_filter=True)
        for block in response.xpath('//section[@class="carousel"]//a'):
            redirection_url = block.xpath('./@href').get('')
            image_url= block.xpath('./img/@src').get('')
            hash_obj = hashlib.md5(image_url.encode("utf-8"))
            filename = hash_obj.hexdigest()+".png"
            if not os.path.exists(f"{self.screenshot_folder}/{filename}"):
                self.image_download(image_url,filename)
            slider={"format": "Slider",
                "page_url": response.url,
                "image_url": image_url,
                "screenshot": filename,
                "redirection_url": redirection_url,
                "products_count":0,
                "master_products": []
                }
            yield scrapy.Request(redirection_url,callback=self.homepage_product_listing,cb_kwargs={"item":slider.copy()},meta={'proxy': self.ROTATING_PROXY},dont_filter=True)


    
    def parse(self,response,url):
        for block in response.xpath('//div[@class="carousel-inner"]/div'):
            redirection_url=(block.xpath('./a/@href').get(''))
            if block.xpath('.//img/@src').get(""):
                image_url = response.urljoin(block.xpath('.//img/@src').get(""))
            elif json.loads(block.xpath('.//img/@data-media').get({})).get('992',''):
                image_url=response.urljoin(json.loads(block.xpath('.//img/@data-media').get({})).get('992'))
            hash_obj = hashlib.md5(redirection_url.encode("utf-8"))
            filename = hash_obj.hexdigest()+".png"
            if not os.path.exists(f"{self.screenshot_folder}/{filename}"):
                self.image_download(image_url,filename)
            main_slider={"format": "Main Slider",
            "page_url": url,
            "image_url": image_url,
            "screenshot": filename,
            "redirection_url": redirection_url,
            "products_count":0,
            "master_products": []
            }
            yield scrapy.Request(redirection_url,callback=self.homepage_product_listing,cb_kwargs={"item":main_slider.copy()},errback=self.errback_yield,dont_filter=True,meta={'proxy': self.ROTATING_PROXY})

    def image_download(self,url,filename):
        response=requests.get(url,headers=self.headers,proxies=self.proxies)
        with open(f"{self.screenshot_folder}/{filename}",'wb') as f:f.write(response.content)

    async def homepage_product_listing(self, response,item):
        blocks=response.xpath('//ul[@class="product-listing product-grid"]/li')
        if blocks:
            for block in blocks:
                product_url=response.urljoin(block.xpath(".//@href").get(''))
                product_response = await  self.requests_process(product_url)
                if product_response is not None:
                    product_response_text=product_response.text
                    # product_response=Selector(text=product_response.text)
                    
                    data = {}
                    data["product_id"]= product_url.split('/')[-1]
                    data["name"] = product_response.xpath("//span[@class='producRangeName']/text()").get('').replace('+','')
                    if re.search(r'brand\"\:\s*\{\s*.*\s*\"name\"\:\s*\"(.*?)\"',product_response_text):
                        data["brand"]= re.findall(r'brand\"\:\s*\{\s*.*\s*\"name\"\:\s*\"(.*?)\"',product_response_text)[0]
                    data["url"] = product_url
                    price = ''.join(product_response.xpath('//div[@class="finalPrice"]//text()').getall()).strip()
                    data['price'] = f"€{price.replace('€','.')}"
                    image_url =  product_response.xpath('//div[@class="gallery__container"]//@data-zoom-image').getall() 
                    data['image_url'] = ['https://www.marionnaud.fr/'+ item for item in image_url]
                    if data["name"]:
                        item['master_products'].append(data.copy())
            curent_page=int(response.xpath("//*[@id='js_currentPage']/@value").get('0').strip())
            total_page=int(response.xpath("//*[@id='js_numberofPage']/@value").get('0').strip())
            if curent_page<total_page:
                next_page_link =response.urljoin(f'?page={curent_page}')
                # self.headers['user-agent']=random.choice(self.user_agents).strip()
                yield scrapy.Request(next_page_link,callback=self.homepage_product_listing,cb_kwargs={"item":item.copy()},errback=self.errback_yield,meta={'proxy': self.ROTATING_PROXY},dont_filter=True)
            else:
                yield item
        elif response.xpath("//span[@class='producRangeName']/text()").get('').replace('+',''):
            data = {}
            data["product_id"]= response.url.split('/')[-1]
            data["name"] =response.xpath("//span[@class='producRangeName']/text()").get('').replace('+','')
            if re.search(r'brand\"\:\s*\{\s*.*\s*\"name\"\:\s*\"(.*?)\"',response.text):
                data["brand"]= re.findall(r'brand\"\:\s*\{\s*.*\s*\"name\"\:\s*\"(.*?)\"',response.text)[0]
            data["url"] = response.url
            price = ''.join(response.xpath('//div[@class="finalPrice"]//text()').getall()).strip()
            data['price'] = f"€{price.replace('€','.')}"
            image_url =response.xpath('//div[@class="gallery__container"]//@data-zoom-image').getall() 
            data['image_url'] = ['https://www.marionnaud.fr/'+ item for item in image_url]
            if data["name"]:
                item['master_products'].append(data.copy())
                yield item
        else:
            item

                

    # async def requests_process(self,url):
    #     """ scrapy request"""
    #     try:
    #         url=url.strip()
    #         response=requests.get(url,headers=self.headers,proxies={'http':self.ROTATING_PROXY,'https':self.ROTATING_PROXY})
    #     except:
    #         return None
    #     return response

    async def requests_process(self,url):
        try:
            """ scrapy request"""
            url=url.strip()
            request = scrapy.Request(url,headers=self.headers,dont_filter=True,meta={'proxy': self.ROTATING_PROXY})
            response = await self.crawler.engine.download(request,self)
            return response
        except Exception as e:
            print(e,flush=True)
            return None

    def errback_yield(self, failure):
        try:
            yield failure.request.cb_kwargs["item"]
        except:
            pass
