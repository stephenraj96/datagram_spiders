import scrapy
import hashlib
from scrapy import signals
import json
import os
import datetime
from dotenv import load_dotenv
from pathlib import Path
import re
import shutil
import html
import logging
import time


class NotinoSpider(scrapy.Spider):
    name = 'notino_fr_media'
    headers = {
  'accept-encoding': 'gzip',
  'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
}
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
    screenshot_folder=os.getcwd()+rf"/exports/{name}/notino_fr_screenshots_{DATE}"
    if not os.path.exists(screenshot_folder):
        os.makedirs(screenshot_folder)
    

    custom_settings={
                    "HTTPCACHE_ENABLED" : True,
                    "CONCURRENT_REQUESTS":10,
                    "HTTPCACHE_DIR" : 'httpcache',
                    'HTTPCACHE_EXPIRATION_SECS':82800,
                    "HTTPCACHE_STORAGE" : "scrapy.extensions.httpcache.FilesystemCacheStorage",
                    'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/notino_fr/%(name)s_{DATE}.json": {"format": "json",}},
                    'ROTATING_PROXY_LIST' : [ROTATING_PROXY]
                    }
    
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(NotinoSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signals.spider_closed)
        return spider
    
    def spider_closed(self, spider):
        if os.path.exists(self.screenshot_folder):
            shutil.make_archive(os.getcwd()+rf"/exports/{self.spider_name}/"+f"notino_fr_screenshots_{self.DATE}", 'zip', self.screenshot_folder)
            screenshot_zip_file=os.getcwd()+rf"/exports/{self.spider_name}/"+f"notino_fr_screenshots_{self.DATE}.zip"
            logging.warning(f"{screenshot_zip_file}")
            time.sleep(4)
            if os.path.exists(screenshot_zip_file):
                os.system(f"/usr/local/bin/aws s3 cp {screenshot_zip_file}  s3://scraping-external-feeds-lapis-data/{self.CURRENT_DATE}/notino_fr/")
                logging.warning(f"Completed aws s3 cp {screenshot_zip_file}  s3://scraping-external-feeds-lapis-data/{self.CURRENT_DATE}/notino_fr/")
            else:
                logging.warning(f" File Not Found {screenshot_zip_file}")
            
    def start_requests(self):
        self.spider_name=self.name
        NotinoSpider.name="notino_fr_pdp"
        homepage_url="https://www.notino.fr/"
        yield scrapy.Request(homepage_url,headers=self.headers,callback=self.home_page)
        
    async def home_page(self, response):
        for count,main_block in enumerate(response.xpath("//div[@class='carousel-inner ']/div"),start=1):
            image_url=main_block.xpath(".//source[@class='desktop']/@srcset|.//source[@class='desktop']/img/@data-srcset").get()
            redirection_url=main_block.xpath("./a/@href").get()
            hash_obj = hashlib.md5(image_url.encode("utf-8"))
            filename = hash_obj.hexdigest()+".jpg"
            image_response=await self.request(image_url)
            with open(f"{self.screenshot_folder}/{filename}",'wb') as f:f.write(image_response.body)
            main_slider={"format": "Main Slider",
            "page_url": response.url,
            "image_url": re.sub("\s+|\[|\]","",image_url.replace("order_2k",'detail_zoom').replace("`","%60")),
            "screenshot": filename,
            "redirection_url": redirection_url,
            "master_products": []
            }
            yield scrapy.Request(redirection_url,headers=self.headers,callback=self.product_listing,cb_kwargs={"item":main_slider},dont_filter=True)
        for count,brand in enumerate(response.xpath("//div[@class='hp-brands']/div"),start=1):
            if brand.xpath("./a/@href").get():
                image_url= brand.xpath("./a/img/@data-src").get()
                redirection_url=response.urljoin(brand.xpath("./a/@href").get())
                hash_obj = hashlib.md5(redirection_url.encode("utf-8"))
                filename = hash_obj.hexdigest()+".jpg"
                image_response=await self.request(image_url)
                with open(f"{self.screenshot_folder}/{filename}",'wb') as f:f.write(image_response.body)
                sponsored_brand={"format": "Sponsored Brand",
                        "page_url": response.url,
                        "image_url": re.sub("\s+|\[|\]","",image_url.replace("order_2k",'detail_zoom').replace("`","%60")),
                        "screenshot": filename,
                        "redirection_url": redirection_url,
                        "master_products": []
                        }
                yield scrapy.Request(redirection_url,headers=self.headers,callback=self.product_listing,cb_kwargs={"item":sponsored_brand},dont_filter=True)
        for count,brand in enumerate(response.xpath("//div[@class='hp-promotions-banners']/div"),start=1):
            if brand.xpath("./a/@href").get():
                image_url=brand.xpath("./a/img/@data-src").get()
                redirection_url=response.urljoin(brand.xpath("./a/@href").get())
                hash_obj = hashlib.md5(redirection_url.encode("utf-8"))
                filename = hash_obj.hexdigest()+".jpg"
                image_response=await self.request(image_url)
                with open(f"{self.screenshot_folder}/{filename}",'wb') as f:f.write(image_response.body)
                banner={"format": "Sponsored Brand",
                        "page_url": response.url,
                        "image_url":  re.sub("\s+|\[|\]","",image_url.replace("order_2k",'detail_zoom').replace("`","%60")),
                        "screenshot": filename,
                        "redirection_url": redirection_url,
                        "master_products": []
                        }
                yield scrapy.Request(redirection_url,headers=self.headers,callback=self.product_listing,cb_kwargs={"item":banner},dont_filter=True)
        dir_path=os.path.abspath(__file__ + "/../../../")
        supporting_files=os.path.join(dir_path,"supporting_files")
        with open(f'{supporting_files}/notino_urls.txt') as f:urls = f.readlines()
        for url in urls:
            yield scrapy.Request(url.strip(),headers=self.headers,callback=self.slider_screenshot)


    async def slider_screenshot(self,response):
        if response.xpath("//script[@id='navigation-fragment-state']/text()").get():
            json_data=json.loads(response.xpath("//script[@id='navigation-fragment-state']/text()").get({}))
            for data in json_data.get("fragmentContextData",{}).get("NavigationPage",{}).get("parfumsInitialData",{}).get("pageComponents",[]):
                if  data.get("data",None) is not None:
                    for slider_block in  data.get("data",{}).get("items",[]):
                        if isinstance(slider_block.get("image",),dict):
                            image_url=slider_block.get("image",{}).get("desktop",'')
                            if image_url:
                                image_url=f'https://cdn.notinoimg.com/images{image_url}'
                                redirection_url=response.urljoin(slider_block.get("link",''))
                                hash_obj = hashlib.md5(redirection_url.encode("utf-8"))
                                filename = hash_obj.hexdigest()+".jpg"
                                if not os.path.exists(f"{self.screenshot_folder}/{filename}"):
                                    image_response=await self.request(image_url)
                                    with open(f"{self.screenshot_folder}/{filename}",'wb') as f:f.write(image_response.body)
                            slider={"format": "Slider",
                            "page_url": response.url,
                            "image_url": re.sub("\s+|\[|\]","",image_url.replace("order_2k",'detail_zoom').replace("`","%60")),
                            "screenshot": filename,
                            "redirection_url": redirection_url,
                            "master_products": []
                            }
                            yield scrapy.Request(redirection_url,headers=self.headers,callback=self.product_listing,cb_kwargs={"item":slider},dont_filter=True)


    async def product_listing(self,response,item={}):
        for block in response.xpath("//div[@class='styled__PageGridWrapper-sc-1yds6ou-0 ediuOu']/div[@data-testid='product-container']/a"):
            product_url=response.urljoin(block.xpath("./@href").get(""))
            request = scrapy.Request(product_url,headers=self.headers,dont_filter=True)
            product_response = await self.crawler.engine.download(request, self)
            data={}
            if product_response.xpath("//script[@type='application/ld+json']/text()").get():
                json_data=json.loads(product_response.xpath("//script[@type='application/ld+json']/text()").get('{}'))
                data["product_id"]=json_data.get("sku",None)
                data["name"] = product_response.xpath("//span[@class='sc-3sotvb-4 kSRNEJ']/text()").get()
                data["brand"]=html.unescape(json_data.get("brand",{}).get("name",""))
                data["url"] =product_url
                data['price']=  ' '.join(product_response.xpath("//div[@id='pd-price']//text()").getall())
                data['image_url']=[re.sub("\s+|\[|\]","",image_url.replace("order_2k",'detail_zoom').replace("`","%60")) for image_url in json_data.get("image",[])]
                item['master_products'].append(data.copy())
        next_page=response.xpath("//link[@rel='next']/@href").get()
        if next_page:
            yield scrapy.Request(next_page,headers=self.headers, callback=self.product_listing,cb_kwargs={"item":item})
        else:
            yield item

    async def request(self,url):
        """ scrapy request"""
        request = scrapy.Request(url,headers=self.headers,dont_filter=True)
        response = await self.crawler.engine.download(request,self)
        return response