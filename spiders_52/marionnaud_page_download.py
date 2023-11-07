import os
import datetime
import scrapy
import hashlib
from pathlib import Path
from dotenv import load_dotenv
import requests
import json




class MarionnaudFrMediaSpider(scrapy.Spider):
    name = "marionnaud_page_download"
    dir_path=os.path.abspath(__file__ + "/../../../")
    try:
        load_dotenv()
    except:
        env_path = Path(".env")
        load_dotenv(dotenv_path=env_path)
    
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    screenshot_folder=os.getcwd()+rf"/exports/marionnaud_fr_media/screenshots_{DATE}"
    if not os.path.exists(screenshot_folder):
        os.makedirs(screenshot_folder)
    

    def __init__(self, *args, **kwargs):
        self.url=kwargs.get("url")
        super().__init__(*args, **kwargs)

    ROTATING_PROXY = os.getenv("ROTATING_PROXY_FR")
    proxies={'http':ROTATING_PROXY,'https':ROTATING_PROXY}
    custom_settings={ "CONCURRENT_REQUESTS":100,
        'ROTATING_PROXY_LIST' : [ROTATING_PROXY],
        "HTTPCACHE_ENABLED" : True,
        "HTTPCACHE_DIR" : 'httpcache',
        'HTTPCACHE_EXPIRATION_SECS':79200,
        "HTTPCACHE_STORAGE" : "scrapy.extensions.httpcache.FilesystemCacheStorage",
        'EXTENSIONS':{},
        'LOG_FILE':None,
        'ROTATING_PROXY_PAGE_RETRY_TIMES':1,
        'DOWNLOAD_TIMEOUT':15,
        # 'DOWNLOADER_CLIENT_TLS_METHOD':'TLSv1.2'        
        }
    
    headers = {
    'accept-encoding': 'gzip, deflate, br',
  'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
}


    def start_requests(self):
        MarionnaudFrMediaSpider.name="marionnaud_fr_media"
        if self.url=='https://www.marionnaud.fr/':
            yield scrapy.Request(self.url,callback=self.homepage,headers=self.headers,meta={'proxy': self.ROTATING_PROXY})
        else:
            yield scrapy.Request(self.url,callback=self.parse,headers=self.headers,meta={'proxy': self.ROTATING_PROXY})

    def homepage(self,response):
        
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
            yield scrapy.Request(redirection_url,callback=self.homepage_product_listing,meta={'proxy': self.ROTATING_PROXY})
        #         # # # 3 Banners in the middle 
        
        for block in response.xpath("//div[@class='wrapper_SliderTrioBig']/div/a"):
            redirection_url=(block.xpath('.//@href').get(''))
            image_url = response.urljoin(block.xpath('.//img/@src').get(""))
            hash_obj = hashlib.md5(image_url.encode("utf-8"))
            filename = hash_obj.hexdigest()+".png"
            if not os.path.exists(f"{self.screenshot_folder}/{filename}"):
                self.image_download(image_url,filename)
            yield scrapy.Request(redirection_url,callback=self.homepage_product_listing,meta={'proxy': self.ROTATING_PROXY})
        
        for block in response.xpath('//section[@class="carousel"]//a'):
            redirection_url = block.xpath('.//@href').get('')
            image_url= block.xpath('./img/@src').get('')
            hash_obj = hashlib.md5(image_url.encode("utf-8"))
            filename = hash_obj.hexdigest()+".png"
            if not os.path.exists(f"{self.screenshot_folder}/{filename}"):
                self.image_download(image_url,filename)
            yield scrapy.Request(redirection_url,callback=self.homepage_product_listing,meta={'proxy': self.ROTATING_PROXY})

    def parse(self,response):
        for block in response.xpath('//div[@class="carousel-inner"]/div'):
            redirection_url=(block.xpath('./a/@href').get(''))
            if block.xpath('.//img/@src').get(""):
                image_url = response.urljoin(block.xpath('.//img/@src').get(""))
            elif json.loads(block.xpath('.//img/@data-media').get({})).get('992',''):
                image_url=response.urljoin(json.loads(block.xpath('.//img/@data-media').get({})).get('992',''))
            hash_obj = hashlib.md5(redirection_url.encode("utf-8"))
            filename = hash_obj.hexdigest()+".png"
            if not os.path.exists(f"{self.screenshot_folder}/{filename}"):
                self.image_download(image_url,filename)
            yield scrapy.Request(redirection_url,callback=self.homepage_product_listing,meta={'proxy': self.ROTATING_PROXY})
        
    def image_download(self,url,filename):
        response=requests.get(url,headers=self.headers,proxies=self.proxies)
        with open(f"{self.screenshot_folder}/{filename}",'wb') as f:f.write(response.content)
        
    
    def homepage_product_listing(self, response):
        blocks=response.xpath('//ul[@class="product-listing product-grid"]/li')
        if blocks:
            for block in blocks:
                product_url=response.urljoin(block.xpath(".//@href").get(''))
                yield scrapy.Request(product_url,callback=self.parse_product,meta={'proxy': self.ROTATING_PROXY})
            curent_page=int(response.xpath("//*[@id='js_currentPage']/@value").get('0').strip())
            total_page=int(response.xpath("//*[@id='js_numberofPage']/@value").get('0').strip())
            if curent_page<total_page:
                next_page_link =response.urljoin(f'?page={curent_page}')
                yield scrapy.Request(next_page_link,callback=self.homepage_product_listing,meta={'proxy': self.ROTATING_PROXY})

    def parse_product(self,response):
        pass

                


