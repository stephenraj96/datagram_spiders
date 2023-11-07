import scrapy
from playwright.sync_api import sync_playwright
from urllib.parse import urljoin
import hashlib
import time
from scrapy import signals
import os
import datetime
from dotenv import load_dotenv
from pathlib import Path
import logging
import shutil
import re


class LookfantasticSpider(scrapy.Spider):
    name = 'lookfantastic_uk_media'
    headers = {
            'accept-encoding': 'gzip, deflate, br',
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
    ROTATING_PROXY = os.getenv("ROTATING_PROXY")
    screenshot_folder=os.getcwd()+rf"/exports/{name}/screenshots_{DATE}"
    if not os.path.exists(screenshot_folder):
        os.makedirs(screenshot_folder)

    temporary_files=os.getcwd()+"/temporary_files"
    if not os.path.exists(temporary_files):
        os.makedirs(temporary_files)

    custom_settings={
                    "HTTPCACHE_ENABLED" : True,
                    "CONCURRENT_REQUESTS":1,
                    "HTTPCACHE_DIR" : 'httpcache',
                    'HTTPCACHE_EXPIRATION_SECS':79200,
                    "HTTPCACHE_STORAGE" : "scrapy.extensions.httpcache.FilesystemCacheStorage",
                    'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/lookfantastic_uk/%(name)s_{DATE}.json": {"format": "json",}},
                    'ROTATING_PROXY_LIST' : [ROTATING_PROXY]
                    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(LookfantasticSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signals.spider_closed)
        return spider

    def spider_opened(self):
        self.spider_name=self.name
        LookfantasticSpider.name="lookfantastic_uk_pdp"
        playwright = sync_playwright().start()
        browser = playwright.chromium.launch(headless=True,args=["--start-maximized"])
        self.context = browser.new_context(java_script_enabled=True)
        self.page = self.context.new_page()
        self.page.set_viewport_size({"width": 1600, "height": 1200})
    
    def spider_closed(self, spider):
        self.page.close()
        if os.path.exists(self.screenshot_folder):
            shutil.make_archive(os.getcwd()+rf"/exports/{self.spider_name}/"+f"screenshots_{self.DATE}", 'zip', self.screenshot_folder)
            screenshot_zip_file=os.getcwd()+rf"/exports/{self.spider_name}/"+f"screenshots_{self.DATE}.zip"
            if os.path.exists(screenshot_zip_file):
                os.system(f"aws s3 cp {screenshot_zip_file}  s3://scraping-external-feeds-lapis-data/{self.CURRENT_DATE}/lookfantastic_uk/")
            
    def start_requests(self):
        homepage_url="https://www.lookfantastic.com/"
        yield scrapy.Request(homepage_url,headers=self.headers,callback=self.home_page)
        

    async def sponsored_listing_page(self, response,rank=0,sponsored_products={}):
        text=response.text.replace("\n",'')
        with open(f"{self.temporary_files}/lookfantastic_uk.html","w") as f:f.write(text)
        try:
            self.page.goto(f"file://{self.temporary_files}/lookfantastic_uk.html")
            time.sleep(3)
            self.page.wait_for_selector('//button[@id="onetrust-accept-btn-handler"]').click()
        except:
            logging.warning("ERROR AT LINE 87")
        if rank==0:
            sponsored_products ={
                    "format": "Sponsored Products",
                    "title":  response.url ,
                    "page_url": response.url,
                    "master_products": []
                    }
        for count,block in enumerate(response.xpath('//li[contains(@class,"productListProducts_product")]'),start=1):
            rank+=1
            if block.xpath("./self::li[contains(@class,'sponsoredProductsList')]"):
                product_url=urljoin("https://www.lookfantastic.com/",block.xpath(".//a[@class='productBlock_link']/@href").get(''))
                hash_obj = hashlib.md5(product_url.encode("utf-8"))
                filename = hash_obj.hexdigest()+".png"
                if not os.path.exists(f"{self.screenshot_folder}/{filename}"):
                    try:
                        time.sleep(2)
                        self.page.locator(f"li.sponsoredProductsList:nth-child({count+1})").screenshot(path=f"{self.screenshot_folder}/{filename}")
                    except:
                        logging.warning("ERROR AT LINE 106")
                        continue
                request = scrapy.Request(product_url,headers=self.headers,dont_filter=True)
                product_response = await self.crawler.engine.download(request, self)
                item = {}
                item["product_id"]= product_response.xpath("//div[@class='productPrice']/@data-product-id").get("").strip()
                item["name"] = product_response.xpath("//*[@class='productName_title']/text()").get("").strip()
                item["brand"]= product_response.xpath("//div[@class='productBrandLogo']/img/@title").get()
                if item['brand'] is None:
                    if re.findall("productBrand: \"(.*)\"",product_response.text):
                        item['brand']=re.findall("productBrand:\s*\"(.*)\"",product_response.text)[0]
                item["url"] =product_url
                item['image_url']=product_response.xpath("//*[@data-size='1600']/@data-path").getall()
                item['price']=  product_response.xpath("//*[@class='productPrice_price  ']/text()").get('').strip()
                item['screenshot']=filename
                item['position']=rank
                if os.path.exists(f"{self.screenshot_folder}/{filename}"):
                    sponsored_products['master_products'].append(item.copy())
        yield sponsored_products
        
        
            

    async def home_page(self, response):
        text=response.text.replace("\n",'')
        with open(f"{self.temporary_files}/lookfantastic_uk.html","w") as f:f.write(text)
        self.page.goto(f"file://{self.temporary_files}/lookfantastic_uk.html")
        try:
            self.page.wait_for_selector('//button[@id="onetrust-accept-btn-handler"]').click()
            self.page.wait_for_selector('//button[@class="emailReengagement_close_button"]').click()
        except:
            pass
        self.page.mouse.wheel(0, 150)
        time.sleep(5)
        slide_count=len(response.css("ul.responsiveSlider_navBullets li"))
        main_slider_list=[]
        main_slider_checklist=[]
        for count,block in enumerate(response.css("div.responsiveSlider_slideContainer"),start=1):
            redirection_url=urljoin("https://www.lookfantastic.com/",block.xpath(".//a/@href").get(''))
            hash_obj = hashlib.md5(redirection_url.encode("utf-8"))
            filename = hash_obj.hexdigest()+".png"
            try:
                self.page.locator(f"div.responsiveSlider_slideContainer:nth-child({count}) > div").screenshot(path=f"{self.screenshot_folder}/{filename}")
            except:
                logging.warning("ERROR AT LINE 152")
                continue
            main_slider={"format": "Main Slider",
            "page_url": response.url,
            "image_url": block.xpath(".//img[@class='primaryBanner_imageLarge primaryBanner_imageLarge-left']/@src").get(""),
            "screenshot": filename,
            "redirection_url": redirection_url,
            "master_products": []
            }
            if not filename in main_slider_checklist:
                main_slider_checklist.append(filename)
                main_slider_list.append(main_slider.copy())
        for main in main_slider_list:
            redirection_url=main.get("redirection_url",None)
            yield scrapy.Request(redirection_url,headers=self.headers,callback=self.homepage_product_listing,cb_kwargs={"item":main},dont_filter=True)
        if response.css('div.responsiveSlot2 div.primaryBanner_container'):
            middle_banner_url=urljoin("https://www.lookfantastic.com/",response.xpath("//div[@class='responsiveSlot2']//a/@href").get(""))
            middle_image_url= response.xpath("//div[@class='responsiveSlot2']//source/@srcset").get("")
            hash_obj = hashlib.md5(middle_image_url.encode("utf-8"))
            filename = hash_obj.hexdigest()+".png"
            self.page.locator(f"div.responsiveSlot2 div.primaryBanner_container").screenshot(path=f"{self.screenshot_folder}/{filename}")
            middle_banner={"format": "Banner",
                "page_url": response.url,
                "image_url": middle_image_url,
                "screenshot": filename,
                "redirection_url": middle_banner_url,
                "master_products": []
                }
            yield scrapy.Request(middle_banner_url,headers=self.headers,callback=self.homepage_product_listing,cb_kwargs={"item":middle_banner},dont_filter=True)
        if response.css('div.responsiveSlot5 div.primaryBanner_container'):
            middle_banner_url=urljoin("https://www.lookfantastic.com/",response.xpath("//div[@class='responsiveSlot5']//a/@href").get(""))
            middle_image_url= response.xpath("//div[@class='responsiveSlot5']//source/@srcset").get("")
            hash_obj = hashlib.md5(middle_image_url.encode("utf-8"))
            filename = hash_obj.hexdigest()+".png"
            self.page.locator(f"div.responsiveSlot5 div.primaryBanner_container").screenshot(path=f"{self.screenshot_folder}/{filename}")
            middle_banner={"format": "Banner",
                "page_url": response.url,
                "image_url": middle_image_url,
                "screenshot": filename,
                "redirection_url": middle_banner_url,
                "master_products": []
                }
            yield scrapy.Request(middle_banner_url,headers=self.headers,callback=self.homepage_product_listing,cb_kwargs={"item":middle_banner},dont_filter=True)

        banner_list=[]
        for count,block in enumerate(response.xpath("//div[@class='threeItemEditorial_item']"),start=1):
            redirection_url=urljoin("https://www.lookfantastic.com/",block.xpath(".//a/@href").get(''))
            hash_obj = hashlib.md5(redirection_url.encode("utf-8"))
            filename = hash_obj.hexdigest()+".png"
            if count<4:
                row1=count
                class_name="responsiveSlot10"
            else:
                row1=count-3
                class_name="responsiveSlot11"
            try:
                self.page.locator(f"div.{class_name} > div > div > div:nth-child({row1})").screenshot(path=f"{self.screenshot_folder}/{filename}")
            except :
                logging.warning("ERROR AT LINE 210")
                continue
            time.sleep(2)
            banner={"format": "Banner",
            "page_url": response.url,
            "image_url": block.xpath(".//img/@src").get(""),
            "screenshot": filename,
            "redirection_url": redirection_url,
            "master_products": []
            }
            banner_list.append(banner.copy())
        for banner in banner_list:
            redirection_url=banner.get("redirection_url",None)
            yield scrapy.Request(redirection_url,headers=self.headers,callback=self.homepage_product_listing,cb_kwargs={"item":banner})
        showcase_url=urljoin("https://www.lookfantastic.com/",response.xpath("//div[@class='promoProductSlider_container']/a/@href").get(""))
        showcase_image_url=urljoin("https://www.lookfantastic.com/",response.xpath("//div[@class='promoProductSlider_container']//img/@src").get(""))
        hash_obj = hashlib.md5(showcase_image_url.encode("utf-8"))
        filename = hash_obj.hexdigest()+".png"
        try:
            self.page.locator(f"div.promoProductSlider_container").screenshot(path=f"{self.screenshot_folder}/{filename}")
        except:
            logging.warning("ERROR AT LINE 231")
        showcase={"format": "ShowCase",
            "page_url": response.url,
            "image_url": showcase_image_url,
            "screenshot": filename,
            "redirection_url": showcase_url,
            "master_products": []
            }
        yield scrapy.Request(showcase_url,headers=self.headers,callback=self.homepage_product_listing,cb_kwargs={"item":showcase},dont_filter=True)
        supporting_files=os.path.join(self.dir_path,"supporting_files")
        with open(f'{supporting_files}/lookfantastic_urls_media.txt') as f:urls = f.readlines()
        for url in urls:
            yield scrapy.Request(url.strip(),headers=self.headers,callback=self.sponsored_listing_page,cb_kwargs={"rank":0,"sponsored_products":{}})

    async def homepage_product_listing(self, response,item):
        for count,block in  enumerate(response.xpath("//li[contains(@class,'productListProducts_product')]"),1):
            product_url=response.urljoin(block.xpath(".//a[@class='productBlock_link']/@href").get(''))
            request = scrapy.Request(product_url,headers=self.headers,dont_filter=True)
            product_response = await self.crawler.engine.download(request, self)
            data = {}
            data["product_id"]= product_response.xpath("//div[@class='productPrice']/@data-product-id").get("").strip()
            data["name"] = product_response.xpath("//*[@class='productName_title']/text()").get("").strip()
            data["brand"]= product_response.xpath("//div[@class='productBrandLogo']/img/@title").get()
            if data['brand'] is None:
                if re.findall("productBrand: \"(.*)\"",product_response.text):
                    data['brand']=re.findall("productBrand:\s*\"(.*)\"",product_response.text)[0]
            data["url"] =product_url
            data['price']=  product_response.xpath("//*[@class='productPrice_price  ']/text()").get('').strip()
            data['image_url']=product_response.xpath("//*[@data-size='1600']/@data-path").getall()
            if data["name"]:
                item['master_products'].append(data.copy())
        next_page=response.xpath("//a[@aria-current='true']/parent::li/following-sibling::li[1]/a/@href").get("").strip()
        if next_page:
            next_page=response.urljoin(next_page)
            yield scrapy.Request(next_page,headers=self.headers, callback=self.homepage_product_listing,cb_kwargs={"item":item},dont_filter=True)
        else:
            yield item