import re
import os
import json
import time
import shutil
import scrapy
import hashlib
import logging
import datetime
from pathlib import Path
from scrapy import signals
from parsel import Selector
from dotenv import load_dotenv
import traceback2 as traceback
from playwright.sync_api import sync_playwright



class SephoraUkMediaSpider(scrapy.Spider):
    name = 'sephora_uk_media'
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

    custom_settings={
                    "CONCURRENT_REQUESTS":32,
                    # "HTTPCACHE_ENABLED" : True,
                    # "HTTPCACHE_DIR" : 'httpcache',
                    # 'HTTPCACHE_EXPIRATION_SECS':86400,
                    # "HTTPCACHE_STORAGE" : "scrapy.extensions.httpcache.FilesystemCacheStorage",
                    # 'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/sephora_uk/%(name)s_{DATE}.json": {"format": "json",}},
                    'ROTATING_PROXY_LIST' : [ROTATING_PROXY]
                    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(SephoraUkMediaSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signals.spider_closed)
        return spider

    def spider_opened(self):
        headers = {
                    'authority': 'www.sephora.co.uk',
                    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'accept-language': 'en-US,en;q=0.9',
                    # 'cache-control': 'max-age=0',
                    # 'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
                    # 'sec-ch-ua-mobile': '?0',
                    # 'sec-ch-ua-platform': '"Windows"',
                    # 'sec-fetch-dest': 'document',
                    # 'sec-fetch-mode': 'navigate',
                    # 'sec-fetch-site': 'none',
                    # 'sec-fetch-user': '?1',
                    # 'upgrade-insecure-requests': '1',
                    'Content-Encoding':'gzip',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
                    }
        self.spider_name=self.name
        SephoraUkMediaSpider.name="sephora_uk_pdp"
        playwright = sync_playwright().start()
        browser = playwright.chromium.launch(headless=True,args=["--start-maximized"],
                                             proxy={
        "server": "brd.superproxy.io:22225",
        "username": "brd-customer-hl_fb2f275a-zone-datagram_residential-country-uk",
        "password": "p30epw6kjmb2",
    },
    )
        
        self.context = browser.new_context(java_script_enabled=True,extra_http_headers=headers)
        # self.context = browser.new_context(java_script_enabled=True)
        self.page = self.context.new_page()
        # self.page.set_viewport_size({"width": 1600, "height": 1200})
        self.page.goto('https://www.sephora.co.uk/')
        self.page.wait_for_timeout(3000)
        self.page.locator('//div[@id="notice-content"]/div[@class="right"]/a').click()
        # stealth_sync(self.page)
    
    def spider_closed(self, spider):
        self.page.close()
        if os.path.exists(self.screenshot_folder):
            shutil.make_archive(os.getcwd()+rf"/exports/{self.spider_name}/"+f"screenshots_{self.DATE}", 'zip', self.screenshot_folder)
            screenshot_zip_file=os.getcwd()+rf"/exports/{self.spider_name}/"+f"screenshots_{self.DATE}.zip"
            # if os.path.exists(screenshot_zip_file):
                # os.system(f"aws s3 cp {screenshot_zip_file}  s3://scraping-external-feeds-lapis-data/{self.CURRENT_DATE}/sephora_uk/")
            
    def home_page_process(self,main_url,image_url,response,media_format):
        
        hash_obj = hashlib.md5(image_url.encode("utf-8"))
        filename = hash_obj.hexdigest()+".png"
        if not os.path.exists(f"screenshots/{filename}"):
            image_url = image_url.split('?')[0]
            # self.page.goto('https://www.sephora.co.uk/')
            
            self.page.goto(image_url,wait_until='load')
            try:
                time.sleep(3)
                self.page.locator('//img').screenshot(path=f"{self.screenshot_folder}/{filename}")
            except:
                logging.warning("ERROR AT LINE 95")
            # self.page.close()
        item = {}
        item['format'] = media_format
        item['page_url'] = response.url
        item['image_url'] = image_url
        item['screenshot'] = filename
        item['redirection_url'] = response.urljoin(main_url)
        item['products_count'] = 0
        item['master_products'] = []
        return main_url,item
    
    def start_requests(self):
        homepage_url="https://www.sephora.co.uk/"
        yield scrapy.Request(homepage_url,headers=self.headers,callback=self.parse)
        

    async def sponsored_listing_page(self, media_search_url,item):
        try:
            # page1 = self.context.new_page()
            # page1.wait_for_timeout(3000)
            logging.warning("before_page_ping_media",media_search_url)
            self.page.goto(media_search_url,wait_until='load',timeout=100000)
            self.page.wait_for_timeout(3000)
            # breakpoint()
        
            # page1.wait_for_timeout(10000)
            media_search_selector = Selector(text=self.page.content())
            count = 1
            item['format'] = 'Sponsored Products'
            if 'q=' in media_search_url:
                item['title'] = media_search_url.split('q=')[1].replace('+',' ').capitalize().strip()
            else:
                item['title'] = media_search_url.split('/')[-1].strip()
            item['page_url'] = media_search_url.strip()
            item['master_products'] = []
            for sponsored_listing in media_search_selector.xpath('//div[@class="Product Product- sponsored"]'):
                sponsored_url = sponsored_listing.xpath('./a/@href').get()
                hash_obj = hashlib.md5(sponsored_url.encode("utf-8"))
                filename = hash_obj.hexdigest()+".png"
                try:
                    time.sleep(3)
                    self.page.locator(f'//div[@class="Product Product- sponsored"][{count}]').screenshot(path=f"{self.screenshot_folder}/{filename}")
                    self.page.locator(f'//div[@class="Product Product- sponsored"][{count}]').screenshot(path=f"{self.screenshot_folder}/{filename}")
                except:
                    logging.warning("ERROR AT LINE 121")
                sponsored_url = f'https://www.sephora.co.uk{sponsored_url}'
                product_response = await self.requests_process((sponsored_url))
                data = {}
                data["product_id"] = None
                if  re.search('\"data\"\:\[\"([^>]*?)\"\]',product_response.text):
                    data["product_id"] = re.findall('\"data\"\:\[\"([^>]*?)\"\]',product_response.text)[0]
                data['position'] = count
                data['screenshot'] = filename
                data["name"] =  product_response.xpath('//h1/span[@class="pdp-product-brand-name"]/text()').get('').strip()
                data['brand'] = None
                if re.search(r'<script\s*type\=\"application\/ld\+json\"[^>]*?>[^>]*?\"brand\"\:\s*\"([^>]*?)\"[^>]*?<\/script>',product_response.text):
                    data['brand'] = re.findall(r'<script\s*type\=\"application\/ld\+json\"[^>]*?>[^>]*?\"brand\"\:\s*\"([^>]*?)\"[^>]*?<\/script>',product_response.text)[0] 
                    
                data["url"] = sponsored_url
                data['price'] = None
                if product_response.xpath('//p[@class="price-info"]/span/span/text()'):
                    data['price'] = ''.join(product_response.xpath('//p[@class="price-info"]/span/span/text()').getall()).strip()
                    
                image_url_list = list(set(product_response.xpath("//div[contains(@class,'productpage-gallery')]/div/a/@href|//div[contains(@class,'productpage-image')]/img[1]/@src").getall()))
                image_urls = [ url.replace(' ','%20') for url in image_url_list if not 'youtube' in url ]
                data['image_url'] = image_urls
                item['master_products'].append(data.copy())
                count+=1
        except Exception as e:
            logging.warning("error_page_ping_media",media_search_url)
            logging.warning("ERROR AT LINE 116",traceback.format_exc())
        # page1.close()
        logging.warning("completed search url",media_search_url)
        logging.warning('completed item',item)
        return item
        
    

    async def parse(self, response):
        for main_silder in response.xpath('//div[@id="fullwidth"]/div/div'):
            main_silder_image_block = main_silder.xpath('./@data-hero-sephora-slide').get('')
            main_silder_url = main_silder.xpath('./a/@href').get('')
            main_silder_json = json.loads(main_silder_image_block)
            main_silder_image_url = main_silder_json.get('pwaImgDpr1')
            media_format = 'Main slider'
            main_silder_url,item =  self.home_page_process(main_silder_url,main_silder_image_url,response,media_format)
            yield scrapy.Request(response.urljoin(main_silder_url),callback=self.homepage_product_listing,cb_kwargs={"item":item})
        for main_banner in response.xpath('//div[contains(@class,"u-soft")]/div/a'):
            main_banner_url = main_banner.xpath('./@href').get()
            main_banner_image_url = main_banner.xpath('./div/div[@class="Media-image"]/img/@data-src').get()
            media_format = 'Banner'
            main_url,item = self.home_page_process(main_banner_url,main_banner_image_url,response,media_format)
            yield scrapy.Request(response.urljoin(main_url),callback=self.homepage_product_listing,cb_kwargs={"item":item})
        for main_brand in response.xpath('//div[@class="Brands h-push"]/a'):
            main_brand_url = main_brand.xpath('./@href').get()
            main_image_url = main_brand.xpath('./img/@data-src').get()
            media_format = 'Sponsored Brand'
            main_url,item = self.home_page_process(main_brand_url,main_image_url,response,media_format)
            yield scrapy.Request(response.urljoin(main_url),callback=self.homepage_product_listing,cb_kwargs={"item":item})
        """ read loccitane search url text file"""
        dir_path=os.path.abspath(__file__ + "/../../../")
        supporting_files=os.path.join(dir_path,"supporting_files")
        with open(f'{supporting_files}/sephora_uk_media_input.txt') as f:
            media_search_urls=f.readlines()
        for media_search_url in media_search_urls:
            item = {} 
            logging.warning("before_function_media",media_search_url)
            item =  await self.sponsored_listing_page(media_search_url,item)
            logging.warning("final item",item)
            if item:
                yield item

    async def homepage_product_listing(self, response,item):
        products_count = ' '.join(response.xpath('//div[@class="eba-product-count"]/div/div/i/text()').getall()).replace('Showing   of ','')
        product_links = response.xpath('//div[@class="eba-component eba-product-listing"]/div[contains(@class,"Product")]/a/@href').getall()
        
        if item['products_count'] == 0:
            item['products_count'] = products_count if products_count else len(product_links)
        
        for product_link in product_links:
            try:
                product_response = await self.requests_process(response.urljoin(product_link))
                data = {}
                data["product_id"] = None
                if  re.search('\"data\"\:\[\"([^>]*?)\"\]',product_response.text):
                    data["product_id"] = re.findall('\"data\"\:\[\"([^>]*?)\"\]',product_response.text)[0]
                data["name"] =  product_response.xpath('//h1/span[@class="pdp-product-brand-name"]/text()').get('').strip()
                data['brand'] = None
                if re.search(r'<script\s*type\=\"application\/ld\+json\"[^>]*?>[^>]*?\"brand\"\:\s*\"([^>]*?)\"[^>]*?<\/script>',product_response.text):
                    data['brand'] = re.findall(r'<script\s*type\=\"application\/ld\+json\"[^>]*?>[^>]*?\"brand\"\:\s*\"([^>]*?)\"[^>]*?<\/script>',product_response.text)[0] 
                data["url"] = response.urljoin(product_link)
                data['price'] = None
                if product_response.xpath('//p[@class="price-info"]/span/span/text()'):
                    data['price'] = ''.join(product_response.xpath('//p[@class="price-info"]/span/span/text()').getall()).strip()
                image_url_list = list(set(product_response.xpath("//div[contains(@class,'productpage-gallery')]/div/a/@href|//div[contains(@class,'productpage-image')]/img[1]/@src").getall()))
                image_urls = [ url.replace(' ','%20') for url in image_url_list if not 'youtube' in url ]
                data['image_url'] = image_urls
                item['master_products'].append(data.copy())
            except:
                pass
        next_page=response.xpath('//div[@class="loadMore loadMoreBottom"]/a/@href').get("").strip()
        if next_page:
            next_page=response.urljoin(next_page)
            yield scrapy.Request(next_page,headers=self.headers, callback=self.homepage_product_listing,cb_kwargs={"item":item},dont_filter=True)
        else:
            yield item

    async def requests_process(self,url):
        request = scrapy.Request(url)
        response = await self.crawler.engine.download(request, self)
        return response