import re
import os
import json
import time
import shutil
import scrapy
import hashlib
import logging
import asyncio
import datetime
from pathlib import Path
from scrapy import signals
from parsel import Selector
from dotenv import load_dotenv
# from playwright_stealth import stealth_sync
from playwright.sync_api import sync_playwright



class BodegaMxMediaSpider(scrapy.Spider):
    name = 'bodega_mx_media'
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
    api_response = []
    banner_api_response = []
    if not os.path.exists(screenshot_folder):
        os.makedirs(screenshot_folder)

    custom_settings={
                    "CONCURRENT_REQUESTS":32,
                    'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/bodega_mx/%(name)s_{DATE}.json": {"format": "json",}},
                    'ROTATING_PROXY_LIST' : [ROTATING_PROXY]
                    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(BodegaMxMediaSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(spider.spider_closed, signals.spider_closed)
        return spider

    def spider_opened(self):
        headers = {
                    # 'authority': 'www.sephora.co.uk',
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
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
                    }
        self.spider_name=self.name
        # SephoraUkMediaSpider.name="sephora_uk_pdp"
        playwright = sync_playwright().start()
        browser = playwright.chromium.launch(headless=True,args=["--start-maximized"],
    #                                          proxy={
    #     "server": "zproxy.lum-superproxy.io:22225",
    #     "username": "brd-customer-hl_fb2f275a-zone-datagram-country-us",
    #     "password": "pcfc2c07ek55",
    # },
    )
        
        self.context = browser.new_context(java_script_enabled=True,extra_http_headers=headers,no_viewport=True)
        # self.context = browser.new_context(java_script_enabled=True)
        self.page = self.context.new_page()
        # self.page.set_viewport_size({"width": 1600, "height": 1200})
        self.page.on("response", lambda response: asyncio.create_task(self.api_store_id(response)))
        self.page.goto('https://despensa.bodegaaurrera.com.mx/',wait_until='load')
        self.page.wait_for_timeout(40000)
        
        
        # stealth_sync(self.page)
    
    def spider_closed(self, spider):
        self.page.close()
        if os.path.exists(self.screenshot_folder):
            shutil.make_archive(os.getcwd()+rf"/exports/{self.spider_name}/"+f"screenshots_{self.DATE}", 'zip', self.screenshot_folder)
            screenshot_zip_file=os.getcwd()+rf"/exports/{self.spider_name}/"+f"screenshots_{self.DATE}.zip"
            if os.path.exists(screenshot_zip_file):
                os.system(f"aws s3 cp {screenshot_zip_file}  s3://scraping-external-feeds-lapis-data/{self.CURRENT_DATE}/bodega_mx/")
            
    def start_requests(self):
        homepage_url="https://despensa.bodegaaurrera.com.mx/"
        yield scrapy.Request(homepage_url,headers=self.headers,callback=self.parse)
        
    async def banner_collection(self,media_search_url,item):
        self.page.on("response", lambda response: asyncio.create_task(self.banner_link(response)))
        self.page.goto(media_search_url,wait_until='load')
        for page_down_count in range(25):
            self.page.keyboard.press('PageDown')
            self.page.wait_for_timeout(2000)
        for page_up_count in range(25):
            self.page.keyboard.press('PageUp')
            self.page.wait_for_timeout(2000)
        
        for banner_api_link in self.banner_api_response:
            banner_response_body = await self.requests_process(banner_api_link)
            image_url,banner_silder_url = None,None
            if re.search(r"https\:\/\/securepubads\.g\.doubleclick\.net\/gampad\/ads\?",banner_api_link):
                if re.search(r'<img\s*id\=\"myImage\"\s*onclick\=\"openURL\(\)\"\s*src\=\"([^>]*?)"',str(banner_response_body.text)):
                    image_url = re.findall(r'<img\s*id\=\"myImage\"\s*onclick\=\"openURL\(\)\"\s*src\=\"([^>]*?)"',str(banner_response_body.text))[0]
                if re.search(r'window\.open\(\"([^>]*?)\"',str(banner_response_body.text)):
                    banner_silder_url = re.findall(r'window\.open\(\"([^>]*?)\"',str(banner_response_body.text))[0]
            else:
                index_response = Selector(text=banner_response_body.text)
                image_url = index_response.xpath('//meta[@name="banner.thumbnail"]/@content').get('')
                if re.search('\"global_hyperlinkValue\"\:\"([^>]*?)\"',banner_response_body.text):
                    banner_silder_url = re.findall('\"global_hyperlinkValue\"\:\"([^>]*?)\"',banner_response_body.text)[0]
            if image_url and banner_silder_url:
                if re.search('http[^>]*?(http[^>]*?)$',banner_silder_url):
                    banner_silder_url = re.findall('http[^>]*?(http[^>]*?)$',banner_silder_url)[0]
                print("main_silder_url",banner_silder_url)
                print("image_url",image_url)
                hash_obj = hashlib.md5(image_url.encode("utf-8"))
                filename = hash_obj.hexdigest()+".png"
                image_response = await self.requests_process(image_url)
                with open(f"{self.screenshot_folder}/{filename}",'wb') as f:f.write(image_response.body)
                item = {}
                item = await self.banner_silder(item,banner_silder_url,image_url,filename,media_search_url)
                return item

    async def banner_silder(self,item,banner_silder_url,image_url,filename,media_search_url):
        main_silder_api_url = "https://nextgentheadless.instaleap.io/api/v2"
        if re.search(r'name\=([^>]*?)\&',banner_silder_url):
            banner_silder_name = re.findall(r'name\=([^>]*?)\&',banner_silder_url)[0]
        elif re.search(r'name\%3D([^>]*?)\%26',banner_silder_url):
            banner_silder_name = re.findall(r'name\%3D([^>]*?)\%26',banner_silder_url)[0]
        else:
            breakpoint()
        print("banner_silder_name",banner_silder_name)
        payload = json.dumps([
                            {
                                "operationName": "GetProducts",
                                "variables": {
                                "filter": {},
                                "pagination": {
                                    "pageSize": 100,
                                    "currentPage": 1
                                },
                                "orderBy": {},
                                "search": {
                                    "text": banner_silder_name,
                                    "language": "ES"
                                },
                                "showProductsWithoutStock": True,
                                "storeId": "565"
                                },
                                "query": "fragment BaseProductV2 on Product {\n  id\n  description\n  name\n  brand\n  photosUrls\n  sku\n  unit\n  price\n  specialPrice\n  promotion {\n    description\n    type\n    isActive\n    conditions\n    __typename\n  }\n  variants {\n    selectors\n    productModifications\n    __typename\n  }\n  isAvailable\n  stock\n  nutritionalDetails\n  clickMultiplier\n  subQty\n  subUnit\n  maxQty\n  minQty\n  specialMaxQty\n  ean\n  boost\n  showSubUnit\n  isActive\n  slug\n  categoriesPath\n  categories {\n    id\n    name\n    __typename\n  }\n  formats {\n    format\n    equivalence\n    unitEquivalence\n    minQty\n    maxQty\n    __typename\n  }\n  tags {\n    id\n    tagReference\n    name\n    filter\n    enabled\n    description\n    backgroundColor\n    textColor\n    __typename\n  }\n  __typename\n}\n\nquery GetProducts($pagination: paginationInput, $search: SearchInput, $storeId: ID!, $categoryId: ID, $onlyThisCategory: Boolean, $filter: ProductsFilterInput, $orderBy: productsSortInput, $variants: Boolean, $showProductsWithoutStock: Boolean) {\n  getProducts(\n    pagination: $pagination\n    search: $search\n    storeId: $storeId\n    categoryId: $categoryId\n    onlyThisCategory: $onlyThisCategory\n    filter: $filter\n    orderBy: $orderBy\n    variants: $variants\n    showProductsWithoutStock: $showProductsWithoutStock\n  ) {\n    redirectTo\n    products {\n      ...BaseProductV2\n      __typename\n    }\n    paginator {\n      pages\n      page\n      __typename\n    }\n    __typename\n  }\n}"
                            }
                            ])
        api_headers = {
                        'authority': 'nextgentheadless.instaleap.io',
                        'accept': '*/*',
                        'accept-language': 'en-US,en;q=0.9',
                        'apollographql-client-name': 'Ecommerce',
                        'apollographql-client-version': '3.26.11',
                        'content-type': 'application/json',
                        'origin': 'https://despensa.bodegaaurrera.com.mx',
                        'referer': banner_silder_url,
                        'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"Windows"',
                        'sec-fetch-dest': 'empty',
                        'sec-fetch-mode': 'cors',
                        'sec-fetch-site': 'cross-site',
                        'token': '',
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
                        }
        main_silder_response = await self.requests_process_post(main_silder_api_url,payload,api_headers)
        
        item['format'] = 'Banner'
        item['page_url'] = media_search_url
        item['image_url'] = image_url
        item["screenshot"] = filename
        item['redirection_url'] = banner_silder_url
        item['master_products'] = []
        for product in main_silder_response.json()[0]['data']['getProducts']['products']:
            product_dict = {}
            product_dict['product_id'] = product.get('sku','')
            product_dict['name'] =  re.sub(r'\s+',' ',str(product.get('name','')))
            product_dict['price'] = f"${'%.2f' % float(product.get('price',''))}"
            if product.get('specialPrice',''):
                    product_dict['price'] = f"${'%.2f' % float(product.get('specialPrice',''))}"
            product_dict['url'] = f"https://despensa.bodegaaurrera.com.mx/p/{product.get('slug','')}"
            product_dict['image_url'] = product.get('photosUrls',[])
            product_dict['brand'] = product.get('brand',None)
            product_dict['gtin'] = product.get('ean')[0]
            item['master_products'].append(product_dict.copy())
        return item
    
    async def sponsored_product_collection(self):
        sponsored_category_url = 'https://nextgentheadless.instaleap.io/api/v2'
        payload = json.dumps([
                            {
                                "operationName": "GetStore",
                                "variables": {
                                "storeId": "565",
                                "clientId": "BODEGA_AURRERA"
                                },
                                "query": "query GetStore($storeId: ID, $clientId: String) {\n  getStore(storeId: $storeId, clientId: $clientId) {\n    dynamicParams\n    id\n    name\n    code\n    categories {\n      id\n      image\n      slug\n      name\n      redirectTo\n      isAvailableInHome\n      __typename\n    }\n    banners {\n      id\n      title\n      desktopImage\n      mobileImage\n      targetCategory\n      targetUrl {\n        url\n        type\n        __typename\n      }\n      __typename\n    }\n    state\n    cities {\n      name\n      __typename\n    }\n    __typename\n  }\n}"
                            }
                            ])
        api_headers = {
                            'authority': 'nextgentheadless.instaleap.io',
                            'accept': '*/*',
                            'accept-language': 'en-US,en;q=0.9',
                            'apollographql-client-name': 'Ecommerce',
                            'apollographql-client-version': '3.26.11',
                            'content-type': 'application/json',
                            'origin': 'https://despensa.bodegaaurrera.com.mx',
                            'referer': 'https://despensa.bodegaaurrera.com.mx/',
                            'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
                            'sec-ch-ua-mobile': '?0',
                            'sec-ch-ua-platform': '"Windows"',
                            'sec-fetch-dest': 'empty',
                            'sec-fetch-mode': 'cors',
                            'sec-fetch-site': 'cross-site',
                            'token': '',
                            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
                         }
        sponsored_category_response = await self.requests_process_post(sponsored_category_url,payload,api_headers) 
         
        sponsored_product_dict = {}
        sponsored_product_dict['products'] = []
        for store_category in sponsored_category_response.json()[0]['data']['getStore']['categories']:
            category_id =  store_category.get('id')
            category_payload = json.dumps([
                                {
                                    "operationName": "GetProducts",
                                    "variables": {
                                    "pagination": {
                                        "pageSize": 6,
                                        "currentPage": 1
                                    },
                                    "storeId": "565",
                                    "categoryId": category_id,
                                    "variants": False,
                                    "showProductsWithoutStock": True
                                    },
                                    "query": "fragment BaseProductV2 on Product {\n  id\n  description\n  name\n  brand\n  photosUrls\n  sku\n  unit\n  price\n  specialPrice\n  promotion {\n    description\n    type\n    isActive\n    conditions\n    __typename\n  }\n  variants {\n    selectors\n    productModifications\n    __typename\n  }\n  isAvailable\n  stock\n  nutritionalDetails\n  clickMultiplier\n  subQty\n  subUnit\n  maxQty\n  minQty\n  specialMaxQty\n  ean\n  boost\n  showSubUnit\n  isActive\n  slug\n  categoriesPath\n  categories {\n    id\n    name\n    __typename\n  }\n  formats {\n    format\n    equivalence\n    unitEquivalence\n    minQty\n    maxQty\n    __typename\n  }\n  tags {\n    id\n    tagReference\n    name\n    filter\n    enabled\n    description\n    backgroundColor\n    textColor\n    __typename\n  }\n  __typename\n}\n\nquery GetProducts($pagination: paginationInput, $search: SearchInput, $storeId: ID!, $categoryId: ID, $onlyThisCategory: Boolean, $filter: ProductsFilterInput, $orderBy: productsSortInput, $variants: Boolean, $showProductsWithoutStock: Boolean) {\n  getProducts(\n    pagination: $pagination\n    search: $search\n    storeId: $storeId\n    categoryId: $categoryId\n    onlyThisCategory: $onlyThisCategory\n    filter: $filter\n    orderBy: $orderBy\n    variants: $variants\n    showProductsWithoutStock: $showProductsWithoutStock\n  ) {\n    redirectTo\n    products {\n      ...BaseProductV2\n      __typename\n    }\n    paginator {\n      pages\n      page\n      __typename\n    }\n    __typename\n  }\n}"
                                }
                                ])
            category_response = await self.requests_process_post(sponsored_category_url,category_payload,api_headers)
            for sponsored_product in category_response.json()[0]['data']['getProducts']['products']:
                product_dict = {}
                product_dict['product_id'] = sponsored_product.get('sku','')
                product_dict['name'] =  re.sub(r'\s+',' ',str(sponsored_product.get('name','')))
                brand = sponsored_product.get('brand',None)
                product_dict['brand'] = brand if brand else None
                product_dict['price'] = f"${'%.2f' % float(sponsored_product.get('price',''))}"
                product_dict['url'] = f"https://despensa.bodegaaurrera.com.mx/p/{sponsored_product.get('slug','')}"
                product_dict['image_url'] = sponsored_product.get('photosUrls',[])
                product_dict['gtin'] = sponsored_product.get('ean','')[0]
                if sponsored_product.get('specialPrice',''):
                    product_dict['price'] = f"${'%.2f' % float(sponsored_product.get('specialPrice',''))}"
                sponsored_product_dict['products'].append(product_dict.copy())
        return sponsored_product_dict 
    async def main_silder(self,item,main_silder_url,image_url,filename):
        main_silder_api_url = "https://nextgentheadless.instaleap.io/api/v2"
        main_silder_name = None
        if re.search(r'name\=([^>]*?)\&',main_silder_url):
            main_silder_name = re.findall(r'name\=([^>]*?)\&',main_silder_url)[0]
        elif re.search(r'name\%3D([^>]*?)\%26',main_silder_url):
            main_silder_name = re.findall(r'name\%3D([^>]*?)\%26',main_silder_url)[0]
        else:
            breakpoint()
        print("main_silder_name",main_silder_name)
        payload = json.dumps([
                            {
                                "operationName": "GetProducts",
                                "variables": {
                                "filter": {},
                                "pagination": {
                                    "pageSize": 100,
                                    "currentPage": 1
                                },
                                "orderBy": {},
                                "search": {
                                    "text": main_silder_name,
                                    "language": "ES"
                                },
                                "showProductsWithoutStock": True,
                                "storeId": "565"
                                },
                                "query": "fragment BaseProductV2 on Product {\n  id\n  description\n  name\n  brand\n  photosUrls\n  sku\n  unit\n  price\n  specialPrice\n  promotion {\n    description\n    type\n    isActive\n    conditions\n    __typename\n  }\n  variants {\n    selectors\n    productModifications\n    __typename\n  }\n  isAvailable\n  stock\n  nutritionalDetails\n  clickMultiplier\n  subQty\n  subUnit\n  maxQty\n  minQty\n  specialMaxQty\n  ean\n  boost\n  showSubUnit\n  isActive\n  slug\n  categoriesPath\n  categories {\n    id\n    name\n    __typename\n  }\n  formats {\n    format\n    equivalence\n    unitEquivalence\n    minQty\n    maxQty\n    __typename\n  }\n  tags {\n    id\n    tagReference\n    name\n    filter\n    enabled\n    description\n    backgroundColor\n    textColor\n    __typename\n  }\n  __typename\n}\n\nquery GetProducts($pagination: paginationInput, $search: SearchInput, $storeId: ID!, $categoryId: ID, $onlyThisCategory: Boolean, $filter: ProductsFilterInput, $orderBy: productsSortInput, $variants: Boolean, $showProductsWithoutStock: Boolean) {\n  getProducts(\n    pagination: $pagination\n    search: $search\n    storeId: $storeId\n    categoryId: $categoryId\n    onlyThisCategory: $onlyThisCategory\n    filter: $filter\n    orderBy: $orderBy\n    variants: $variants\n    showProductsWithoutStock: $showProductsWithoutStock\n  ) {\n    redirectTo\n    products {\n      ...BaseProductV2\n      __typename\n    }\n    paginator {\n      pages\n      page\n      __typename\n    }\n    __typename\n  }\n}"
                            }
                            ])
        api_headers = {
                        'authority': 'nextgentheadless.instaleap.io',
                        'accept': '*/*',
                        'accept-language': 'en-US,en;q=0.9',
                        'apollographql-client-name': 'Ecommerce',
                        'apollographql-client-version': '3.26.11',
                        'content-type': 'application/json',
                        'origin': 'https://despensa.bodegaaurrera.com.mx',
                        'referer': main_silder_url,
                        'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"Windows"',
                        'sec-fetch-dest': 'empty',
                        'sec-fetch-mode': 'cors',
                        'sec-fetch-site': 'cross-site',
                        'token': '',
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
                        }
        main_silder_response = await self.requests_process_post(main_silder_api_url,payload,api_headers)
        
        item['format'] = 'Main Slider'
        item['page_url'] = 'https://despensa.bodegaaurrera.com.mx/'
        item['image_url'] = image_url
        item["screenshot"] = filename
        item['redirection_url'] = main_silder_url
        item['master_products'] = []
        try:
            for product in main_silder_response.json()[0]['data']['getProducts']['products']:
                product_dict = {}
                product_dict['product_id'] = product.get('sku','')
                product_dict['name'] = re.sub(r'\s+',' ',str(product.get('name','')))
                product_dict['price'] = f"${'%.2f' % float(product.get('price',''))}"
                product_dict['url'] = f"https://despensa.bodegaaurrera.com.mx/p/{product.get('slug','')}"
                product_dict['image_url'] = product.get('photosUrls',[])
                product_dict['brand'] = product.get('brand',None)
                product_dict['gtin'] = product.get('ean')[0]
                if product.get('specialPrice',''):
                    product_dict['price'] = f"${'%.2f' % float(product.get('specialPrice',''))}"
                item['master_products'].append(product_dict.copy())
            return item
        except:
            return item
    
    async def banner_link(self,response):
        if re.search(r"index\.html$",str(response.request.url),):
            self.banner_api_response.append(response.url)
        if re.search(r"https\:\/\/securepubads\.g\.doubleclick\.net\/gampad\/ads\?",str(response.request.url),):
            self.banner_api_response.append(response.url)
    
    async def api_store_id(self,response):
        if re.search(r"index\.html$",str(response.request.url),):
            self.api_response.append(response.url)
        if re.search(r"https\:\/\/securepubads\.g\.doubleclick\.net\/gampad\/ads\?",str(response.request.url),):
            self.api_response.append(response.url)
            # self.api_response_text.append(await response.text())
    
    
    async def parse(self, response):
        # for _ in range(5):
        #     self.page.locator("//button[contains(@class,'Arrows__RightArrow')]").click()
        #     self.page.wait_for_timeout(5000)
        
        for index_url in self.api_response:
            index_response_body = await self.requests_process(index_url)
            image_url,main_silder_url = None,None
            if re.search(r"https\:\/\/securepubads\.g\.doubleclick\.net\/gampad\/ads\?",index_url):
                if re.search(r'<img\s*id\=\"myImage\"\s*onclick\=\"openURL\(\)\"\s*src\=\"([^>]*?)"',str(index_response_body.text)):
                    image_url = re.findall(r'<img\s*id\=\"myImage\"\s*onclick\=\"openURL\(\)\"\s*src\=\"([^>]*?)"',str(index_response_body.text))[0]
                if re.search(r'window\.open\(\"([^>]*?)\"',str(index_response_body.text)):
                    main_silder_url = re.findall(r'window\.open\(\"([^>]*?)\"',str(index_response_body.text))[0]
            else:
                index_response = Selector(text=index_response_body.text)
                image_url = index_response.xpath('//meta[@name="banner.thumbnail"]/@content').get('')
                if re.search('\"global_hyperlinkValue\"\:\"([^>]*?)\"',index_response_body.text):
                    main_silder_url = re.findall('\"global_hyperlinkValue\"\:\"([^>]*?)\"',index_response_body.text)[0]
            if image_url and main_silder_url:
                # print("main_silder_url",main_silder_url)
                # print("image_url",image_url)
                hash_obj = hashlib.md5(image_url.encode("utf-8"))
                filename = hash_obj.hexdigest()+".png"
                image_response = await self.requests_process(image_url)
                with open(f"{self.screenshot_folder}/{filename}",'wb') as f:f.write(image_response.body)
                item = {}
                item = await self.main_silder(item,main_silder_url,image_url,filename)
                yield item

        for page_down_count in range(20):
            self.page.keyboard.press('PageDown')
            self.page.wait_for_timeout(2000)
        page_xpath = Selector(text=self.page.content())
        product_sku_dict = {}
        for count,product_locator in enumerate(page_xpath.xpath('//div[contains(@class,"card-product-vertical product-card")]'),1):
            product_image = product_locator.xpath('.//img/@src').get()
            print("product_image => ",product_image)
            product_image_sku = product_image.split('/')[-1].replace('L.jpg','')
            hash_obj = hashlib.md5(str(product_image).encode("utf-8"))
            filename = hash_obj.hexdigest()+".png"
            try:
                self.page.locator(f'(//div[contains(@class,"card-product-vertical product-card")])[{count}]').screenshot(path=f"{self.screenshot_folder}/{filename}")
                time.sleep(1)
                self.page.locator(f'(//div[contains(@class,"card-product-vertical product-card")])[{count}]').screenshot(path=f"{self.screenshot_folder}/{filename}")
            except:
                logging.warning("ERROR AT LINE 95")
            product_sku_dict[product_image_sku] = [filename,count]
        product_sku_lists = list(product_sku_dict.keys())
        sponsored_products = await self.sponsored_product_collection()
        for product_sku_list in product_sku_lists:
            for sponsored_product in sponsored_products['products']:
                if product_sku_list in sponsored_product.get('product_id'):
                    screenshot,position = product_sku_dict[product_sku_list]
                    sponsored_product_data = {}
                    sponsored_product_data['format'] = 'sponsored_product'
                    sponsored_product_data['position'] = position
                    sponsored_product_data['banner_id'] = product_sku_list
                    sponsored_product_data['page_url'] = 'https://despensa.bodegaaurrera.com.mx/'
                    sponsored_product_data['product'] = []
                    data = {}
                    data['product_id'] = product_sku_list
                    data['name'] = sponsored_product.get('name')
                    data['brand'] = sponsored_product.get('brand')
                    data['prices'] = (sponsored_product.get('price'))
                    data['url'] = sponsored_product.get('url')
                    data['image_url'] = sponsored_product.get('image_url')
                    data['gtin'] = sponsored_product.get('gtin')
                    sponsored_product_data['product'].append(data)
                    sponsored_product_data['screenshot'] = screenshot
                    yield sponsored_product_data
        
        """ read loccitane search url text file"""
        dir_path=os.path.abspath(__file__ + "/../../../")
        supporting_files=os.path.join(dir_path,"supporting_files")
        with open(f'{supporting_files}/bodega_mx_media_input.txt') as f:
            media_search_urls=f.readlines()
        for media_search_url in media_search_urls:
            item = {} 
            item = await self.banner_collection(media_search_url.strip(),item)
            self.banner_api_response = []
            yield item
        
    

    async def requests_process(self,url):
        request = scrapy.Request(url)
        response = await self.crawler.engine.download(request, self)
        return response
    async def requests_process_post(self,url,payload,header):
        request = scrapy.Request(url,method='POST',headers=header,body=payload)
        response = await self.crawler.engine.download(request, self)
        return response