import re
import os
import json
import scrapy
from pathlib import Path
from scrapy import Request
from dotenv import load_dotenv
import datetime as currentdatetime
from inline_requests import inline_requests


""" load env file """
try:
    load_dotenv()
except:
    env_path = Path(".env")
    load_dotenv(dotenv_path=env_path)

class SephoraUkOffersSpider(scrapy.Spider):
    name = 'bodega_mx_offers'
    headers = {
        'accept-encoding': 'gzip, deflate, br',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
            }
    CURRENT_DATETIME = currentdatetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    ROTATING_PROXY = os.getenv("ROTATING_PROXY")
    custom_settings={
        'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/bodega_mx/%(name)s_{DATE}.json": {"format": "json",}},
        "CONCURRENT_REQUESTS":10,
        'ROTATING_PROXY_LIST' : [ROTATING_PROXY],
        # "HTTPCACHE_ENABLED" : True,
        # "HTTPCACHE_DIR" : 'httpcache',
        # 'HTTPCACHE_EXPIRATION_SECS':86400,
        # "HTTPCACHE_IGNORE_HTTP_CODES":[502,504],
        # "HTTPCACHE_STORAGE" : "scrapy.extensions.httpcache.FilesystemCacheStorage",
        }
    
    def start_requests (self):
        self.spider_name=self.name
        dir_path=os.path.abspath(__file__ + "/../../../")
        supporting_files=os.path.join(dir_path,"supporting_files")  
        with open(f'{supporting_files}/bodega_mx_offer_input.txt') as f:offer_urls = f.readlines()
        for offer_url in offer_urls:
            offer_url = offer_url.strip()
            url = "https://nextgentheadless.instaleap.io/api/v3"
            category_id = offer_url.split('/')[-1]
            payload = json.dumps([
                        {
                            "operationName": "GetProductsByCategory",
                            "variables": {
                            "getProductsByCategoryInput": {
                                "categoryReference": category_id,
                                "categoryId": "null",
                                "clientId": "BODEGA_AURRERA",
                                "storeReference": "9999",
                                "currentPage": 1,
                                "pageSize": 100,
                                "filters": {}
                            }
                            },
                            "query": "fragment CategoryFields on CategoryModel {\n  active\n  boost\n  hasChildren\n  categoryNamesPath\n  isAvailableInHome\n  level\n  name\n  path\n  reference\n  slug\n  photoUrl\n  imageUrl\n  shortName\n  isFeatured\n  isAssociatedToCatalog\n  __typename\n}\n\nfragment CatalogProductTagModel on CatalogProductTagModel {\n  description\n  enabled\n  textColor\n  filter\n  tagReference\n  backgroundColor\n  name\n  __typename\n}\n\nfragment CatalogProductFormatModel on CatalogProductFormatModel {\n  format\n  equivalence\n  unitEquivalence\n  __typename\n}\n\nfragment PromotionCondition on PromotionCondition {\n  quantity\n  price\n  __typename\n}\n\nfragment Promotion on Promotion {\n  type\n  isActive\n  conditions {\n    ...PromotionCondition\n    __typename\n  }\n  description\n  endDateTime\n  startDateTime\n  __typename\n}\n\nfragment CatalogProductModel on CatalogProductModel {\n  name\n  price\n  photosUrl\n  unit\n  subUnit\n  subQty\n  description\n  sku\n  ean\n  maxQty\n  minQty\n  clickMultiplier\n  nutritionalDetails\n  isActive\n  slug\n  brand\n  stock\n  securityStock\n  boost\n  isAvailable\n  location\n  promotion {\n    ...Promotion\n    __typename\n  }\n  categories {\n    ...CategoryFields\n    __typename\n  }\n  categoriesData {\n    ...CategoryFields\n    __typename\n  }\n  formats {\n    ...CatalogProductFormatModel\n    __typename\n  }\n  tags {\n    ...CatalogProductTagModel\n    __typename\n  }\n  score\n  relatedProducts\n  ingredients\n  stockWarning\n  __typename\n}\n\nfragment CategoryWithProductsModel on CategoryWithProductsModel {\n  name\n  reference\n  level\n  path\n  hasChildren\n  active\n  boost\n  isAvailableInHome\n  slug\n  photoUrl\n  categoryNamesPath\n  imageUrl\n  shortName\n  isFeatured\n  products {\n    ...CatalogProductModel\n    __typename\n  }\n  __typename\n}\n\nfragment PaginationTotalModel on PaginationTotalModel {\n  value\n  relation\n  __typename\n}\n\nfragment PaginationModel on PaginationModel {\n  page\n  pages\n  total {\n    ...PaginationTotalModel\n    __typename\n  }\n  __typename\n}\n\nfragment AggregateBucketModel on AggregateBucketModel {\n  min\n  max\n  key\n  docCount\n  __typename\n}\n\nfragment AggregateModel on AggregateModel {\n  name\n  docCount\n  buckets {\n    ...AggregateBucketModel\n    __typename\n  }\n  __typename\n}\n\nfragment BannerModel on BannerModel {\n  id\n  storeId\n  title\n  desktopImage\n  mobileImage\n  targetUrl\n  targetUrlInfo {\n    type\n    url\n    __typename\n  }\n  targetCategory\n  index\n  categoryId\n  __typename\n}\n\nquery GetProductsByCategory($getProductsByCategoryInput: GetProductsByCategoryInput!) {\n  getProductsByCategory(getProductsByCategoryInput: $getProductsByCategoryInput) {\n    category {\n      ...CategoryWithProductsModel\n      __typename\n    }\n    pagination {\n      ...PaginationModel\n      __typename\n    }\n    aggregates {\n      ...AggregateModel\n      __typename\n    }\n    banners {\n      ...BannerModel\n      __typename\n    }\n    __typename\n  }\n}"
                        }
                        ])
            headers = {
                    'authority': 'nextgentheadless.instaleap.io',
                    'accept': '*/*',
                    # 'accept-encoding': 'gzip, deflate, br',
                    'accept-language': 'en-US,en;q=0.9',
                    'apollographql-client-name': 'Ecommerce',
                    'apollographql-client-version': '3.26.4',
                    'content-type': 'application/json',
                    'origin': 'https://despensa.bodegaaurrera.com.mx',
                    'referer': offer_url,
                    'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'cross-site',
                    'token': '',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
                        }
            
            yield scrapy.Request(url=url,callback=self.parse,method='POST',body=payload,headers=headers,cb_kwargs={'offer_url':offer_url,'category_id':category_id,'total_pages':1,'page':2,"item":{},"rank":0})
    
    # @inline_requests    
    def parse(self,response,offer_url,category_id,total_pages,page,item={},rank=0):    
        if not bool(item):
            
            title = response.json()[0]['data']['getProductsByCategory']['category']['name']
            item['title'] = title
            item['page_url'] = offer_url
            item['count'] =  response.json()[0]['data']['getProductsByCategory']['pagination']['total']['value']
            
            item['category_crumb']=[]
            item['category_crumb'].append({'name':'Homepage','url':"https://despensa.bodegaaurrera.com.mx/"})
            item['category_crumb'].append({'name':title,'url':f"https://despensa.bodegaaurrera.com.mx/ca/{response.json()[0]['data']['getProductsByCategory']['category']['slug']}"})
            item['products']=[]
            
        for product in response.json()[0]['data']['getProductsByCategory']['category']['products']:
            
            product_dict = {}
            rank=rank+1
            product_dict['rank'] = rank
            product_dict['url'] = f"https://despensa.bodegaaurrera.com.mx/p/{product.get('slug','')}"
            product_dict['image_url'] = product.get('photosUrl',[])
            product_dict['has_video'] = False
            product_dict["video"] = []
            
            product_dict['gtin'] = product.get('ean')[0]
            product_dict['master_product_id'] = product.get('sku','')
            product_dict['name'] = re.sub(r'\s+',' ',str(product.get('name','')))
            product_dict['brand'] = product.get('brand','')
            product_dict['price'] = f"${'%.2f' % float(product.get('price',''))}"
            product_dict['in-stock'] = product.get('isAvailable','')
            product_dict['price_before'] = None
            product_dict['promo_label'] = None
            if product.get('promotion',{}):
                if 'specialPrice' in product.get('promotion',{}).get('type',''):
                    product_dict['price_before'] = f"${'%.2f' % float(product.get('price',''))}"
                    product_dict['price'] = f"${'%.2f' % float(product.get('promotion',{}).get('conditions')[0].get('price'))}"
                    product_dict['promo_label'] = f"Ahorro ${product.get('price','')-product.get('promotion',{}).get('conditions')[0].get('price')}"
            item['products'].append(product_dict.copy())
        
        
        total_pages = response.json()[0]['data']['getProductsByCategory']['pagination']['pages']
        api_url = "https://nextgentheadless.instaleap.io/api/v3"
        total_pages = total_pages if total_pages else 0
        if page <= total_pages:
            payload = json.dumps([
                                {
                                    "operationName": "GetProductsByCategory",
                                    "variables": {
                                    "getProductsByCategoryInput": {
                                        "categoryReference": category_id,
                                        "categoryId": "null",
                                        "clientId": "BODEGA_AURRERA",
                                        "storeReference": "9999",
                                        "currentPage": page,
                                        "pageSize": 100,
                                        "filters": {}
                                    }
                                    },
                                    "query": "fragment CategoryFields on CategoryModel {\n  active\n  boost\n  hasChildren\n  categoryNamesPath\n  isAvailableInHome\n  level\n  name\n  path\n  reference\n  slug\n  photoUrl\n  imageUrl\n  shortName\n  isFeatured\n  isAssociatedToCatalog\n  __typename\n}\n\nfragment CatalogProductTagModel on CatalogProductTagModel {\n  description\n  enabled\n  textColor\n  filter\n  tagReference\n  backgroundColor\n  name\n  __typename\n}\n\nfragment CatalogProductFormatModel on CatalogProductFormatModel {\n  format\n  equivalence\n  unitEquivalence\n  __typename\n}\n\nfragment PromotionCondition on PromotionCondition {\n  quantity\n  price\n  __typename\n}\n\nfragment Promotion on Promotion {\n  type\n  isActive\n  conditions {\n    ...PromotionCondition\n    __typename\n  }\n  description\n  endDateTime\n  startDateTime\n  __typename\n}\n\nfragment CatalogProductModel on CatalogProductModel {\n  name\n  price\n  photosUrl\n  unit\n  subUnit\n  subQty\n  description\n  sku\n  ean\n  maxQty\n  minQty\n  clickMultiplier\n  nutritionalDetails\n  isActive\n  slug\n  brand\n  stock\n  securityStock\n  boost\n  isAvailable\n  location\n  promotion {\n    ...Promotion\n    __typename\n  }\n  categories {\n    ...CategoryFields\n    __typename\n  }\n  categoriesData {\n    ...CategoryFields\n    __typename\n  }\n  formats {\n    ...CatalogProductFormatModel\n    __typename\n  }\n  tags {\n    ...CatalogProductTagModel\n    __typename\n  }\n  score\n  relatedProducts\n  ingredients\n  stockWarning\n  __typename\n}\n\nfragment CategoryWithProductsModel on CategoryWithProductsModel {\n  name\n  reference\n  level\n  path\n  hasChildren\n  active\n  boost\n  isAvailableInHome\n  slug\n  photoUrl\n  categoryNamesPath\n  imageUrl\n  shortName\n  isFeatured\n  products {\n    ...CatalogProductModel\n    __typename\n  }\n  __typename\n}\n\nfragment PaginationTotalModel on PaginationTotalModel {\n  value\n  relation\n  __typename\n}\n\nfragment PaginationModel on PaginationModel {\n  page\n  pages\n  total {\n    ...PaginationTotalModel\n    __typename\n  }\n  __typename\n}\n\nfragment AggregateBucketModel on AggregateBucketModel {\n  min\n  max\n  key\n  docCount\n  __typename\n}\n\nfragment AggregateModel on AggregateModel {\n  name\n  docCount\n  buckets {\n    ...AggregateBucketModel\n    __typename\n  }\n  __typename\n}\n\nfragment BannerModel on BannerModel {\n  id\n  storeId\n  title\n  desktopImage\n  mobileImage\n  targetUrl\n  targetUrlInfo {\n    type\n    url\n    __typename\n  }\n  targetCategory\n  index\n  categoryId\n  __typename\n}\n\nquery GetProductsByCategory($getProductsByCategoryInput: GetProductsByCategoryInput!) {\n  getProductsByCategory(getProductsByCategoryInput: $getProductsByCategoryInput) {\n    category {\n      ...CategoryWithProductsModel\n      __typename\n    }\n    pagination {\n      ...PaginationModel\n      __typename\n    }\n    aggregates {\n      ...AggregateModel\n      __typename\n    }\n    banners {\n      ...BannerModel\n      __typename\n    }\n    __typename\n  }\n}"
                                }
                                ])
            headers = {
                        'authority': 'nextgentheadless.instaleap.io',
                        'accept': '*/*',
                        'accept-language': 'en-US,en;q=0.9',
                        'apollographql-client-name': 'Ecommerce',
                        'apollographql-client-version': '3.26.11',
                        'content-type': 'application/json',
                        'origin': 'https://despensa.bodegaaurrera.com.mx',
                        'referer': offer_url,
                        'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Microsoft Edge";v="116"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"Windows"',
                        'sec-fetch-dest': 'empty',
                        'sec-fetch-mode': 'cors',
                        'sec-fetch-site': 'cross-site',
                        'token': '',
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.76'
                        }
            page += 1
            yield scrapy.Request(api_url,headers=headers,method='POST',body=payload,callback=self.parse,cb_kwargs={'offer_url':offer_url,'category_id':category_id,'total_pages':total_pages,'page':page,"item":item,"rank":rank})
        else:
            yield item
        
    
        
                
                
     