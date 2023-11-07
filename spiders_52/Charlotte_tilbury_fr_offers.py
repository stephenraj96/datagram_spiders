import os
import re
import json
import scrapy
import urllib.parse
from datetime import datetime
class CharlotteTilburyFROffersSpider(scrapy.Spider):
    name = 'Charlotte_tilbury_fr_offers'

    start_urls = ['https://www.charlottetilbury.com/fr']
    CURRENT_DATETIME = datetime.now() #today's date
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d") #2023-04-25
    DATE=CURRENT_DATE.replace("-","_") #2023_04_25
    ROTATING_PROXY = os.getenv("ROTATING_PROXY_FR")
    custom_settings={
        'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/Charlotte_tilbury_fr/%(name)s_{DATE}.json": {"format": "json",}},
                     'ROTATING_PROXY_LIST' : [ROTATING_PROXY],
                     "CONCURRENT_REQUESTS": 1,
                     "DOWNLOAD_DELAY":2,
                     "DUPEFILTER_DEBUG": True,
                     "DUPEFILTER_CLASS": "scrapy.dupefilters.BaseDupeFilter",
                     }
    def parse(self, response):
        dir_path=os.path.abspath(__file__ + "/../../../")
        supporting_files=os.path.join(dir_path,"supporting_files") #reading input file 'Charlotte_tilbury_uk_input_urls.txt' from the directory path of supporting_files
       
        with open(f'{supporting_files}/Charlotte_tilbury_fr_input_urls.txt','r') as f:
            urls=f.readlines()
        for url in urls:
            yield scrapy.Request(url.strip(),callback=self.parse_category)

    async def parse_category(self,response):
        item={}
        regex_data=re.findall(r'\<script\sid\=\"\_\_NEXT\_DATA\_\_\"\stype\=\"application\/json\"\>([\w\W]+)\<\/script\>',response.text)[0]
        json_data=json.loads(regex_data)
        categories=json_data['props']['breadcrumbs']
        item['title']=None
        item['page_url']=response.url
        item['count']=None
        if '?search=' in response.url:
            
            search_value=str(response.url).split('?search=')[-1]
            item['title']=urllib.parse.unquote_plus(search_value)
            post_url = "https://ztf0lv96g2-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.11.0)%3B%20Browser%20(lite)%3B%20JS%20Helper%20(3.11.3)%3B%20react%20(17.0.2)%3B%20react-instantsearch%20(6.39.0)&x-algolia-api-key=e3c8012d28b6c013906005377ec04f03&x-algolia-application-id=ZTF0LV96G2"
            # post_url="https://ztf0lv96g2-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.11.0)%3B%20Browser%20(lite)%3B%20JS%20Helper%20(3.11.3)%3B%20react%20(17.0.2)%3B%20react-instantsearch%20(6.39.0)&x-algolia-api-key=e3c8012d28b6c013906005377ec04f03&x-algolia-application-id=ZTF0LV96G2"
            # payload='{"requests":[{"indexName":"Products_Store_FR","params":"clickAnalytics=true&facets=%5B%5D&filters=prices.listingPrices.currencyCode%3AGBP&highlightPostTag=%3C%2Fais-highlight-0000000000%3E&highlightPreTag=%3Cais-highlight-0000000000%3E&hitsPerPage=100&page=0&query='+urllib.parse.quote(search_value)+'&ruleContexts=%5B%22web_search%22%5D&tagFilters=&"},{"indexName":"Products_Store_FR_Query_Suggestions","params":"clickAnalytics=true&facets=%5B%5D&highlightPostTag=%3C%2Fais-highlight-0000000000%3E&highlightPreTag=%3Cais-highlight-0000000000%3E&hitsPerPage=100&page=0&query='+urllib.parse.quote(search_value)+'&ruleContexts=%5B%22web_search%22%5D&tagFilters=&"}]}'
            payload = '{"requests":[{"indexName":"Products_Store_FR","params":"clickAnalytics=true&facets=%5B%5D&filters=prices.listingPrices.currencyCode%3AEUR&highlightPostTag=%3C%2Fais-highlight-0000000000%3E&highlightPreTag=%3Cais-highlight-0000000000%3E&hitsPerPage=120&page=0&query='+urllib.parse.quote(search_value)+'&ruleContexts=%5B%22web_search%22%5D&tagFilters=&"},{"indexName":"Products_Store_FR_Query_Suggestions","params":"clickAnalytics=true&facets=%5B%5D&highlightPostTag=%3C%2Fais-highlight-0000000000%3E&highlightPreTag=%3Cais-highlight-0000000000%3E&hitsPerPage=120&page=0&query='+urllib.parse.quote(search_value)+'&ruleContexts=%5B%22web_search%22%5D&tagFilters=&"}]}'
            headers={
                'Referer': 'https://www.charlottetilbury.com/',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
                }
           
            search_request=await self.request_process(post_url,headers=headers,payload=payload)
            search_response=search_request.json()
            data=search_response['results'][0]['hits']
            item['count']=search_response['results'][0]['nbHits']
          
        else:
            item['title']=json_data['props']['title']
            item['count']=len(json_data['props']['products'])
            data=json_data['props']['products']
        item['category_crumb']=[]
        
        for i in categories:
            category={}
            category['name']=i['label']
            if response.url.__contains__('/products/'):
                category['url']=f'https://www.charlottetilbury.com/fr/products/{i["href"]}'
            item['category_crumb'].append(category)
        item['products']=[]
        n = 0
        for count,product in enumerate(data,start=1):
            product_url=f'https://www.charlottetilbury.com/fr/product/{product["href"]}'
            if not (
                product_url.__contains__("build-your-own")
                or product_url.__contains__("/free-")
            ):
                headers={'Referer': 'https://www.charlottetilbury.com/'}
                payload={}
                product_response=await self.request_process(product_url,headers=headers,payload=payload)
                # if product_response.status==200:
                product_regex_data=re.findall(r'\<script\sid\=\"\_\_NEXT\_DATA\_\_\"\stype\=\"application\/json\"\>([\w\W]+)\<\/script\>',product_response.text)[0]
                product_json_data=json.loads(product_regex_data)
                product_content= product_json_data.get("props",'').get("product",'')
                products={}
                if product_content:
                    n += 1
                    products["rank"]= n
                    products["url"]=f'https://www.charlottetilbury.com/fr/product/{product_json_data["props"]["product"]["href"]}'
                    products["image_url"]=[f"https:{i['imageSrc']}" for i in product_json_data["props"]["product"]['images']] 
                    products['has_video']=False
                    products['video']=[]
                    video=product_json_data.get("props",{}).get("product",{}).get('shortVideo',{})
                    if (video==None) or (video==''):
                        video='' 
                    else: 
                        video=video.get('videoSrc','')
                    if video:
                        products['has_video']=True
                        video='https:'+video
                        products['video'].append(video)
                    products["master_product_id"]= product_json_data["props"]["product"]["sku"]
                    
                    products["name"]= product_json_data["props"]["product"]['title']
                    subtitle=product_json_data["props"]["product"].get('subtitle','')
                    if subtitle:
                        products["name"]= f"{product_json_data['props']['product']['title']} - {subtitle}"
                    products["brand"]= "Charlotte Tilbury"
                    products["price"]=None
                    price= product_json_data.get("props",{}).get("product",{}).get("price",{}).get("purchasePrice",{}).get("value","")
                    if price:
                        products["price"]= f'{"%.2f" %price} €'
                    products["in-stock"]= False
                    in_stock=product_json_data['props']['product']['availability']
                    if in_stock=="AVAILABLE":
                        products["in-stock"]= True   # True if the product can be added to the cart, False otherwise
                    products["price_before"]=None
                    price_before=product_json_data.get("props",{}).get("product",{}).get("price",{}).get("listingPrice",{}).get("value","")
                    if price_before:
                        products["price_before"]= f'{"%.2f" %price_before} €' # price before the discount if exist else None
                    products["promo_label"]= None
                    promo_lable=product_json_data["props"]["product"].get("badge",'')
                    if promo_lable:
                        products["promo_label"]= promo_lable# discount label if exist else None
                    if products["price"]==products["price_before"]:
                        products["price_before"]=None
                    if products["price_before"]==None:
                        products["promo_label"]=None
                    item['products'].append(products)
                    
                    
        yield item
    async def request_process(self,url,headers,payload):
        if payload=={}:
            request=scrapy.Request(url)
        else:
            
            request=scrapy.Request(url,method='POST',headers=headers,body=payload)
        response = await self.crawler.engine.download(request, self)
       
        return response
