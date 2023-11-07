import re
import os
import json
import scrapy
import urllib.parse
from parsel import Selector
from datetime import datetime 

class ShiseidofroffersSpider(scrapy.Spider):
    name = 'shiseido_fr_offers'
    start_urls = ['https://www.shiseido.fr/fr/fr/']
    CURRENT_DATETIME = datetime.now() #today's date
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d") #2023-04-25
    DATE=CURRENT_DATE.replace("-","_") #2023_04_25
    ROTATING_PROXY = os.getenv("ROTATING_PROXY_FR")
    custom_settings={
        'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/shiseido_fr/%(name)s_{DATE}.json": {"format": "json",}},
                     'ROTATING_PROXY_LIST' : [ROTATING_PROXY],
                     }

    def start_requests(self):
        dir_path=os.path.abspath(__file__ + "/../../../")
        supporting_files=os.path.join(dir_path,"supporting_files") #reading input file 'shiseido_fr_input_urls.txt' from the directory path of supporting_files
        with open(f'{supporting_files}/shiseido_fr_input_urls.txt','r') as f:
            urls=f.readlines()
        for url in urls:
            yield scrapy.Request(url,callback=self.parse)
    
    async def parse(self,response):
        count=response.xpath('//div[@class="page-count"]//text()').getall()
        count=' '.join(count)
        Total=re.findall(r'\d+',str(count))
        item={}
        if "/Search-Show?q-hint=&q=" in response.url:
            title=urllib.parse.unquote(response.url.split('=')[-1])
            item['title']=title.replace('+',' ')
            item['page_url']=response.url
            item['count']=0
            item['category_crumb']=[]
        else:
            title=response.url.split('/')
            item['title']=urllib.parse.unquote(title[-1].upper())
            if item['title']=='':
                item['title']=urllib.parse.unquote(title[-2].upper())
            item['page_url']=response.url
            item['count']=0
            item['category_crumb']=[]
            for categories in response.xpath('(//div[@class="breadcrumbs-wrapper"])[1]//div[@itemprop="itemListElement"]'):
                category={}
                if categories.xpath('./a/span//text()'):
                    category['name']=str(categories.xpath('./a/span//text()').get('').strip()).upper()
                    category['url']=categories.xpath('./a/@href').get('').strip()
                    item['category_crumb'].append(category)
                else:
                    category['name']=item['title']
                    category['url']=response.url
                    item['category_crumb'].append(category)
                    
        item['products']=[]
        
        if Total:
            item['count']=Total[0]
            headers={}
            
            url=response.url+f"?start=1&sz={Total[0]}&format=page-element"
            products_request=await self.request_process(url,headers=headers,payload={})
            products_response=Selector(products_request.text)
            if (products_response.xpath('//div[@class="product-image"]')==[]) and (int(Total[0])>=1):
                products_response=response
            item['count'] = len(products_response.xpath('//div[@class="product-image"]'))
            for count,product in enumerate(products_response.xpath('//div[@class="product-image"]'),start=1):
                products={}
                product_link=product.xpath('./a[@class="thumb-link"]/@href').get('').strip()
                product_link=response.urljoin(product_link)
                product_request=await self.request_process(product_link,headers=headers,payload={})
              
                product_response=Selector(product_request.text)
               
                json_data=re.findall(r'\<script\slanguage\=\"javascript\"\stype\=\"text\/\S+\"\sorig\_index\=\"0\"\>\s+var\sproductCache\s\=\s([\w\W]+?)\;\s+var\spdpdata\s\=\sproductCache\;',product_request.text)
                if json_data:
                    json_value=json.loads(json_data[0])
                    products['rank']=count
                    products['url']=product_link
                    products['image_url']=[str(image['url']).replace(' ','%20') for image in json_value['images']['hiRes']]
                    products['has_video']=False
                    products['video']=[]
                    products['master_product_id']=json_value['masterID']
                    products['gtin']=json_value['ID']
                    products['name']=json_value['name']
                    products['brand']=json_value['customBrand']
                    # products['price']=json_value['pricing']['formattedSale']
                    try:
                        products['price']=json_value.get('pricing',{}).get('formattedSale','')
                    except:
                        products['price'] = None
                    products['in-stock']=json_value['availability']['inStock']
                    try: 
                        products['price_before']=json_value['pricing']['formattedStandard']
                    except:
                        products['price_before'] = None
                    if products['price_before']==products['price']:
                        products['price_before']=None
                      
                    promo_label=product_response.xpath('//div[@class="discount-percentage clearfix desktop-only"]//text()').getall()
                    if products['price']=="N/A":
                        products['price']=None
                    promo_label=''.join(promo_label).strip()
                    products['promo_label']=promo_label
                    if products['promo_label']=='':
                        products['promo_label']=None
                    item['products'].append(products)
        yield item
    async def request_process(self,url,headers,payload):
        if payload=={}:
            request=scrapy.Request(url)
        else:
            request=scrapy.Request(url,method='POST',headers=headers,body=payload)
        response = await self.crawler.engine.download(request, self)
        return response
