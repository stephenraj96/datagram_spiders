import scrapy
import os
import json
import datetime
from dotenv import load_dotenv
from pathlib import Path
import re
import html

""" load env file """
try:
    load_dotenv()
except:
    env_path = Path(".env")
    load_dotenv(dotenv_path=env_path)

class NotinoSpider(scrapy.Spider):
    name = 'notino_fr_offers'
    headers = {'accept-encoding': 'gzip',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}
    
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    ROTATING_PROXY = os.getenv("ROTATING_PROXY_FR")
    custom_settings={
                    "CONCURRENT_REQUESTS":10,
                    "HTTPCACHE_ENABLED" : True,
                    "HTTPCACHE_DIR" : 'httpcache',
                    'HTTPCACHE_EXPIRATION_SECS':82800,
                    "HTTPCACHE_STORAGE" : "scrapy.extensions.httpcache.FilesystemCacheStorage",
                    'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/notino_fr/%(name)s_{DATE}.json": {"format": "json",}},
                    'ROTATING_PROXY_LIST' : [ROTATING_PROXY]
                    }

    def start_requests(self):
        self.spider_name=self.name
        NotinoSpider.name='notino_fr_pdp'
        dir_path=os.path.abspath(__file__ + "/../../../")
        supporting_files=os.path.join(dir_path,"supporting_files")
        with open(f'{supporting_files}/notino_urls.txt') as f:urls = f.readlines()
        for url in urls:
            yield scrapy.Request(url.strip(),headers=self.headers,callback=self.parse,cb_kwargs={"item":{},'rank':0,'url':url.strip(),'page_num':2})


    async def parse(self, response,item={},rank=0,url='',page_num=None):
        if not  bool(item):
            item['title']= response.xpath("//h1[@class='styled__StyledPageTitle-sc-1ddrixz-1 SORUw']/text()|//strong[@class='styled__SearchResultsExpressionText-sc-14y14oz-3 kuzOh']/text()").get('').title()
            if not item['title']:
                item['title']=re.findall("https://www.notino.fr/(.*)\/",url)[0].title().replace("-"," ")
            item['page_url']=response.url
            item['count']=0
            if "search" not in response.url:
                item['category_crumb']=[]
                for category in  response.xpath("//div[@class='styled__LinksWrapper-sc-caahyp-5 cZfFAb']/*"):
                    if category.xpath("./text()").get('').strip() !="/":
                        name=category.xpath("./text()").get("").strip()
                        if not category.xpath("./@href").get("").strip():
                            url=response.url
                        else:
                            url=response.urljoin(category.xpath("./@href").get("").strip())
                        item['category_crumb'].append({"name":name,"url":url}.copy())
            item['products']=[]
        product_count=len(response.xpath('//div/div[@data-testid="product-container"]/a'))
        item['count']=item['count']+product_count
        for block in response.xpath('//div/div[@data-testid="product-container"]/a'):
            product_url=response.urljoin(block.xpath("./@href").get(""))
            product_response= await self.request(product_url)
            if product_response.xpath("//script[@type='application/ld+json']/text()").get():
                json_data=json.loads(product_response.xpath("//script[@type='application/ld+json']/text()").get('{}'))
                rank=rank+1
                data={}
                data['rank']=rank
                data["url"] = product_url
                data['name']=product_response.xpath("//span[@class='sc-3sotvb-4 kSRNEJ']/text()").get()
                data['gtin']=json_data.get("gtin13",None)
                data['image_url']=[re.sub("\s+|\[|\]","",image_url.replace("order_2k",'detail_zoom').replace("`","%60")) for image_url in json_data.get("image",[])]
                data['video']= product_response.xpath("//div[@class='sc-1s81hkh-1 cPDPWg']/iframe/@src").getall()
                if data['video']:
                    data['has_video']= True
                else:
                    data['has_video']= False
                data['brand']=html.unescape(json_data.get("brand",{}).get("name",""))
                data['master_product_id']=json_data.get("sku",None)
                data['subtitle']=product_response.xpath("//span[@class='sc-3sotvb-5 bBQCfG']/text()").get('')
                data['price']=' '.join(product_response.xpath("//div[@id='pd-price']//text()").getall())
                data['price_before']=' '.join(product_response.xpath("//span[@class='sc-wpdhab-6 kPjZwv']//text()").getall())
                data['promo_label']=product_response.xpath("//span[@class='sc-wpdhab-5 wDBvY']//text()").get()
                if 'en stock' in product_response.xpath("//span[@class='sc-mu8uqe-4 firsAk']//text()|//span[@class='sc-mu8uqe-4 frURDV']/text()").get('').lower().strip():
                    data['in-stock']=True
                else:
                    data['in-stock']=False
                item['products'].append(data.copy())
        if not response.xpath('//div[contains(@class,"nextPage")]/@hidden'):
            if re.search(r'\"pageUrl\":\"([^>]*?)\"',str(response.text)):        
                next_page_url = re.findall(r'\"pageUrl\":\"([^>]*?)\"',str(response.text))[0]
                next_page_url = next_page_url.replace('{sortOption}','1')
                next_page_url = next_page_url.replace('{pageNumber}',f'{page_num}')
                page_num += 1
                yield scrapy.Request(response.urljoin(next_page_url),headers=self.headers,callback=self.parse,cb_kwargs={"item":item,"rank":rank,'url':url,'page_num':page_num})
        # next_page=response.xpath("//link[@rel='next']/@href").get()
        # if next_page:
        #     yield scrapy.Request(next_page,headers=self.headers, callback=self.parse,cb_kwargs={"item":item,"rank":rank,'url':url})
        else:
            yield item

    async def request(self,url):
        """ scrapy request"""
        request = scrapy.Request(url,headers=self.headers,dont_filter=True)
        response = await self.crawler.engine.download(request,self)
        return response