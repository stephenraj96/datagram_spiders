import scrapy
import os
import json
import re
import datetime
from dotenv import load_dotenv
from pathlib import Path

""" load env file """
try:
    load_dotenv()
except:
    env_path = Path(".env")
    load_dotenv(dotenv_path=env_path)


class GuerlainSpider(scrapy.Spider):
    name = 'guerlain_fr_offers'
    headers = {
  'accept-encoding': 'gzip, deflate, br',
  'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
}
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    ROTATING_PROXY = os.getenv("ROTATING_PROXY_FR")
    custom_settings={
                    # "HTTPCACHE_ENABLED" : True,
                    # "HTTPCACHE_DIR" : 'httpcache',
                    # 'HTTPCACHE_EXPIRATION_SECS':86400,
                    # "HTTPCACHE_STORAGE" : "scrapy.extensions.httpcache.FilesystemCacheStorage",
                    'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/guerlain_fr/%(name)s_{DATE}.json": {"format": "json",}},
                    'ROTATING_PROXY_LIST' : [ROTATING_PROXY]
                    }

    def start_requests(self):
        self.spider_name=self.name
        category_spider_name=(GuerlainSpider.name).replace("offers","categories")
        dir_path= os.getcwd()+rf"/exports/{category_spider_name}"
        with open(os.path.join(dir_path,f"{category_spider_name}_{self.CURRENT_DATE}.json"), 'r',encoding="utf-8") as f:
            contents = json.loads(f.read())
        GuerlainSpider.name="guerlain_fr_pdp"
        for makeup in contents:
            first_name= makeup.get("name","")
            first_url=makeup.get("url",'')
            yield scrapy.Request(first_url,headers=self.headers, callback=self.parse,cb_kwargs={"category":[],"item":{},'count':1,"url":first_url},dont_filter=True)
            for category in makeup.get("category_crumb",[]):
                second_name= category.get("name","")
                second_url=category.get("url","")
                two_category=[{"name":first_name,"url":first_url}.copy(),{"name":second_name,"url":second_url}.copy()]
                yield scrapy.Request(second_url,headers=self.headers, callback=self.parse,cb_kwargs={"category":two_category,"item":{},'count':1,"url":second_url},dont_filter=True)
                for sub_category in category.get("category_crumb",[]):
                    third_name= sub_category.get("name","")
                    third_url=sub_category.get("url","")
                    category=[{"name":first_name,"url":first_url}.copy(),{"name":second_name,"url":second_url}.copy(),{"name":third_name,"url":third_url}.copy()]
                    if not  any(chr.isdigit() for chr in third_url):
                        yield scrapy.Request(third_url,headers=self.headers, callback=self.parse,cb_kwargs={"category":category,"item":{},'count':1,"url":third_url},dont_filter=True)
        dir_path=os.path.abspath(__file__ + "/../../../")
        supporting_files=os.path.join(dir_path,"supporting_files")
        with open(f'{supporting_files}/guerlain_fr_offers_urls.txt') as f:urls = f.readlines()
        for url in urls:
            if "search?q=" in url:
                title=url.strip().split("=")[1]
                keyword=url.strip().split("=")[1].replace("+","%20")
                page_url=f'https://www.guerlain.com/on/demandware.store/Sites-Guerlain_FR-Site/fr_FR/Search-UpdateGrid?cgid=root-category-fr&q={keyword}&prefn1=CORE_online_country&prefv1=FR&prefn2=CORE_product_type&prefv2=SALE&start=0&sz=1000'
                yield scrapy.Request(page_url.strip(),headers=self.headers,callback=self.parse,cb_kwargs={"item":{},'count':1,'category':[],"url":url.strip(),'title':title},dont_filter=True)
            else:
                yield scrapy.Request(url.strip(),headers=self.headers,callback=self.parse,cb_kwargs={"item":{},'count':1,'category':[],"url":url.strip()},dont_filter=True)

        
    async def parse(self, response,item={},count=1,category=[],url=None,title=None):
        async def product(product_url,item,rank):
            request = scrapy.Request(product_url,headers=self.headers,dont_filter=True)
            product_response = await self.crawler.engine.download(request, self)
            data = {}
            data['rank']=rank
            data["url"] = product_url
            data['image_url']=[]
            images= product_response.xpath("//div[contains(@class,'in-zoom')]/@zoom-src").getall()
            if not images:
                shade= product_response.xpath("//*[@data-type-product='SHADE']/@data-pid").get('').strip()
                case=product_response.xpath("//*[@data-type-product='CASE']/@data-pid").get('').strip()
                if 'false' in case: 
                    url=f"https://www.guerlain.com/on/demandware.store/Sites-Guerlain_FR-Site/fr_FR/Product-VariationRougeG?pShade={shade}"
                elif 'false' in shade:
                    url=f"https://www.guerlain.com/on/demandware.store/Sites-Guerlain_FR-Site/fr_FR/Product-VariationRougeG?pCase={case}&engravement=yes"
                else:
                    url=f"https://www.guerlain.com/on/demandware.store/Sites-Guerlain_FR-Site/fr_FR/Product-VariationRougeG?pShade={shade}&pCase={case}&engravement=yes"
                request=scrapy.Request(url,dont_filter=True)
                next_response = await self.crawler.engine.download(request, self)
                json_data=json.loads(next_response.text)
                for image in json_data.get("images",{}).get("zoom",[]):
                    images.append(image.get("url",""))
            if images:
                for image in images:
                    data['image_url'].append(image.strip().replace(" ","%20"))
            data['has_video']= False
            data['video']= []
            data['master_product_id']=product_response.xpath("//article[@class='product']/@data-pid|//meta[@itemprop='productID']/@content").get("")
            data['name']=product_response.xpath("//p[@class='product-desc']/text()|//h1[@class='product-name text-center']/span/text()").get("")
            data["brand"]= "Guerlain"
            data["in-stock"]=stock
            data["price"]= product_response.xpath("//span[@class='sales']/span/text()").get("").strip()
            data["price_before"]= None
            data['promo_label']= None
            data =self.item_clean(data)
            if data['name']:
                item['count']=item['count']+1
                item['products'].append(data.copy())
        if not  bool(item):
            if "search"  not in url:
                title=url.strip("/").split("/")[-1].replace(".html","")
            else:
                title=title
            item['title']= re.sub('\s+', ' ',title )
            item['page_url']=url
            item['count']=0
            if category:
                item['category_crumb']=category
            item['products']=[]
            item['count']=0
        blocks = response.xpath("(//ul[@class='row product-grid-items'])[1]//article[@class='product']")
        if blocks == []:
            blocks = response.xpath("//article[@class='product']")
        for count,block in enumerate(blocks,1):
            rank=count
            product_url = response.urljoin(block.xpath(".//a/@href").get("")).replace("%2DGP","")
            json_data = json.loads(block.xpath("./div/@data-gtminfo").get("")).get("products", [])
            if json_data:
                json_data = json_data[0]
                stock = json_data.get("productStock", "")
                stock = True if "In stock" in stock else False
            if "noname" not in product_url:
                await product(product_url,item,rank)
        yield item


    def item_clean(self,item):
        for key, value in item.items():
            if isinstance(value, str):
                item[key]=value.strip()
                if value=="":
                    item[key]=None
        return item
