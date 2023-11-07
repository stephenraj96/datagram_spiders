import scrapy
import os
import json
import datetime
import re
from dotenv import load_dotenv
from pathlib import Path

""" load env file """
try:
    load_dotenv()
except:
    env_path = Path(".env")
    load_dotenv(dotenv_path=env_path)

class GuerlainSpider(scrapy.Spider):
    name = 'guerlain_fr_pdp'
    headers = {'accept-encoding': 'gzip, deflate, br',
                'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    ROTATING_PROXY = os.getenv("ROTATING_PROXY_FR")
    custom_settings={"HTTPCACHE_ENABLED" : True,
                    "HTTPCACHE_DIR" : 'httpcache',
                    'HTTPCACHE_EXPIRATION_SECS':72000,
                    "HTTPCACHE_STORAGE" : "scrapy.extensions.httpcache.FilesystemCacheStorage",
                    'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/guerlain_fr/%(name)s_{DATE}.json": {"format": "json",}},
                    'ROTATING_PROXY_LIST' : [ROTATING_PROXY]
                    }

    def start_requests(self):
        self.spider_name=self.name
        category_spider_name=(GuerlainSpider.name).replace("pdp","categories")
        dir_path= os.getcwd()+rf"/exports/{category_spider_name}"
        with open(os.path.join(dir_path,f"{category_spider_name}_{self.CURRENT_DATE}.json"), 'r',encoding="utf-8") as f:
            contents = json.loads(f.read())
        for makeup in contents:
            first_name= makeup.get("name","")
            first_url=makeup.get("url",'')
            for category in makeup.get("category_crumb",[]):
                second_name= category.get("name","")
                second_url=category.get("url","")
                for sub_category in category.get("category_crumb",[]):
                    third_name= sub_category.get("name","")
                    third_url=sub_category.get("url","")
                    category=[{"name":first_name,"url":first_url}.copy(),{"name":second_name,"url":second_url}.copy(),{"name":third_name,"url":third_url}.copy()]
                    if  not any(chr.isdigit() for chr in third_url) or "soin" in third_url:
                        yield scrapy.Request(third_url,headers=self.headers, callback=self.parse,cb_kwargs={"category":category})
                    else:
                        yield scrapy.Request(third_url,headers=self.headers, callback=self.parse_product,cb_kwargs={"category":category})

    def parse(self, response,category=[]):
        if response.xpath("//div[@class='row tab-category swiper-container']//div[@class='swiper-slide item']/a"):
            for i in response.xpath("//div[@class='row tab-category swiper-container']//div[@class='swiper-slide item']/a"):
                url=i.xpath("./@href").get("")
                yield response.follow(url, callback=self.parse,cb_kwargs={"category":category})
        blocks = response.xpath("//article[@class='product']")
        for block in blocks:
            url = response.urljoin(block.xpath(".//a/@href").get(""))
            if "personnalisable" not in url:
                yield scrapy.Request(url,callback=self.parse_product,cb_kwargs={"category": category},dont_filter=True)

    async def parse_product(self,response,category):
        item={}
        item['url']=response.url
        images= response.xpath("//div[contains(@class,'in-zoom')]/@zoom-src").getall()
        item['image_url']=[]
        if images:
            for image in images:
                item['image_url'].append(image.strip().replace(" ","%20"))
        item['has_video']= False
        item['video']= []
        item['master_product_id']=response.xpath("//article[@class='product']/@data-pid|//meta[@itemprop='productID']/@content").get("")
        item['gtin'] = None
        product_gtin_api_url = f'https://api.bazaarvoice.com/data/products.json?passkey=caT4MV4Fmj6JY7lwJQiuRHXmpM3uzNP2mC22CFOIFnCe8&locale=fr_FR&allowMissing=true&apiVersion=5.4&filter=id:{item["master_product_id"]}'
        product_gtin_api_response = await self.requests_process(product_gtin_api_url)
        if product_gtin_api_response.json().get('Results',[]):
            gtin = product_gtin_api_response.json().get('Results',[])[0]
            if gtin.get('EANs',[]):
                item["gtin"]= gtin.get('EANs',[])[0]
        item['name']=response.xpath("//p[@class='product-desc']/text()").get("")
        item["brand"]= "Guerlain"
        if response.xpath("//*[contains(text(),'Ajouter au panier')]"):
            item["in-stock"]=True
        else:
            item["in-stock"]=False
        item["price"]= response.xpath("//span[@class='sales']/span/text()").get("").strip()
        item["price_before"]= None
        item['promo_label']= None
        item['prices']=[]
        variant_url=""
        if response.xpath("//div[@id='color-swatchs-palette']/ul/li"):
            variant_url=response.xpath("//div[@id='color-swatchs-palette']/ul/li/button/@data-url").get("")
        elif response.xpath("//div[@id='simplebar-content']/ul/li"):
            variant_url=response.xpath("//div[@id='simplebar-content']/ul/li/button/@data-url").get('')
        if variant_url:
            request=scrapy.Request(response.urljoin(variant_url))
            next_response = await self.crawler.engine.download(request, self)
            json_data=json.loads(next_response.text)
            for variant in  json_data.get("product",{}).get("variantList",[]):
                if variant.get("online",None):
                    data={}
                    gtin_api_url = f'https://api.bazaarvoice.com/data/products.json?passkey=caT4MV4Fmj6JY7lwJQiuRHXmpM3uzNP2mC22CFOIFnCe8&locale=fr_FR&allowMissing=true&apiVersion=5.4&filter=id:{variant.get("prodID","").strip()}'
                    gtin_api_response = await self.requests_process(gtin_api_url)
                    data['variant_url']=  response.urljoin(variant.get('selectedProductUrl',""))
                    data['sku_id']= variant.get("prodID","").strip()
                    data['image_url']=[]
                    for image in variant.get("images",{}).get("large",[]):
                        if image.get("url",None):
                            data['image_url'].append(image.get("url","").replace(" ",'%20'))
                    data['price']=variant.get("price",{}).get("sales",{}).get("formatted",'').strip()
                    data["price_before"]= None
                    data["promo_label"]= None
                    if gtin_api_response.json().get('Results',[]):
                        data["gtin"]=gtin_api_response.json().get('Results',[])[0].get('EANs',[])[0]
                    else:
                        data["gtin"] = None
                    
                    data['in-stock']=True if variant.get("available",0)==1 else False
                    data_variant= variant.get("customName","").strip()
                    if data_variant.lower().endswith("ml") or re.findall('\d+g',data_variant):
                        data['data_size']=data_variant
                        data['data_color']=None
                    else:
                        data['data_size']=None
                        data['data_color']=data_variant
                    data =self.item_clean(data)
                    item['prices'].append(data.copy()) 
        item["description"]= " ".join([i.replace("\n","").strip() for i in response.css("div.text-description.text-description-product.collapsed::text ").getall() if i.strip() and "Voir plus" not in i and "Voir moins" not in i]).strip()
        item['subtitle'] = None
        item['category_crumb']=category
        item['reviews']=[]
        product_id=item['master_product_id'][1:]
        review_url=f"https://api.bazaarvoice.com/data/batch.json?passkey=caAmR29taoYvFuJaRUyVw9kSYlxoWAJ5eOglga6Z702mw&apiversion=5.5&displaycode=13364-fr_fr&resource.q0=reviews&filter.q0=isratingsonly%3Aeq%3Afalse&filter.q0=productid%3Aeq%3AG{product_id}&filter.q0=contentlocale%3Aeq%3Afr%2Cfr_FR&sort.q0=submissiontime%3Adesc&stats.q0=reviews&filteredstats.q0=reviews&include.q0=authors%2Cproducts%2Ccomments&filter_reviews.q0=contentlocale%3Aeq%3Afr%2Cfr_FR&filter_reviewcomments.q0=contentlocale%3Aeq%3Afr%2Cfr_FR&filter_comments.q0=contentlocale%3Aeq%3Afr%2Cfr_FR&limit.q0=100&offset.q0=0&limit_comments.q0=3&callback=bv_351_34293"
        yield response.follow(review_url,headers=self.headers, callback=self.reviews,cb_kwargs={"item":item,"offset":0,"product_id":product_id},dont_filter=True)


    def reviews(self,response,item,offset,product_id):
        json_data=json.loads(response.text.replace('bv_351_34293(',"")[:-1])
        results=json_data.get("BatchedResults",{})
        total_count=results.get("q0",{}).get('TotalResults','')
        for row in results.get("q0",{}).get("Results",[]):
            data={}
            title=row.get("Title","")
            review_text=row.get("ReviewText","").strip()
            review_text=re.sub('\s+', ' ',review_text )
            data['review']=f'[{title}][{review_text}]'
            data['user']=row.get("UserLocation","")
            data['stars']=row.get("Rating",0)
            data['date']=row.get("LastModificationTime","").strip().split("T")[0]
            item['reviews'].append(data.copy())
        offset=offset+100
        if offset<total_count:
            review_url=f"https://api.bazaarvoice.com/data/batch.json?passkey=caAmR29taoYvFuJaRUyVw9kSYlxoWAJ5eOglga6Z702mw&apiversion=5.5&displaycode=13364-fr_fr&resource.q0=reviews&filter.q0=isratingsonly%3Aeq%3Afalse&filter.q0=productid%3Aeq%3AG{product_id}&filter.q0=contentlocale%3Aeq%3Afr%2Cfr_FR&sort.q0=submissiontime%3Adesc&stats.q0=reviews&filteredstats.q0=reviews&include.q0=authors%2Cproducts%2Ccomments&filter_reviews.q0=contentlocale%3Aeq%3Afr%2Cfr_FR&filter_reviewcomments.q0=contentlocale%3Aeq%3Afr%2Cfr_FR&filter_comments.q0=contentlocale%3Aeq%3Afr%2Cfr_FR&limit.q0=100&offset.q0={offset}&limit_comments.q0=3&callback=bv_351_34293"
            yield response.follow(review_url,headers=self.headers, callback=self.reviews,cb_kwargs={"item":item,"offset":offset,"product_id":product_id},dont_filter=True)
        else:
            item =self.item_clean(item)
            if item['name'] is not None:
                yield item

    def item_clean(self,item):
        for key, value in item.items():
            if isinstance(value, str):
                item[key]=value.strip()
                if value=="":
                    item[key]=None
        return item

        
    async def requests_process(self,url):
            request = scrapy.Request(url)
            response = await self.crawler.engine.download(request, self)
            return response






