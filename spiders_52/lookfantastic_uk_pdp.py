import scrapy
import re
import json
from inline_requests import inline_requests
import os
import datetime


class LookfantasticSpider(scrapy.Spider):
    name = 'lookfantastic_uk_pdp'
    headers = {
  'accept-encoding': 'gzip, deflate, br',
  'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}   
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    ROTATING_PROXY = os.getenv("ROTATING_PROXY")
    custom_settings={"CONCURRENT_REQUESTS":5,
                    "HTTPCACHE_ENABLED" : True,
                    "HTTPCACHE_DIR" : 'httpcache',
                    'HTTPCACHE_EXPIRATION_SECS':79200,
                    "HTTPCACHE_STORAGE" : "scrapy.extensions.httpcache.FilesystemCacheStorage",
                    'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/lookfantastic_uk/%(name)s_{DATE}.json": {"format": "json",}},
                    'ROTATING_PROXY_LIST' : [ROTATING_PROXY]
                    }

    def start_requests(self):
        category_spider_name=(LookfantasticSpider.name).replace("pdp","categories")
        dir_path= os.getcwd()+rf"/exports/{category_spider_name}"
        with open(os.path.join(dir_path,f"{category_spider_name}_{self.CURRENT_DATE}.json"), 'r',encoding="utf-8") as f:
            contents = json.loads(f.read())
        for makeup in contents:
            first_name= makeup.get("name","")
            first_url=makeup.get("url","")
            for category in makeup.get("category_crumb",[]):
                second_name= category.get("name","")
                second_url= category.get("url","")
                for sub_category in category.get("category_crumb",[]):
                    third_name= sub_category.get("name","")
                    third_url=sub_category.get("url","")
                    category=[{"name":first_name,"url":first_url}.copy(),{"name":second_name,"url":second_url}.copy(),{"name":third_name,"url":third_url}.copy()]
                    if third_url:
                        yield scrapy.Request(third_url,headers=self.headers, callback=self.parse,cb_kwargs={"category":category})

    def parse(self, response,category=[]):
        for count,block in  enumerate(response.xpath("//li[contains(@class,'productListProducts_product')]"),1):
            url=response.urljoin(block.xpath(".//a[@class='productBlock_link']/@href").get(''))
            if  any(chr.isdigit() for chr in url):
                yield scrapy.Request(url,headers=self.headers,callback=self.parse_product,cb_kwargs={"category": category},dont_filter=True)
        next_page=response.xpath("//a[@aria-current='true']/parent::li/following-sibling::li[1]/a/@href").get("").strip()
        if next_page:
            next_page=response.urljoin(next_page)
            yield scrapy.Request(next_page,headers=self.headers, callback=self.parse,cb_kwargs={"category":category},dont_filter=True)

    @inline_requests
    def parse_product(self, response, category):
        no_product = response.xpath('//h2[@class="productAlternativesWrapper_title"]/text()').get('').strip()
        if not no_product:
            item = {}
            item["url"] = response.url
            item['image_url']=response.xpath("//*[@data-size='1600']/@data-path").getall()
            if  response.xpath("//*[@type='video/mp4']/@src").getall():
                item['has_video']=True
                item['video']= response.xpath("//*[@type='video/mp4']/@src").getall()
            else:
                item['has_video']=False
                item['video']= []
            item["name"] = response.xpath("//*[@class='productName_title']/text()").get("")
            item["subtitle"] =  response.xpath("//*[@class='productName_subtitle']/text()").get()
            item['description']=" ".join([ i.strip()for i in response.xpath("//*[@id='product-description-content-lg-2']/div//text()").getall() if i.strip()]).strip()
            item['category_crumb']=category
            item["brand"]= response.xpath("//div[@class='productBrandLogo']/img/@title").get()
            if item['brand'] is None:
                if re.findall("productBrand: \"(.*)\"",response.text):
                    item['brand']=re.findall("productBrand:\s*\"(.*)\"",response.text)[0]
            item["master_product_id"]= response.xpath("//div[@class='productPrice']/@data-product-id").get()
            json_data=json.loads( response.xpath("//*[@id='productSchema']/text()").get("{}"))
            item["mpn"]= json_data.get("mpn",None)
            if "InStock" in response.xpath("//*[@id='productSchema']/text()").get("").strip():
                item["in-stock"]= True
            else:
                item["in-stock"]= False
            item['price']=  response.xpath("//*[@class='productPrice_price  ']/text()").get()
            if response.xpath("//*[@class='productPrice_rrp']/text()").get():
                item["price_before"]=response.xpath("//*[@class='productPrice_rrp']/text()").get('').replace("RRP:","").strip()
            else:
                item["price_before"]=None
            item["promo_label"]= response.xpath("//*[@class='papDescription_title']/text()").get()
            item["prices"]= []
            product_ids=[]
            for variant in json_data.get("offers",[]):
                data={}
                if "instock" in variant.get("availability","").lower():
                    data["sku_id"] =variant.get("sku",None)
                    if data['sku_id']==item['master_product_id']:
                        continue
                    data["variant_url"]=variant.get("url",None)
                    product_ids.append(data["sku_id"])
                    variant_response=yield scrapy.Request(data["variant_url"],headers=self.headers,dont_filter=True)
                    data["image_url"] =variant_response.xpath("//*[@data-size='1600']/@data-path").getall()
                    if re.findall("selected>\n(.*)",variant_response.text):
                        data['data_color']=re.findall("selected>\n(.*)",variant_response.text)[0].strip()
                        if "button" in data['data_color']:
                            data['data_color']=variant_response.xpath("//span[contains(text(),'selected')]/parent::button/text()").get("").strip()
                    else:
                        data['data_color']=None
                        break
                    if data['data_color'].endswith("ml") or data['data_color'].endswith("tablets"):
                            data['data_size']=data['data_color']
                            data['data_color']=None
                    else:
                        data['data_size']=None
                    data["in-stock"]= True
                    if variant.get("price",""):
                        data["price"]= "£"+"{:.2f}".format(float(variant.get("price","")))
                    else:
                        data["price"]=None
                    if variant_response.xpath("//*[@class='productPrice_rrp']/text()").get(''):
                        data["price_before"]= variant_response.xpath("//*[@class='productPrice_rrp']/text()").get('').replace("RRP:","").strip()
                    else:
                        data["price_before"]=None
                    data["promo_label"] =variant_response.xpath("//*[@class='papDescription_title']/text()").get()
                    gtin = variant_response.xpath('//li[@class="athenaProductVariations_listItem"]/input[@data-selected]/parent::li/button/@data-option-barcode').get()
                    data['gtin'] = gtin if gtin else None
                    data =self.item_clean(data)
                    item["prices"].append(data.copy())
            variants=response.xpath(".//*[@class='athenaProductVariations_listItem']")
            if  len(item["prices"])<=1 and len(variants)>len(item["prices"]):
                for variant in variants:
                    product_id=variant.xpath(".//@data-linked-product-id").get()
                    if product_id not  in product_ids:
                        data={}
                        data["sku_id"] =product_id
                        data["gtin"] = variant.xpath(".//button/@data-option-barcode").get()
                        data["variant_url"]=response.url
                        image_url=f"https://www.lookfantastic.com/{product_id}.images?variation=false&isPersonalisableProduct=false&stringTemplatePath=components/athenaProductImageCarousel/athenaProductImageCarousel"
                        image_response=yield  scrapy.Request(image_url,headers=self.headers,dont_filter=True)
                        data["image_url"] =image_response.xpath("//*[@data-size='1600']/@data-path").getall()
                        data_variant=variant.xpath(".//button/text()").get('').strip()
                        if not data_variant:
                            data_variant=variant.xpath("./button/span/span/text()").get('').strip()
                        if data_variant.endswith("ml") or  data_variant.endswith("tablets"):
                            data['data_size']=data_variant
                            data['data_color']=None
                        else:
                            data['data_size']=None
                            data['data_color']=data_variant
                        data["in-stock"]= True
                        price_url=f"https://www.lookfantastic.com/{product_id}.price"
                        price_response=yield scrapy.Request(price_url,headers=self.headers,dont_filter=True)
                        if price_response.xpath("//*[@class='productPrice_price  ']/text()").get():
                            data["price"]= price_response.xpath("//*[@class='productPrice_price  ']/text()").get().strip()
                        else:
                            data["price"]=None
                        if price_response.xpath("//*[@class='productPrice_rrp']/text()").get(''):
                            data["price_before"]= price_response.xpath("//*[@class='productPrice_rrp']/text()").get('').replace("RRP:","").strip()
                        else:
                            data["price_before"]=None
                        data["promo_label"]=response.xpath("//*[@class='papDescription_title']/text()").get()
                        data =self.item_clean(data)
                        item["prices"].append(data.copy())
                        product_ids.append(data["sku_id"])
            item['reviews']=[]
            review_url=response.xpath("//*[@class='athenaProductReviews_seeReviewsButton']/@href").get('').strip()
            if review_url:
                yield response.follow(review_url,headers=self.headers, callback=self.reviews,cb_kwargs={"item":item},dont_filter=True)
            else:
                if  response.xpath("//div[@class='athenaProductReviews_topReviewSingle']"):
                    for block in response.xpath("//div[@class='athenaProductReviews_topReviewSingle']"):
                        data={}
                        header=block.xpath(".//*[@class='athenaProductReviews_topReviewTitle']/text()").get('').strip()
                        review_text=block.xpath(".//*[@class='athenaProductReviews_topReviewsExcerpt']/text()").get('').replace("\n"," ").strip()
                        review_text= re.sub('\s+', ' ', review_text)
                        data['review']=f"[{header}][{review_text}]"
                        data['stars']= int(block.xpath(".//*[@class='athenaProductReviews_topReviewsRatingStarsContainer']/@aria-label").get('0').replace("Stars","").strip())
                        data['user']=block.xpath(".//*[@data-js-element='createdDate']/following-sibling::span/text()").get()
                        data['date']=block.xpath(".//*[@data-js-element='createdDate']/text()").get()
                        data =self.item_clean(data)
                        item['reviews'].append(data.copy())
                item =self.item_clean(item)
                if item["name"] is not None:
                    yield item


    def reviews(self,response,item):
        for block in response.xpath("//*[@class='athenaProductReviews_review']"):
            data={}
            header=block.xpath(".//*[@class='athenaProductReviews_reviewTitle']/text()").get('').strip()
            review_text=block.xpath(".//*[@class='athenaProductReviews_reviewContent']/text()").get('').strip()
            review_text= re.sub('\s+', ' ', review_text)
            data['review']=f"[{header}][{review_text}]"
            data['stars']= int(block.xpath(".//*[@class='athenaProductReviews_reviewRatingStarsContainer']/@aria-label").get('0').replace("Stars","").strip())
            data['user']=block.xpath(".//*[@data-js-element='createdDate']/following-sibling::span/text()").get()
            data['date']=block.xpath(".//*[@data-js-element='createdDate']/text()").get()
            data =self.item_clean(data)
            item['reviews'].append(data.copy())
        review_url=response.xpath("//*[@aria-label='Next page']/@href").get('').strip()
        if review_url:
            yield response.follow(review_url,headers=self.headers, callback=self.reviews,cb_kwargs={"item":item},dont_filter=True)
        else:
            item =self.item_clean(item)
            yield item
    

    def item_clean(self,item):
        for key, value in item.items():
            if isinstance(value, str):
                item[key]=value.strip()
                if value=="":
                    item[key]=None
        return item

