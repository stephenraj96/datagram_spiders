import re
import os
import json
import html
import scrapy
import urllib.parse
from parsel import Selector
from datetime import datetime

class ShiseidofrPdpSpider(scrapy.Spider):
    name = 'shiseido_fr_pdp'
    CURRENT_DATETIME = datetime.now() #today's date
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d") #2023-04-25
    DATE=CURRENT_DATE.replace("-","_") #2023_04_25
    ROTATING_PROXY = os.getenv("ROTATING_PROXY_FR")
    custom_settings={
        'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/shiseido_fr/%(name)s_{DATE}.json": {"format": "json",}},
                     'ROTATING_PROXY_LIST' : [ROTATING_PROXY],
                     }
    def start_requests(self):
        category_spider_name=(ShiseidofrPdpSpider.name).replace("pdp","categories")
        dir_path= os.getcwd()+rf"/exports/{category_spider_name}"
        with open(os.path.join(dir_path,f"{category_spider_name}_{self.CURRENT_DATE}.json"), 'r',encoding="utf-8") as f:
            contents = json.loads(f.read())
        for makeup in contents:
            first_name= makeup.get("name","")
            first_url=makeup.get("url",'')
            if 'category_crumb' in (makeup).keys():
                if not makeup.get("category_crumb",[]) == []:
                    for category in makeup.get("category_crumb",[]):
                        second_name= category.get("name","")
                        second_url=category.get("url","")
                        if 'category_crumb' in (category).keys():
                            for sub_category in category.get("category_crumb",[]):
                                third_name= sub_category.get("name","")
                                third_url=sub_category.get("url","")
                                category=[{"name":first_name,"url":first_url}.copy(),{"name":second_name,"url":second_url}.copy(),{"name":third_name,"url":third_url}.copy()]
                                yield scrapy.Request(third_url, callback=self.parse,cb_kwargs={"category":category})
                        else:
                            category=[{"name":first_name,"url":first_url}.copy(),{"name":second_name,"url":second_url}.copy()]
                            yield scrapy.Request(second_url, callback=self.parse,cb_kwargs={"category":category})
                else:
                    category=[{"name":first_name,"url":first_url}.copy()]
                    yield scrapy.Request(first_url, callback=self.parse,cb_kwargs={"category":category})
    async def parse(self,response,category):
        count=response.xpath('//div[@class="page-count"]//text()').getall()
        count=' '.join(count)
        Total=re.findall(r'\d+',str(count))
        category_crumb=category

        titles=response.url.split('/')
        title=urllib.parse.unquote(titles[-1].upper())
        if title=='':
            title=urllib.parse.unquote(titles[-2].upper())
        if Total:
            headers={}
            if "/Search-Show?q-hint=&q=" not in response.url:
                url=response.url+f"?start=1&sz={Total[0]}&format=page-element"
                products_request=await self.request_process(url,headers=headers,payload={})
                products_response=Selector(products_request.text)
            else:
                products_response=response
            if (products_response.xpath('//div[@class="product-image"]')==[]) and (int(Total[0])>=1):
                products_response=response       
            
            for count,product in enumerate(products_response.xpath('//div[@class="product-image"]'),start=1):
                products={}
                product_link=product.xpath('./a[@class="thumb-link"]/@href').get('').strip()
                product_link=response.urljoin(product_link)
                if product_link:
                    product_request=await self.request_process(product_link,headers=headers,payload={})
                    product_response=Selector(product_request.text)
                    json_data=re.findall(r'\<script\slanguage\=\"javascript\"\stype\=\"text\/\S+\"\sorig\_index\=\"0\"\>\s+var\sproductCache\s\=\s([\w\W]+?)\;\s+var\spdpdata\s\=\sproductCache\;',product_request.text)
                    if json_data:
                        json_value=json.loads(json_data[0])
                        varients=json_value.get('variants','')
                        products['url']=product_link
                        products['image_url']=[str(i['url']).replace(' ','%20') for i in json_value['images']['hiRes']]
                        products['has_video']=False
                        products['video']=[]
                        products['name']=json_value['name']
                        products['subtitle']=product_response.xpath('(//div[@class="product-title "])[1]/h3/text()').get('').strip()
                        if products['subtitle']=='':
                            products['subtitle']=None
                        description=product_response.xpath('//div[@id="overview"]/div[@class="pdp-accordion__content-desc"]//text()').getall()
                        description = html.unescape(' '.join(description).strip())
                        products['description']=re.sub(r'\s+',' ',str(description))
                        if products['description']=='':
                            products['description']=None
                        products['brand']=json_value['customBrand']
                        products['master_product_id']=json_value['masterID']
                        products['gtin']=json_value['ID']
                        products['in-stock']=json_value['availability']['inStock']
                        products['price']=json_value['pricing']['formattedSale']
                        
                        products['price_before']=json_value['pricing']['formattedStandard']
                        if products['price_before']==products['price']:
                            products['price_before']=None
                        promo_label=product_response.xpath('//div[@class="discount-percentage clearfix desktop-only"]//text()').getall()
                        if products['price']=="N/A":
                            products['price']=None
                        promo_label=''.join(promo_label).strip()
                        products['promo_label']=promo_label
                        if products['promo_label']=='':
                            products['promo_label']=None
                        products['category_crumb']=category_crumb
                        products['prices']=[]
                        size=json_value.get('size',None)
                        if varients:
                            for varients_data in varients:
                                varient={}
                                varient['sku_id']=json_value['masterID']
                                varient['gtin']=json_value['variants'][varients_data]['id']
                                varient['variant_url']=re.sub(r'\-\d+.html',f"-{varient['gtin']}.html",product_link)
                                varient['has_video']=False
                                varient['video']=[]
                                varient['image_url']=[]
                                varient['data_color']=None 
                                varient['data_size']=None 
                                attr_check= json_value.get('variations',{}).get('attributes',{})
                                if attr_check:
                                    attr=attr_check[0].get('id','')
                                    if attr=='size':
                                        try:
                                            varient['data_size']=json_value.get('variants',{}).get(varients_data,{}).get('attributes',{}).get('size',None)
                                        except:
                                            varient['data_size']=None
                                        varient['image_url']=products['image_url']
                                    else:
                                        sku_id=str(varients_data).replace('color-','')
                                        if re.search(r'\d+',sku_id):
                                            varient['sku_id']=sku_id
                                        data_color=json_value['variants'][varients_data]['attributes']['color']
                                        images=[i['images']['hiRes'] for i in json_value['variations']['attributes'][0]['vals'] if i['val']==data_color]
                                        image=[str(j['url']).replace(' ','%20') for i in images for j in i]
                                        varient['image_url'].extend(image)
                                        color=[i['id'] for i in json_value['variations']['attributes'][0]['vals'] if i['val']==data_color]
                                        if color:
                                            varient['data_color']=color[0]
                                            if re.search(r'C\d+',varient['data_color']):
                                                varient['data_color']=data_color
                                            elif re.search(r'\d{9,12}',varient['data_color']):
                                                varient['data_color']=data_color
                                        else:
                                            varient['data_color']=varients_data.replace('color-','')
                                            if re.search(r'C\d+',varient['data_color']):
                                                varient['data_color']=data_color
                                            elif re.search(r'\d{9,12}',varient['data_color']):
                                                varient['data_color']=data_color
                                        if varient['image_url']==[]:
                                            varient['image_url']=products['image_url']
                                if varient['data_size']==None:
                                    varient['data_size']=size 
                                if varient['data_size']=="":
                                    varient['data_size'] = None
                                varient['in-stock']=json_value['variants'][varients_data]['availability']['inStock']
                                varient['price']  =json_value['variants'][varients_data]['pricing']['formattedSale']
                                
                                varient['price_before']=json_value['variants'][varients_data]['pricing']['formattedStandard']
                                if varient['price_before']==varient['price']:
                                    varient['price_before']=None
                                varient['promo_label'] =products['promo_label'] 
                                if varient['price']=="N/A":
                                    varient['price']=None
                                
                                products['prices'].append(varient)  
                        products['reviews']=[]
                        review_url=f'https://api.bazaarvoice.com/data/batch.json?passkey=ca8hpgS1rgi5Wgl3YIYhuDuSJ0D1XXr6BTo6pC5poFPB0&apiversion=5.5&displaycode=16276-fr_fr&resource.q0=products&filter.q0=id%3Aeq%3A{products["master_product_id"]}&stats.q0=reviews&filteredstats.q0=reviews&filter_reviews.q0=contentlocale%3Aeq%3Afr*%2Cen_US%2Cen_CA%2Cfr_FR&filter_reviewcomments.q0=contentlocale%3Aeq%3Afr*%2Cen_US%2Cen_CA%2Cfr_FR&resource.q1=reviews&filter.q1=isratingsonly%3Aeq%3Afalse&filter.q1=productid%3Aeq%3A{products["master_product_id"]}&filter.q1=contentlocale%3Aeq%3Afr*%2Cen_US%2Cen_CA%2Cfr_FR&sort.q1=relevancy%3Aa1&stats.q1=reviews&filteredstats.q1=reviews&include.q1=authors%2Cproducts%2Ccomments&filter_reviews.q1=contentlocale%3Aeq%3Afr*%2Cen_US%2Cen_CA%2Cfr_FR&filter_reviewcomments.q1=contentlocale%3Aeq%3Afr*%2Cen_US%2Cen_CA%2Cfr_FR&filter_comments.q1=contentlocale%3Aeq%3Afr*%2Cen_US%2Cen_CA%2Cfr_FR&limit.q1=100&offset.q1=0&limit_comments.q1=10&callback=BV._internal.dataHandler0'
                        headers = {
                        'Accept-Encoding':'gzip',
                        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
                        }
                        review_payload={}
                        review_response = await self.request_process(review_url,headers=headers,payload=review_payload)
                        review_data=re.findall(r'BV\.\_internal\.dataHandler0\((.*)\)',review_response.text)[0]
                        review_json=json.loads(review_data)
                        for reviews in review_json['BatchedResults']['q1']['Results']:
                            review_item={}
                            review_item["review"]=re.sub(r'\s+',' ',str(reviews.get("ReviewText","")))
                            review_item["stars"]=reviews["Rating"]
                            review_item["user"]=reviews.get("UserNickname","")
                            date=reviews.get("SubmissionTime","")
                            if date:
                                review_item["date"]=date.split('T')[0]
                            products['reviews'].append(review_item)
                        result=review_json['BatchedResults']['q1']['TotalResults']
                        result = result if result else 0
                        for page_count in range(1,round(int(result)/100)+1):
                            view_url=f'https://api.bazaarvoice.com/data/batch.json?passkey=ca8hpgS1rgi5Wgl3YIYhuDuSJ0D1XXr6BTo6pC5poFPB0&apiversion=5.5&displaycode=16276-fr_fr&resource.q0=products&filter.q0=id%3Aeq%3A{products["master_product_id"]}&stats.q0=reviews&filteredstats.q0=reviews&filter_reviews.q0=contentlocale%3Aeq%3Afr*%2Cen_US%2Cen_CA%2Cfr_FR&filter_reviewcomments.q0=contentlocale%3Aeq%3Afr*%2Cen_US%2Cen_CA%2Cfr_FR&resource.q1=reviews&filter.q1=isratingsonly%3Aeq%3Afalse&filter.q1=productid%3Aeq%3A{products["master_product_id"]}&filter.q1=contentlocale%3Aeq%3Afr*%2Cen_US%2Cen_CA%2Cfr_FR&sort.q1=relevancy%3Aa1&stats.q1=reviews&filteredstats.q1=reviews&include.q1=authors%2Cproducts%2Ccomments&filter_reviews.q1=contentlocale%3Aeq%3Afr*%2Cen_US%2Cen_CA%2Cfr_FR&filter_reviewcomments.q1=contentlocale%3Aeq%3Afr*%2Cen_US%2Cen_CA%2Cfr_FR&filter_comments.q1=contentlocale%3Aeq%3Afr*%2Cen_US%2Cen_CA%2Cfr_FR&limit.q1=100&offset.q1={100*page_count}&limit_comments.q1=3&callback=BV._internal.dataHandler0'
                            view_payload={}
                            view_response = await self.request_process(view_url,headers=headers,payload=view_payload)
                            view_data=re.findall(r'BV\.\_internal\.dataHandler0\((.*)\)',view_response.text)[0]
                            view_json=json.loads(view_data)
                            for reviews in view_json['BatchedResults']['q1']['Results']:
                                review_item={}
                                review_item["review"]=re.sub(r'\s+',' ',str(reviews.get("ReviewText","")))
                                review_item["stars"]=reviews["Rating"]
                                review_item["user"]=reviews.get("UserNickname","")
                                date=reviews.get("SubmissionTime","")
                                if date:
                                    review_item["date"]=date.split('T')[0]
                                products['reviews'].append(review_item)
                        yield products
                        

    async def request_process(self,url,headers,payload):
        if payload=={}:
            request=scrapy.Request(url)
        else:
            request=scrapy.Request(url,method='POST',headers=headers,body=payload)
        response = await self.crawler.engine.download(request, self)
        return response

