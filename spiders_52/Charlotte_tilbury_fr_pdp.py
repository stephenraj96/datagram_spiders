import os
import re
import json
import html
import scrapy
import urllib.parse
from datetime import datetime
from math import ceil

class CharlotteTilburyFRPdpSpider(scrapy.Spider):
    name = 'Charlotte_tilbury_fr_pdp'
    CURRENT_DATETIME = datetime.now() #today's date
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d") #2023-04-25
    DATE=CURRENT_DATE.replace("-","_") #2023_04_25
    ROTATING_PROXY = os.getenv("ROTATING_PROXY_FR")
    custom_settings={'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/Charlotte_tilbury_fr/%(name)s_{DATE}.json": {"format": "json",}},
                    'ROTATING_PROXY_LIST' : [ROTATING_PROXY]
    }

    def start_requests(self): 
        category_spider_name=(CharlotteTilburyFRPdpSpider.name).replace("pdp","categories")
        dir_path= os.getcwd()+rf"/exports/{category_spider_name}"
        
        with open(os.path.join(dir_path,f"{category_spider_name}_{self.CURRENT_DATE}.json"), 'r',encoding="utf-8") as f:
            contents = json.loads(f.read())
        
        for makeup in contents:
            first_name= makeup.get("name","")
            first_url=makeup.get("url",'')
            for category in makeup.get("category_crumb",[]):
                second_name= category.get("name","")
                second_url=category.get("url","")

                if second_url.__contains__('soin') or second_url.__contains__('soins'):
                    category=[{"name":first_name,"url":first_url}.copy(),{"name":second_name,"url":second_url}.copy()]
                    if  not any(chr.isdigit() for chr in second_url) or "maquillage" in second_url:
                        yield scrapy.Request(second_url,callback=self.parse,cb_kwargs={"category":category})
                else:
                    for sub_category in category.get("category_crumb",[]):
                        third_name= sub_category.get("name","")
                        third_url=sub_category.get("url","")
                        category=[{"name":first_name,"url":first_url}.copy(),{"name":second_name,"url":second_url}.copy(),{"name":third_name,"url":third_url}.copy()]
                        if not any(chr.isdigit() for chr in third_url) or "soin" in third_url:
                            yield scrapy.Request(third_url, callback=self.parse,cb_kwargs={"category":category})

    async def parse(self, response,category):
        regex_data=re.findall(r'\<script\sid\=\"\_\_NEXT\_DATA\_\_\"\stype\=\"application\/json\"\>([\w\W]+)\<\/script\>',response.text)[0]
        json_data=json.loads(regex_data)
        category_crumb=category
        data=[]
        if '?search=' in response.url:
            
            search_value=str(response.url).split('?search=')[-1]
            post_url="https://ztf0lv96g2-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.11.0)%3B%20Browser%20(lite)%3B%20JS%20Helper%20(3.11.3)%3B%20react%20(17.0.2)%3B%20react-instantsearch%20(6.39.0)&x-algolia-api-key=e3c8012d28b6c013906005377ec04f03&x-algolia-application-id=ZTF0LV96G2"
            payload='{"requests":[{"indexName":"Products_Store_UK","params":"clickAnalytics=true&facets=%5B%5D&filters=prices.listingPrices.currencyCode%3AGBP&highlightPostTag=%3C%2Fais-highlight-0000000000%3E&highlightPreTag=%3Cais-highlight-0000000000%3E&hitsPerPage=100&page=0&query='+urllib.parse.quote(search_value)+'&ruleContexts=%5B%22web_search%22%5D&tagFilters=&"},{"indexName":"Products_Store_UK_Query_Suggestions","params":"clickAnalytics=true&facets=%5B%5D&highlightPostTag=%3C%2Fais-highlight-0000000000%3E&highlightPreTag=%3Cais-highlight-0000000000%3E&hitsPerPage=100&page=0&query='+urllib.parse.quote(search_value)+'&ruleContexts=%5B%22web_search%22%5D&tagFilters=&"}]}'
          
            headers={
                'Referer': 'https://www.charlottetilbury.com/',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
                }
          
            search_request=await self.request_process(post_url,headers=headers,payload=payload)
            search_response=search_request.json()
            data=search_response['results'][0]['hits']
        else:
            data=json_data.get('props',{}).get('products',[]) 

        for product in data:
            products={}
            product_url=f'https://www.charlottetilbury.com/fr/product/{product["href"]}'

            if not product_url.__contains__("build-your-own"):
                product_headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}
                product_payload={}
                product_response= await self.request_process(product_url,headers=product_headers,payload=product_payload)
                product_regex_data=re.findall(r'\<script\sid\=\"\_\_NEXT\_DATA\_\_\"\stype\=\"application\/json\"\>([\w\W]+)\<\/script\>',product_response.text)[0]
                product_json_data=json.loads(product_regex_data)            
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
                products["name"]= product_json_data["props"]["product"]['title']
                subtitle=product_json_data["props"]["product"].get('subtitle',None)
                products["subtitle"]=subtitle
                description=product_json_data['props']['product']['longDescription']
                description=re.sub(r'\<\/*[^>.*]\>',' ',description)
                description=re.sub(r'<[^>]*?>',' ',description)
                description=re.sub(r'\s+',' ',html.unescape(description))
                
                products['description']=description
                products["brand"]= "Charlotte Tilbury"
                products["master_product_id"]= product_json_data["props"]["product"]["sku"]
                products["in-stock"]= True  # True if the product can be added to the cart, False otherwise
                products["price"]=None
                price= product_json_data.get("props",{}).get("product",{}).get("price",{}).get("purchasePrice",{}).get("value","")
                if price:
                    products["price"]= f'€{"%.2f" %price}'
                
                products["price_before"]=None
                price_before=product_json_data.get("props",{}).get("product",{}).get("price",{}).get("listingPrice",{}).get("value","")
                if price_before:
                    products["price_before"]= f'€{"%.2f" %price_before}' # price before the discount if exist else None
                products["promo_label"]= None
                promo_lable=product_json_data["props"]["product"]["badge"]
                if promo_lable:
                    if promo_lable!="SUBSCRIBE!":
                        products["promo_label"]= promo_lable# discount label if exist else None
                if products["price"]==products["price_before"]:
                    products["price_before"]=None
                if products["price_before"]==None:
                    products["promo_label"]=None
                products['prices']=[]
                
                sibling=product_json_data.get('props',{}).get('siblings','')
                if sibling:
                    for siblings in product_json_data['props']['siblings']:
                        sib={}
                        sib["sku_id"]= siblings["sku"]
                        sib["variant_url"]= f'https://www.charlottetilbury.com/fr/product/{siblings["href"]}'
                        sib["image_url"]= [f"https:{i['imageSrc']}" for i in siblings['images']] 
                        sib["price"]= None
                        price= siblings.get("price",{}).get("purchasePrice",{}).get("value","")
                        if price:
                            sib["price"]= f'{"%.2f" %price} €'
                        sib["price_before"]= None
                        price_before= siblings.get("price",{}).get("listingPrice",{}).get("value","") # price before the discount if exist else None
                        if price_before:
                            sib["price_before"]= f'{"%.2f" %price_before} €'
                        sib_subtitle=siblings.get('subtitle','')
                    
                        sib["data_size"]=None
                        sib["data_color"]=None
                        if re.search(r'\d+',sib_subtitle):
                            sib["data_size"]=sib_subtitle
                        else:
                            sib["data_color"]=sib_subtitle
                    
                        sib["in-stock"]= False
                        in_stock=siblings['availability']
                        if in_stock=="AVAILABLE":
                            sib["in-stock"]= True
                        if sib["price"] == sib["price_before"]:
                            sib["price_before"]=None
                        if sib["price_before"]==None:
                            sib["promo_label"]=None
                        products['prices'].append(sib)
                products['category_crumb']=category_crumb
                products['reviews']=[]
                review_url=f'https://api.bazaarvoice.com/data/batch.json?passkey=ca5riN2Whh8SPkZlUOKEwP83fc1bGEmNsfm8cJWC988B8&apiversion=5.5&displaycode=16153-fr_fr&resource.q0=products&filter.q0=id%3Aeq%3A{products["master_product_id"]}&stats.q0=reviews&filteredstats.q0=reviews&filter_reviews.q0=contentlocale%3Aeq%3Aen*%2Cfr*%2Cfr_FR&filter_reviewcomments.q0=contentlocale%3Aeq%3Aen*%2Cfr*%2Cfr_FR&resource.q1=reviews&filter.q1=isratingsonly%3Aeq%3Afalse&filter.q1=productid%3Aeq%3A{products["master_product_id"]}&filter.q1=contentlocale%3Aeq%3Aen*%2Cfr*%2Cfr_FR&sort.q1=relevancy%3Aa1&stats.q1=reviews&filteredstats.q1=reviews&include.q1=authors%2Cproducts%2Ccomments&filter_reviews.q1=contentlocale%3Aeq%3Aen*%2Cfr*%2Cfr_FR&filter_reviewcomments.q1=contentlocale%3Aeq%3Aen*%2Cfr*%2Cfr_FR&filter_comments.q1=contentlocale%3Aeq%3Aen*%2Cfr*%2Cfr_FR&limit.q1=100&offset.q1=0&limit_comments.q1=10&resource.q2=reviews&filter.q2=productid%3Aeq%3A{products["master_product_id"]}&filter.q2=contentlocale%3Aeq%3Aen*%2Cfr*%2Cfr_FR&limit.q2=100&resource.q3=reviews&filter.q3=productid%3Aeq%3A{products["master_product_id"]}&filter.q3=isratingsonly%3Aeq%3Afalse&filter.q3=issyndicated%3Aeq%3Afalse&filter.q3=rating%3Agt%3A3&filter.q3=totalpositivefeedbackcount%3Agte%3A3&filter.q3=contentlocale%3Aeq%3Aen*%2Cfr*%2Cfr_FR&sort.q3=totalpositivefeedbackcount%3Adesc&include.q3=authors%2Creviews%2Cproducts&filter_reviews.q3=contentlocale%3Aeq%3Aen*%2Cfr*%2Cfr_FR&limit.q3=1&resource.q4=reviews&filter.q4=productid%3Aeq%3A{products["master_product_id"]}&filter.q4=isratingsonly%3Aeq%3Afalse&filter.q4=issyndicated%3Aeq%3Afalse&filter.q4=rating%3Alte%3A3&filter.q4=totalpositivefeedbackcount%3Agte%3A3&filter.q4=contentlocale%3Aeq%3Aen*%2Cfr*%2Cfr_FR&sort.q4=totalpositivefeedbackcount%3Adesc&include.q4=authors%2Creviews%2Cproducts&filter_reviews.q4=contentlocale%3Aeq%3Aen*%2Cfr*%2Cfr_FR&limit.q4=100&callback=BV._internal.dataHandler0'
                headers = {
                'Referer': 'https://www.charlottetilbury.com/',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
                }

                review_payload={}
                review_response = await self.request_process(review_url,headers=headers,payload=review_payload)
                review_data=re.findall(r'BV\.\_internal\.dataHandler0\((.*)\)',review_response.text)[0]
                review_json=json.loads(review_data)
                result=review_json['BatchedResults']['q2']['TotalResults']  
                t_result=round(int(result)/100)

                for i in range(0,t_result+1):
                    view_url=f'https://api.bazaarvoice.com/data/batch.json?passkey=ca5riN2Whh8SPkZlUOKEwP83fc1bGEmNsfm8cJWC988B8&apiversion=5.5&displaycode=16153-fr_fr&resource.q0=products&filter.q0=id%3Aeq%3A{products["master_product_id"]}&stats.q0=reviews&filteredstats.q0=reviews&filter_reviews.q0=contentlocale%3Aeq%3Aen*%2Cfr*%2Cfr_FR&filter_reviewcomments.q0=contentlocale%3Aeq%3Aen*%2Cfr*%2Cfr_FR&resource.q1=reviews&filter.q1=isratingsonly%3Aeq%3Afalse&filter.q1=productid%3Aeq%3A{products["master_product_id"]}&filter.q1=contentlocale%3Aeq%3Aen*%2Cfr*%2Cfr_FR&sort.q1=relevancy%3Aa1&stats.q1=reviews&filteredstats.q1=reviews&include.q1=authors%2Cproducts%2Ccomments&filter_reviews.q1=contentlocale%3Aeq%3Aen*%2Cfr*%2Cfr_FR&filter_reviewcomments.q1=contentlocale%3Aeq%3Aen*%2Cfr*%2Cfr_FR&filter_comments.q1=contentlocale%3Aeq%3Aen*%2Cfr*%2Cfr_FR&limit.q1=0&limit_comments.q1=10&resource.q2=reviews&filter.q2=productid%3Aeq%3A{products["master_product_id"]}&filter.q2=contentlocale%3Aeq%3Aen*%2Cfr*%2Cfr_FR&limit.q2=100&offset.q2={i*100}&limit_comments.q2=10&resource.q3=reviews&filter.q3=productid%3Aeq%3A{products["master_product_id"]}&filter.q3=isratingsonly%3Aeq%3Afalse&filter.q3=issyndicated%3Aeq%3Afalse&filter.q3=rating%3Agt%3A3&filter.q3=totalpositivefeedbackcount%3Agte%3A3&filter.q3=contentlocale%3Aeq%3Aen*%2Cfr*%2Cfr_FR&sort.q3=totalpositivefeedbackcount%3Adesc&include.q3=authors%2Creviews%2Cproducts&filter_reviews.q3=contentlocale%3Aeq%3Aen*%2Cfr*%2Cfr_FR&limit.q3=100&resource.q4=reviews&filter.q4=productid%3Aeq%3A{products["master_product_id"]}&filter.q4=isratingsonly%3Aeq%3Afalse&filter.q4=issyndicated%3Aeq%3Afalse&filter.q4=rating%3Alte%3A3&filter.q4=totalpositivefeedbackcount%3Agte%3A3&filter.q4=contentlocale%3Aeq%3Aen*%2Cfr*%2Cfr_FR&sort.q4=totalpositivefeedbackcount%3Adesc&include.q4=authors%2Creviews%2Cproducts&filter_reviews.q4=contentlocale%3Aeq%3Aen*%2Cfr*%2Cfr_FR&limit.q4=100&callback=BV._internal.dataHandler0'
                    view_payload={}
                    view_response = await self.request_process(view_url,headers=headers,payload=view_payload)
                    view_data=re.findall(r'BV\.\_internal\.dataHandler0\((.*)\)',view_response.text)[0]
                    view_json=json.loads(view_data)
                    
                    for reviews in view_json['BatchedResults']['q2']['Results']:
                        review_item={}
                        review_title = re.sub(r'\s+',' ',str(reviews.get("Title","")))
                        review_content = re.sub(r'\s+',' ',str(reviews.get("ReviewText","")))
                        if review_title != 'None':
                            review_content = re.sub(r'\s+',' ',str(reviews.get("ReviewText","")))
                            review_item["review"]=f"[{review_title}] {review_content}"
                            review_item["stars"]=reviews["Rating"]
                            review_item["user"]=reviews.get("UserNickname","")
                            date=reviews.get("SubmissionTime","")
                            if date:
                                review_item["date"]=date.split('T')[0]
                            
                            products['reviews'].append(review_item)
                # print(products)  
                yield products

    async def request_process(self,url,headers,payload):
        if payload=={}:
            request=scrapy.Request(url)
        else:
            request=scrapy.Request(url,method='POST',headers=headers,body=payload)
        response = await self.crawler.engine.download(request, self)
        return response
