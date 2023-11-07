import re
import os
import json
import scrapy
import datetime
import traceback2 as traceback
from dotenv import load_dotenv

load_dotenv()
class BobbieOffertUKSpider(scrapy.Spider):
    name = "bobbi_brown_uk_offers"
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    ROTATING_PROXY = os.getenv("ROTATING_PROXY")
    custom_settings={
                    'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/bobbi_brown_uk/%(name)s_{DATE}.json": { "format": "json",}},
                    'ROTATING_PROXY_LIST' : [ROTATING_PROXY],
                    'CONCURRENT_REQUESTS' : 1,
                    'DOWNLOAD_DELAY' : 15
                    }
    async def request_process(self, url,headers):
        try:
            if headers:
                request = scrapy.Request(url,headers=headers)
            else:
                request = scrapy.Request(url)
            response = await self.crawler.engine.download(request, self)
            return response
        except:
            print(traceback.format_exc())
    def regex_parse(self,pattern,text):
        if re.search(pattern,text,re.I):
            data = re.findall(pattern,text,re.I)
            return data[0]
        else:
            return ''

    def clean_text(self, text):
        text = re.sub(r'<.*?>','',str(text))
        return text
    
    async def sub_data_extraction(self,category_name,main_category,sub_category,response,product_url,product_list,rank):
        try:
            item = {}
            headers=None
            parse_resp = await self.request_process(product_url,headers)
            if re.search(r'\"Product\"\,\"name\"\:\"(.*?)\"',parse_resp.text):
                sku_id = self.regex_parse(r'\"sku\"\:\"(.*?)\"',parse_resp.text)
                item['rank'] = rank
                item['name'] = self.regex_parse(r'\"Product\"\,\"name\"\:\"(.*?)\"',parse_resp.text)
                item['url'] = product_url
                item['master_product_id'] = self.regex_parse(r'product\_base\_id\"\:\[(.*?)\]',parse_resp.text)
                item['gtin'] = None
                if re.search(r'\"UPC_CODE\"\:\"([^>]*?)\"',parse_resp.text):
                    gtin = re.findall(r'\"UPC_CODE\"\:\"([^>]*?)\"',parse_resp.text)[0]
                    item["gtin"] = gtin if gtin else None
                item['brand'] = 'Bobbi Brown'
                item['image_url'] = [response.urljoin(e) for e in parse_resp.xpath('//img[@data-swap-image-field="IMAGE_LARGE_COMBINED"]/@src').extract()]
                video_url_id = parse_resp.xpath('//div[contains(@class,"product-how-to-video__container")]/@data-youtube-id').get("")
                item['has_video'] = False
                item['video'] = []
                if video_url_id:
                    item['has_video'] = True
                    video_url_id = video_url_id.split('&')[0]
                    item['video'] = [f'https://www.youtube.com/embed/{video_url_id}']
                
                stock = ''
                if re.findall(r'\"availability\"\:\"http\:\/\/schema\.org\/(.*?)\"\,\"sku\"\:\"(.*?)\"',parse_resp.text,re.DOTALL):
                    stock_dump = re.findall(r'\"availability\"\:\"http\:\/\/schema\.org\/(.*?)\"\,\"sku\"\:\"(.*?)\"',parse_resp.text,re.DOTALL)
                    for stock in stock_dump:
                        if stock[1] == sku_id:
                            stock = stock[0]
                            break
                        else:
                            continue
                if stock == 'InStock':
                    item['in-stock'] = True
                else:
                    item['in-stock'] = False
                item['price'] = self.regex_parse(r'product\_price\"\:\[\"(.*?)\"\]',parse_resp.text)
                if item['price']:
                    item['price'] = '£' + "%.2f" %float(item['price'])
                else:
                    item['price'] = None
                item['price_before'] = self.regex_parse(r'product\_was\_price\"\:\[\"(.*?)\"]',parse_resp.text)
                if item['price_before']:
                    item['price_before'] = '£' + "%.2f" %float(item['price_before'])
                else:
                    item['price_before'] = None
                if item['price_before']:
                    item['promo_label'] = parse_resp.xpath('//div[@class="product-full__offer-text"]/p/span/text()').get('').strip()
                else:
                    item['promo_label'] = None
                product_list.append(item)
                
            else:
                with open('error_product_bobbie_brown_fr.txt','a') as f: f.write(product_url+'\n')
            
        except Exception as e:
            print(traceback.format_exc())


    async def data_extraction(self,sub_cat_url,category_name,category_url,main_category,main_category_url,sub_category,response):
        try:
            sub_cat_parse = await self.request_process(sub_cat_url,headers=None)
            main_item = {}
            if not re.search(r'\"Product\"\,\"name\"\:\"(.*?)\"',sub_cat_parse.text):
                product_urls = sub_cat_parse.xpath('//div[@class="product-brief__headline"]/a/@href|//div[@class="product-brief__image-container-top js-product-brief-image-container-top"]/a[1]/@href').getall()
                category_crumb = []
                if sub_category == '':
                    main_item['title'] = main_category
                    main_item['page_url'] = main_category_url
                    main_item['count'] = len(product_urls)
                    category_dict = {'name':category_name,'url':category_url}
                    main_category_dict = {'name':main_category,'url':main_category_url}
                    category_crumb.append(category_dict)
                    category_crumb.append(main_category_dict)
                    main_item['category_crumb'] = category_crumb
                else:
                    main_item['title'] = sub_category
                    main_item['page_url'] = sub_cat_url
                    main_item['count'] = len(product_urls)
                    category_dict = {'name':category_name,'url':category_url}
                    main_category_dict = {'name':main_category,'url':main_category_url}
                    sub_category_dict = {'name':sub_category,'url':sub_cat_url}
                    category_crumb.append(category_dict)
                    category_crumb.append(main_category_dict)
                    category_crumb.append(sub_category_dict)
                    main_item['category_crumb'] = category_crumb
                product_list = []
                for rank,product_url in enumerate(product_urls,1):
                    product_url = response.urljoin(product_url)
                    sub_data = await self.sub_data_extraction(category_name,main_category,sub_category,response,product_url,product_list,rank)                    
                
                main_item['products'] = product_list
                return main_item
            else:
                product_list = []
                rank = 1
                product_url = sub_cat_url
                sub_data = await self.sub_data_extraction(category_name,main_category,sub_category,response,product_url,product_list,rank)
                return product_list

        except Exception as e:
            print(traceback.format_exc())
            with open('main_error_uk_boobie_text.txt','a') as f: f.write(str(e)+'\n')

    def start_requests(self):
        urls = ['https://www.bobbibrown.co.uk/products/2321/makeup']
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    async def parse(self, response):
        try:
            spider_name = BobbieOffertUKSpider.name.replace('offers','categories')
            current_date = datetime.datetime.now().strftime("%Y-%m-%d")
            category_path = os.getcwd() + f'/exports/{spider_name}/{spider_name}_{current_date}.json'
            with open(category_path,'r',encoding='utf-8-sig') as f:
                json_file = f.read()
            category_names = json.loads(json_file)
            for main_cat in category_names:
                category_name = main_cat.get('name','').title()
                category_url = main_cat.get('url','')
                for main_sub_cat in main_cat['category_crumb']:
                    if 'category_crumb' in (main_sub_cat).keys():
                        main_category = main_sub_cat.get('name','').title()
                        main_category_url = main_sub_cat.get('url','')
                        for sub_cat in main_sub_cat['category_crumb']:
                            sub_category = sub_cat.get('name','').title()
                            sub_cat_url = sub_cat.get('url','')
                            data_list = await self.data_extraction(sub_cat_url,category_name,category_url,main_category,main_category_url,sub_category,response)
                            yield data_list
                    else:
                        
                        main_category = main_sub_cat.get('name','')
                        main_category_url = main_sub_cat.get('url','')
                        sub_category = ''
                        sub_cat_url = main_sub_cat.get('url','')  
                        data_list = await self.data_extraction(sub_cat_url,category_name,category_url,main_category,main_category_url,sub_category,response)
                        yield data_list
            dir_path=os.path.abspath(__file__ + "/../../../")
            supporting_files=os.path.join(dir_path,"supporting_files")
            with open(f'{supporting_files}/bobbibrown_uk_offer.txt') as f:offer_urls = f.readlines()
            for offer_url in offer_urls:
                try:
                    search_item = {}
                    offer_url = offer_url.strip()
                    title = offer_url.split('&search=')[-1]
                    offer_url_search = f"https://www.bobbibrown.co.uk/enrpc/JSONControllerServlet.do?M=host%3Alocalhost%7Cport%3A16580%7Crecs_per_page%3A10000&L=host%3Anjlndca01%7Cport%3A16584&Ntpc=1&Ntpr=1&Nty=1&D={title}&Dx=mode+matchallpartial&Ntt={title}&Ntk=all&Ntx=mode+matchallpartial&Nao=0&Nu=p_PRODUCT_ID&Np=2&N=&Nf=s_searchable%7CGT+0&Nr=AND(NOT(s_INVENTORY_STATUS%3A5)%2Crec_type%3Aproduct%2Clocale%3Aen_GB)&Ne=8061+8062+8127+8050+8063+8053+8089+8051+8095+8096+8147+8052+8054+8157"
                    headers = {
                            'authority': 'www.bobbibrown.co.uk',
                            'accept': '*/*',
                            'accept-language': 'en-US,en;q=0.9',
                            'referer': offer_url,
                            'sec-ch-ua': '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
                            'sec-ch-ua-mobile': '?0',
                            'sec-ch-ua-platform': '"Windows"',
                            'sec-fetch-dest': 'empty',
                            'sec-fetch-mode': 'cors',
                            'sec-fetch-site': 'same-origin',
                            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
                            'x-requested-with': 'XMLHttpRequest'
                            }
                    search_response = await self.request_process(offer_url_search.strip(),headers=headers)
                    product_url_list = []
                    if search_response.json().get('AggrRecords'):
                        for results in search_response.json().get('AggrRecords'):
                            product_url = results.get('Records',{})[0].get('Properties',{}).get('p_url')
                            product_url_list.append(response.urljoin(product_url))    
                        search_item['title'] = title.replace('+',' ')
                        search_item['page_url'] = offer_url
                        search_item['count'] = search_response.json().get('MetaInfo').get('Total Number of Matching Aggregate Records')
                        product_list = []
                        category_name,main_category,sub_category = '','',''
                        for rank, search_product_link in enumerate(product_url_list,1):
                            sub_data = await self.sub_data_extraction(category_name,main_category,sub_category,response,search_product_link,product_list,rank)
                        search_item['products'] = product_list
                        yield search_item
                    else:
                        search_item['title'] = title.replace('+',' ')
                        search_item['page_url'] = offer_url
                        search_item['count'] = 0
                        search_item['products'] = []
                        yield search_item
                except:
                    print(traceback.format_exc())
        except Exception as e:
            print(traceback.format_exc())
            with open('main_error_uk_boobie_text_par.txt','a') as f: f.write(str(e)+'\n')