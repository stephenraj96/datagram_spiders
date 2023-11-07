import os
import scrapy
import urllib.parse
from datetime import datetime

class ElemisUkOffersSpider(scrapy.Spider):
    name = 'elemis_uk_offers'

    start_urls = ['http://uk.elemis.com/']
    
    CURRENT_DATETIME = datetime.now() #today's date
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d") #2023-04-25
    DATE=CURRENT_DATE.replace("-","_") #2023_04_25
    ROTATING_PROXY = os.getenv("ROTATING_PROXY")
    custom_settings={'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/elemis_uk/%(name)s_{DATE}.json": {"format": "json",}},
                     'ROTATING_PROXY_LIST' : [ROTATING_PROXY],
                     'URLLENGTH_LIMIT':5000
                     }
    def start_requests(self):
        dir_path=os.path.abspath(__file__ + "/../../../")
        supporting_files=os.path.join(dir_path,"supporting_files") #reading input file 'Charlotte_tilbury_uk_input_urls.txt' from the directory path of supporting_files
        with open(f'{supporting_files}/elemis_uk_input_urls.txt','r') as f:
            data=f.readlines()

        for url in data:
            yield scrapy.Request(url.strip(),callback=self.parse)
        
    async def parse(self, response):

        if "/search/?query=" in response.url:
            search_url = "https://search.4ed937fd-17ea-48dc-bfe8-688d1759b9b8.prod.systema.cloud/v1/smart-search"
            search_name=response.url.split('?query=')[-1]
            payload = '{"environment":"prod","user_id":{"sid":"16812-34259-6EE9-39F3","fid":"fph0e654c8e0c5ec2c475c19dc033d9c054","uid":""},"display_variants":["attributes","id","brand","currency","description","image","images","in_stock","item_group_id","link","price","promotion","title","sale_price","tags"],"size":300,"start":0,"score":"user","filter":{"text":"'+urllib.parse.unquote(search_name)+'","in_stock_only":false}}'
            headers = {
                'Accept-Encoding':'gzip, deflate, br',
                'authority': 'search.4ed937fd-17ea-48dc-bfe8-688d1759b9b8.prod.systema.cloud',
                'accept': 'application/json, text/plain, */*',
                'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                'cache-control': 'no-cache',
                'content-type': 'application/json;charset=UTF-8',
                'origin': 'https://uk.elemis.com',
                'pragma': 'no-cache',
                'referer': 'https://uk.elemis.com/',
                'sec-ch-ua': '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Linux"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'cross-site',
                'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
                }
            search_request=await self.request_process(search_url,headers=headers,payload=payload)
            search_response=search_request.json()
            item={}
            item['title']=urllib.parse.unquote(response.url.split('/')[-1]).replace('?query=','')
            item['page_url']=response.url
            item['count']=search_response['total']
            item['category_crumb']=[]
            item['products']=[]
            start_value=1
            for count,items in enumerate(search_response['results'],start=start_value):
                products={}
                url_keys=items['link'].split('.com/')[-1].replace('.html','')
                url2=f"https://uk.elemis.com/graphql?query=query%20productDetail(%24urlKey%3AString%2C%24onServer%3ABoolean!)%7BproductDetail%3Aproducts(filter%3A%7Burl_key%3A%7Beq%3A%24urlKey%7D%7D)%7Bitems%7B__typename%20id%20sku%20name%20stock_status%20faq_block%20faq_block_text%20bottle_size%20bottle_size_text%20benefits%20subtitle%20special_price%20url_key%20image%7Burl%20__typename%7Dproduct_badge%20short_description%7Bhtml%20__typename%7Dbv_product_info%7Bbv_product_id%20bv_seo%7Bbv_product_seo_content%20bv_product_seo_summary%20__typename%7D__typename%7Ddescription%7Bhtml%20__typename%7Dkey_ingredients%7Bhtml%20__typename%7Dingredients%20how_to_use%7Bhtml%20__typename%7Drecycling_instructions%7Bhtml%20__typename%7Dvideo_content%7Bhtml%20__typename%7Dbefore_after%7Bhtml%20__typename%7Dupsell_pb%7Bhtml%20__typename%7Drelated_products_pb%7Bhtml%20__typename%7Dworth%20price_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7Dmedia_gallery%7Blabel%20position%20disabled%20url...%20on%20ProductVideo%7Bvideo_content%7Bmedia_type%20video_provider%20video_url%20video_title%20video_metadata%20__typename%7D__typename%7D__typename%7Dcategories%7Bbreadcrumbs%7Bcategory_id%20category_name%20__typename%7D__typename%7D...%20on%20ConfigurableProduct%7Bconfigurable_options%7Battribute_code%20attribute_id%20id%20label%20values%7Bdefault_label%20label%20store_label%20use_default_value%20value_index%20swatch_data%7Btype%20value...%20on%20ImageSwatchData%7Bthumbnail%20__typename%7D__typename%7D__typename%7D__typename%7Dvariants%7Battributes%7Bcode%20value_index%20label%20__typename%7Dproduct%7Bid%20faq_block%20faq_block_text%20price_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7Dmedia_gallery%7Blabel%20position%20disabled%20url...%20on%20ProductVideo%7Bvideo_content%7Bmedia_type%20video_provider%20video_url%20video_title%20video_metadata%20__typename%7D__typename%7D__typename%7Dsku%20stock_status%20product_badge%20__typename%7D__typename%7D__typename%7D...%20on%20GiftCardProduct%7Ballow_open_amount%20open_amount_min%20open_amount_max%20giftcard_type%20is_redeemable%20lifetime%20allow_message%20message_max_length%20giftcard_amounts%7Bvalue_id%20website_id%20website_value%20attribute_id%20value%20__typename%7D__typename%7D...%20on%20BundleProduct%7Bdynamic_sku%20dynamic_price%20dynamic_weight%20price_view%20ship_bundle_items%20exploding_kit%20items%7Boption_id%20title%20required%20type%20position%20sku%20options%7Bid%20quantity%20position%20is_default%20price%20price_type%20can_change_quantity%20label%20product%7Bid%20name%20sku%20stock_status%20price_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7D__typename%7D__typename%7D__typename%7D__typename%7Dmeta_title%40include(if%3A%24onServer)meta_keyword%40include(if%3A%24onServer)meta_description%40include(if%3A%24onServer)canonical_url%7D__typename%7D%7D&operationName=productDetail&variables=%7B%22urlKey%22%3A%22{url_keys}%22%2C%22onServer%22%3Atrue%7D"
                response2=await self.request_process( url2, headers=headers, payload={})
                product_item=response2.json()
                for product in product_item.get('data',{}).get('productDetail',{}).get('items',''):

                    varients=product.get('variants','')
                    
                    if varients:
                        for varient in product['variants']:
                            products_item={}
                            products_item['rank']=count
                            products_item['url']=f"https://uk.elemis.com/{product['canonical_url']}?{varient['attributes'][0]['code']}={varient['attributes'][0]['value_index']}"
                            image_url=[i['url'] for i in varient['product']['media_gallery']]
                            products_item['image_url']=[i for i in image_url if (".png" in i) or (".jpg" in i)]
                            products_item['has_video']=False
                            products_item['video']=[]
                            video=['' if (".png" in i) or (".jpg" in i) else i for i in image_url]
                            video=[i for i in video if i!='']
                            if video!=[]:
                                products_item['video']=video
                                products_item['has_video']=True
                            products_item['master_product_id']=varient['product']['sku']
                            # products_item['variant_id']=varient['product']['id']
                            products_item['name']=str(product['name']+f'_{varient["attributes"][0]["label"]}').strip()
                            products_item['brand']="ELEMIS"
                            products_item['price']=None
                            price=varient['product']['price_range']['minimum_price']['final_price']['value']
                            if price:
                                products_item['price']='£'+str("%.2f" %price)
                            products_item['in-stock']=True
                            in_stock=varient.get('product',{}).get('stock_status','')
                            if in_stock:
                                if in_stock=="OUT_OF_STOCK":
                                    products_item['in-stock']=False
                            
                            products_item['price_before']=None
                            price_before=varient['product']['price_range']['minimum_price']['regular_price']['value']
                            if price_before:
                                products_item['price_before']='£'+str("%.2f" %price_before)
                            products_item['promo_label']=None
                            promo_label=varient['product']['price_range']['minimum_price']['discount']['amount_off']
                            if promo_label>0:
                                products_item['promo_label']=promo_label
                            if products_item['price']==products_item['price_before']:
                                products_item['price_before']=None
                            if products_item['promo_label']==None:
                                products_item['price_before']=None
                            if not str(products_item['promo_label']).__contains__('%'):
                                products_item['promo_label']=None
                            item['products'].append(products_item)
                    else:
                        products['rank']=count
                        products['url']=f"https://uk.elemis.com/{product['canonical_url']}"
                        image_url=[i['url'] for i in product['media_gallery'] ]
                        products['image_url']=[i for i in image_url if (".png" in i) or (".jpg" in i)]
                        products['has_video']=False
                        products['video']=[]
                        video=['' if (".png" in i) or (".jpg" in i) else i for i in image_url]
                        video=[i for i in video if i!='']
                        if video!=[]:
                            products['video']=video
                            products['has_video']=True
                        products['master_product_id']=product['sku']
                        # products['variant_id']=product['id']
                        products['name']=str(product['name']).strip()
                        products['brand']="ELEMIS"
                        products['price']=None
                        price=product['price_range']['minimum_price']['final_price']['value']
                        if price:
                            products['price']='£'+str("%.2f" %price)
                        products['in-stock']=True
                        in_stock=product['stock_status']
                        if in_stock:
                            if in_stock=="OUT_OF_STOCK":
                                products['in-stock']=False
                        products['price_before']=None
                        price_before=product['price_range']['minimum_price']['regular_price']['value']
                        if price_before:
                            products['price_before']='£'+str("%.2f" %price_before)
                        products['promo_label']=None
                        promo_label=product['price_range']['minimum_price']['discount']['amount_off']
                        if promo_label>0:
                            products['promo_label']=promo_label
                        
                        if products['price']==products['price_before']:
                            products['price_before']=None
                        if products['promo_label']==None:
                            products['price_before']=None
                        if not str(products['promo_label']).__contains__('%'):
                            products['promo_label']=None
                        item['products'].append(products)
                    start_value=count+1
            yield item
        else:
            category_url="https://uk.elemis.com/graphql?query=query%20navigationMenu(%24id%3AInt!)%7Bcategory(id%3A%24id)%7B__typename%20id%20name%20children%7B__typename%20children_count%20id%20name%20position%20include_in_menu%20url_path%20url_key%20menu_content%20image%7D%7D%7D&operationName=navigationMenu&variables=%7B%22id%22%3A629%7D"
            headers={'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}
            category_request=await self.request_process(category_url,headers=headers,payload={})
            category_response=category_request.json()
           
            split_url=response.url.split('.com/')[-1]
            id_dict={id['url_path']:id['id'] for id in category_response['data']['category']['children']}
            if len(split_url.split('/'))>1:
                if 'skincare' in response.url:
                    url=f"https://uk.elemis.com/graphql?query=query%20category(%24id%3AString!%2C%24idNum%3AInt!%2C%24pageSize%3AInt!%2C%24currentPage%3AInt!%2C%24onServer%3ABoolean!%2C%24filter%3AProductAttributeFilterInput!%2C%24sort%3AProductAttributeSortInput)%7Bcategory(id%3A%24idNum)%7Bid%20description%20name%20include_in_menu%20url_key%20systema_category_enable%20systema_category_url_key%20product_count%20display_mode%20allowed_tiles%20marketing_tiles%20marketing_tile_1%20marketing_tile_2%20marketing_tile_3%20marketing_tile_4%20marketing_tile_5%20breadcrumbs%7Bcategory_level%20category_name%20category_url_key%20include_in_menu%20__typename%7Dmeta_keywords%40include(if%3A%24onServer)meta_description%40include(if%3A%24onServer)meta_title%40include(if%3A%24onServer)canonical_url%20__typename%7DcategoryList(filters%3A%7Bids%3A%7Bin%3A%5B%24id%5D%7D%7D)%7Bchildren_count%20url_key%20description%20cms_block%7Bcontent%20identifier%20__typename%7Dchildren%7Bid%20level%20name%20path%20url_path%20url_key%20product_count%20image%20children%7Bid%20level%20name%20path%20url_path%20url_key%20__typename%7D__typename%7Dmeta_title%40include(if%3A%24onServer)meta_keywords%40include(if%3A%24onServer)meta_description%40include(if%3A%24onServer)__typename%7Dproducts(pageSize%3A%24pageSize%2CcurrentPage%3A%24currentPage%2Cfilter%3A%24filter%2Csort%3A%24sort)%7Baggregations%7Battribute_code%20count%20label%20options%7Blabel%20value%20count%20__typename%7D__typename%7D__typename%20sort_fields%7Bdefault%20options%7Blabel%20value%20__typename%7D__typename%7Ditems%7B__typename%20id%20name%20subtitle%20bottle_size%20bottle_size_text%20stock_status%20review_details%7Breview_summary%20review_count%20__typename%7Dprice_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20currency%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7Dsku%20exploding_kit%20product_badge%20hover_image%20category_rollover%20small_image%7Burl%20__typename%7Durl_key...%20on%20ConfigurableProduct%7Bconfigurable_options%7Battribute_code%20values%7Bvalue_index%20label%20swatch_data%7Btype%20value...%20on%20ImageSwatchData%7Bthumbnail%20__typename%7D__typename%7D__typename%7D__typename%7Dvariants%7Battributes%7Bcode%20value_index%20label%20__typename%7Dproduct%7Bid%20special_price%20hover_image%20category_rollover%20small_image%7Burl%20__typename%7Dproduct_badge%20price_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7Dsku%20stock_status%20__typename%7D__typename%7D__typename%7D...%20on%20BundleProduct%7Bdynamic_sku%20dynamic_price%20dynamic_weight%20price_view%20ship_bundle_items%20exploding_kit%20items%7Boption_id%20title%20required%20type%20position%20sku%20options%7Bid%20quantity%20position%20is_default%20price%20price_type%20can_change_quantity%20label%20product%7Bid%20name%20sku%20stock_status%20price_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7D__typename%7D__typename%7D__typename%7D__typename%7D%7Dpage_info%7Btotal_pages%20__typename%7Dtotal_count%7D%7D&operationName=category&variables=%7B%22currentPage%22%3A1%2C%22id%22%3A655%2C%22idNum%22%3A655%2C%22pageSize%22%3A200%2C%22filter%22%3A%7B%22category_id%22%3A%7B%22eq%22%3A%22655%22%7D%2C%22exclude_product_ids%22%3A%7B%22in%22%3Afalse%7D%7D%2C%22sort%22%3A%7B%7D%2C%22onServer%22%3Atrue%7D"
                elif 'body-bath' in response.url:
                    url=f"https://uk.elemis.com/graphql?query=query%20category(%24id%3AString!%2C%24idNum%3AInt!%2C%24pageSize%3AInt!%2C%24currentPage%3AInt!%2C%24onServer%3ABoolean!%2C%24filter%3AProductAttributeFilterInput!%2C%24sort%3AProductAttributeSortInput)%7Bcategory(id%3A%24idNum)%7Bid%20description%20name%20include_in_menu%20url_key%20systema_category_enable%20systema_category_url_key%20product_count%20display_mode%20allowed_tiles%20marketing_tiles%20marketing_tile_1%20marketing_tile_2%20marketing_tile_3%20marketing_tile_4%20marketing_tile_5%20breadcrumbs%7Bcategory_level%20category_name%20category_url_key%20include_in_menu%20__typename%7Dmeta_keywords%40include(if%3A%24onServer)meta_description%40include(if%3A%24onServer)meta_title%40include(if%3A%24onServer)canonical_url%20__typename%7DcategoryList(filters%3A%7Bids%3A%7Bin%3A%5B%24id%5D%7D%7D)%7Bchildren_count%20url_key%20description%20cms_block%7Bcontent%20identifier%20__typename%7Dchildren%7Bid%20level%20name%20path%20url_path%20url_key%20product_count%20image%20children%7Bid%20level%20name%20path%20url_path%20url_key%20__typename%7D__typename%7Dmeta_title%40include(if%3A%24onServer)meta_keywords%40include(if%3A%24onServer)meta_description%40include(if%3A%24onServer)__typename%7Dproducts(pageSize%3A%24pageSize%2CcurrentPage%3A%24currentPage%2Cfilter%3A%24filter%2Csort%3A%24sort)%7Baggregations%7Battribute_code%20count%20label%20options%7Blabel%20value%20count%20__typename%7D__typename%7D__typename%20sort_fields%7Bdefault%20options%7Blabel%20value%20__typename%7D__typename%7Ditems%7B__typename%20id%20name%20subtitle%20bottle_size%20bottle_size_text%20stock_status%20review_details%7Breview_summary%20review_count%20__typename%7Dprice_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20currency%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7Dsku%20exploding_kit%20product_badge%20hover_image%20category_rollover%20small_image%7Burl%20__typename%7Durl_key...%20on%20ConfigurableProduct%7Bconfigurable_options%7Battribute_code%20values%7Bvalue_index%20label%20swatch_data%7Btype%20value...%20on%20ImageSwatchData%7Bthumbnail%20__typename%7D__typename%7D__typename%7D__typename%7Dvariants%7Battributes%7Bcode%20value_index%20label%20__typename%7Dproduct%7Bid%20special_price%20hover_image%20category_rollover%20small_image%7Burl%20__typename%7Dproduct_badge%20price_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7Dsku%20stock_status%20__typename%7D__typename%7D__typename%7D...%20on%20BundleProduct%7Bdynamic_sku%20dynamic_price%20dynamic_weight%20price_view%20ship_bundle_items%20exploding_kit%20items%7Boption_id%20title%20required%20type%20position%20sku%20options%7Bid%20quantity%20position%20is_default%20price%20price_type%20can_change_quantity%20label%20product%7Bid%20name%20sku%20stock_status%20price_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7D__typename%7D__typename%7D__typename%7D__typename%7D%7Dpage_info%7Btotal_pages%20__typename%7Dtotal_count%7D%7D&operationName=category&variables=%7B%22currentPage%22%3A1%2C%22id%22%3A729%2C%22idNum%22%3A729%2C%22pageSize%22%3A200%2C%22filter%22%3A%7B%22category_id%22%3A%7B%22eq%22%3A%22729%22%7D%2C%22exclude_product_ids%22%3A%7B%22in%22%3Afalse%7D%7D%2C%22sort%22%3A%7B%7D%2C%22onServer%22%3Atrue%7D"
                if response.url.split('/')[-1] not in ['skincare','body-bath']:
                    category_check_request=await self.request_process(url,headers=headers,payload={})
                    category_check_response=category_check_request.json()
                    check_data=category_check_response['data']['categoryList'][0]['children']
                    id_value=[j for i in check_data for j in i['children'] if j['url_path']==(response.url.split('.com/')[-1])]
                    overall_id_value=729
                    if 'skincare' in response.url:
                        overall_id_value=655                    
                    url=f"https://uk.elemis.com/graphql?query=query%20category(%24id%3AString!%2C%24idNum%3AInt!%2C%24pageSize%3AInt!%2C%24currentPage%3AInt!%2C%24onServer%3ABoolean!%2C%24filter%3AProductAttributeFilterInput!%2C%24sort%3AProductAttributeSortInput)%7Bcategory(id%3A%24idNum)%7Bid%20description%20name%20include_in_menu%20url_key%20systema_category_enable%20systema_category_url_key%20product_count%20display_mode%20allowed_tiles%20marketing_tiles%20marketing_tile_1%20marketing_tile_2%20marketing_tile_3%20marketing_tile_4%20marketing_tile_5%20breadcrumbs%7Bcategory_level%20category_name%20category_url_key%20include_in_menu%20__typename%7Dmeta_keywords%40include(if%3A%24onServer)meta_description%40include(if%3A%24onServer)meta_title%40include(if%3A%24onServer)canonical_url%20__typename%7DcategoryList(filters%3A%7Bids%3A%7Bin%3A%5B%24id%5D%7D%7D)%7Bchildren_count%20url_key%20description%20cms_block%7Bcontent%20identifier%20__typename%7Dchildren%7Bid%20level%20name%20path%20url_path%20url_key%20product_count%20image%20children%7Bid%20level%20name%20path%20url_path%20url_key%20__typename%7D__typename%7Dmeta_title%40include(if%3A%24onServer)meta_keywords%40include(if%3A%24onServer)meta_description%40include(if%3A%24onServer)__typename%7Dproducts(pageSize%3A%24pageSize%2CcurrentPage%3A%24currentPage%2Cfilter%3A%24filter%2Csort%3A%24sort)%7Baggregations%7Battribute_code%20count%20label%20options%7Blabel%20value%20count%20__typename%7D__typename%7D__typename%20sort_fields%7Bdefault%20options%7Blabel%20value%20__typename%7D__typename%7Ditems%7B__typename%20id%20name%20subtitle%20bottle_size%20bottle_size_text%20stock_status%20review_details%7Breview_summary%20review_count%20__typename%7Dprice_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20currency%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7Dsku%20exploding_kit%20product_badge%20hover_image%20category_rollover%20small_image%7Burl%20__typename%7Durl_key...%20on%20ConfigurableProduct%7Bconfigurable_options%7Battribute_code%20values%7Bvalue_index%20label%20swatch_data%7Btype%20value...%20on%20ImageSwatchData%7Bthumbnail%20__typename%7D__typename%7D__typename%7D__typename%7Dvariants%7Battributes%7Bcode%20value_index%20label%20__typename%7Dproduct%7Bid%20special_price%20hover_image%20category_rollover%20small_image%7Burl%20__typename%7Dproduct_badge%20price_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7Dsku%20stock_status%20__typename%7D__typename%7D__typename%7D...%20on%20BundleProduct%7Bdynamic_sku%20dynamic_price%20dynamic_weight%20price_view%20ship_bundle_items%20exploding_kit%20items%7Boption_id%20title%20required%20type%20position%20sku%20options%7Bid%20quantity%20position%20is_default%20price%20price_type%20can_change_quantity%20label%20product%7Bid%20name%20sku%20stock_status%20price_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7D__typename%7D__typename%7D__typename%7D__typename%7D%7Dpage_info%7Btotal_pages%20__typename%7Dtotal_count%7D%7D&operationName=category&variables=%7B%22currentPage%22%3A1%2C%22id%22%3A{overall_id_value}%2C%22idNum%22%3A{id_value[0]['id']}%2C%22pageSize%22%3A200%2C%22filter%22%3A%7B%22category_id%22%3A%7B%22eq%22%3A%22{id_value[0]['id']}%22%7D%2C%22exclude_product_ids%22%3A%7B%22in%22%3Afalse%7D%7D%2C%22sort%22%3A%7B%7D%2C%22onServer%22%3Atrue%7D" 
                    yield scrapy.Request(url,callback=self.parse_details,cb_kwargs={'source_url':response.url,'id_value':id_value[0]['id']})
            else:
                if id_dict[split_url]:
                    print(bool(id_dict[split_url]))
                    url=f"https://uk.elemis.com/graphql?query=query%20category(%24id%3AString!%2C%24idNum%3AInt!%2C%24pageSize%3AInt!%2C%24currentPage%3AInt!%2C%24onServer%3ABoolean!%2C%24filter%3AProductAttributeFilterInput!%2C%24sort%3AProductAttributeSortInput)%7Bcategory(id%3A%24idNum)%7Bid%20description%20name%20include_in_menu%20url_key%20systema_category_enable%20systema_category_url_key%20product_count%20display_mode%20allowed_tiles%20marketing_tiles%20marketing_tile_1%20marketing_tile_2%20marketing_tile_3%20marketing_tile_4%20marketing_tile_5%20breadcrumbs%7Bcategory_level%20category_name%20category_url_key%20include_in_menu%20__typename%7Dmeta_keywords%40include(if%3A%24onServer)meta_description%40include(if%3A%24onServer)meta_title%40include(if%3A%24onServer)canonical_url%20__typename%7DcategoryList(filters%3A%7Bids%3A%7Bin%3A%5B%24id%5D%7D%7D)%7Bchildren_count%20url_key%20description%20cms_block%7Bcontent%20identifier%20__typename%7Dchildren%7Bid%20level%20name%20path%20url_path%20url_key%20product_count%20image%20children%7Bid%20level%20name%20path%20url_path%20url_key%20__typename%7D__typename%7Dmeta_title%40include(if%3A%24onServer)meta_keywords%40include(if%3A%24onServer)meta_description%40include(if%3A%24onServer)__typename%7Dproducts(pageSize%3A%24pageSize%2CcurrentPage%3A%24currentPage%2Cfilter%3A%24filter%2Csort%3A%24sort)%7Baggregations%7Battribute_code%20count%20label%20options%7Blabel%20value%20count%20__typename%7D__typename%7D__typename%20sort_fields%7Bdefault%20options%7Blabel%20value%20__typename%7D__typename%7Ditems%7B__typename%20id%20name%20subtitle%20bottle_size%20bottle_size_text%20stock_status%20review_details%7Breview_summary%20review_count%20__typename%7Dprice_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20currency%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7Dsku%20exploding_kit%20product_badge%20hover_image%20category_rollover%20small_image%7Burl%20__typename%7Durl_key...%20on%20ConfigurableProduct%7Bconfigurable_options%7Battribute_code%20values%7Bvalue_index%20label%20swatch_data%7Btype%20value...%20on%20ImageSwatchData%7Bthumbnail%20__typename%7D__typename%7D__typename%7D__typename%7Dvariants%7Battributes%7Bcode%20value_index%20label%20__typename%7Dproduct%7Bid%20special_price%20hover_image%20category_rollover%20small_image%7Burl%20__typename%7Dproduct_badge%20price_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7Dsku%20stock_status%20__typename%7D__typename%7D__typename%7D...%20on%20BundleProduct%7Bdynamic_sku%20dynamic_price%20dynamic_weight%20price_view%20ship_bundle_items%20exploding_kit%20items%7Boption_id%20title%20required%20type%20position%20sku%20options%7Bid%20quantity%20position%20is_default%20price%20price_type%20can_change_quantity%20label%20product%7Bid%20name%20sku%20stock_status%20price_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7D__typename%7D__typename%7D__typename%7D__typename%7D%7Dpage_info%7Btotal_pages%20__typename%7Dtotal_count%7D%7D&operationName=category&variables=%7B%22currentPage%22%3A1%2C%22id%22%3A{id_dict[split_url]}%2C%22idNum%22%3A{id_dict[split_url]}%2C%22pageSize%22%3A200%2C%22filter%22%3A%7B%22category_id%22%3A%7B%22eq%22%3A%22{id_dict[split_url]}%22%7D%2C%22exclude_product_ids%22%3A%7B%22in%22%3Afalse%7D%7D%2C%22sort%22%3A%7B%7D%2C%22onServer%22%3Atrue%7D" 
                    yield scrapy.Request(url,callback=self.parse_details,cb_kwargs={'source_url':response.url,'id_value':id_dict[split_url]})

    async def parse_details(self,response,source_url,id_value):
    
        headers = {
        'Accept-Encoding':'gzip, deflate, br',
        'sec-ch-ua': '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
        'sec-ch-ua-mobile': '?0',
        'Authorization': '',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
        'content-type': 'application/json',
        'accept': '*/*',
        'Referer': 'https://uk.elemis.com/skincare',
        'Store': 'uk',
        'Content-Currency': 'GBP',
        'sec-ch-ua-platform': '"Linux"'
        }
        value=response.json()
        total_pages=value['data']['products']['page_info']['total_pages']
        item={}
        item['title']=source_url.split('/')[-1]
        item['page_url']=source_url
        item['count']=value['data']['products']['total_count']
        item['category_crumb']=[]
        
        split_url=source_url.split('.com/')[-1]
        if split_url in ['skincare','body-bath']:
            category_crumb={}
            category_crumb['name']=split_url.split('/')[-1]
            category_crumb['url']='https://uk.elemis.com/'+split_url
            item['category_crumb'].append(category_crumb)
        elif len(split_url.split('/'))>1:
       
            id_values=[j['url_path'] for i in value['data']['categoryList'][0]['children'] for j in i['children'] if j['url_path']==(source_url.split('.com/')[-1])]
         
            first_value=id_values[0].split('/')[0]
           
            category_crumb={}
            category_crumb['name']=first_value.split('/')[-1]
            category_crumb['url']='https://uk.elemis.com/'+first_value
            item['category_crumb'].append(category_crumb)
            for i in id_values:
                inner_category_crumb={}
                inner_category_crumb['name']=i.split('/')[-1]
                inner_category_crumb['url']='https://uk.elemis.com/'+i
                item['category_crumb'].append(inner_category_crumb)
            

    
        item['products']=[]
        start_value=1
        for page in range(1,int(total_pages)+1):
            url1 = f"https://uk.elemis.com/graphql?query=query%20category(%24id%3AString!%2C%24idNum%3AInt!%2C%24pageSize%3AInt!%2C%24currentPage%3AInt!%2C%24onServer%3ABoolean!%2C%24filter%3AProductAttributeFilterInput!%2C%24sort%3AProductAttributeSortInput)%7Bcategory(id%3A%24idNum)%7Bid%20description%20name%20include_in_menu%20url_key%20systema_category_enable%20systema_category_url_key%20product_count%20display_mode%20allowed_tiles%20marketing_tiles%20marketing_tile_1%20marketing_tile_2%20marketing_tile_3%20marketing_tile_4%20marketing_tile_5%20breadcrumbs%7Bcategory_level%20category_name%20category_url_key%20include_in_menu%20__typename%7Dmeta_keywords%40include(if%3A%24onServer)meta_description%40include(if%3A%24onServer)meta_title%40include(if%3A%24onServer)canonical_url%20__typename%7DcategoryList(filters%3A%7Bids%3A%7Bin%3A%5B%24id%5D%7D%7D)%7Bchildren_count%20url_key%20description%20cms_block%7Bcontent%20identifier%20__typename%7Dchildren%7Bid%20level%20name%20path%20url_path%20url_key%20product_count%20image%20children%7Bid%20level%20name%20path%20url_path%20url_key%20__typename%7D__typename%7Dmeta_title%40include(if%3A%24onServer)meta_keywords%40include(if%3A%24onServer)meta_description%40include(if%3A%24onServer)__typename%7Dproducts(pageSize%3A%24pageSize%2CcurrentPage%3A%24currentPage%2Cfilter%3A%24filter%2Csort%3A%24sort)%7Baggregations%7Battribute_code%20count%20label%20options%7Blabel%20value%20count%20__typename%7D__typename%7D__typename%20sort_fields%7Bdefault%20options%7Blabel%20value%20__typename%7D__typename%7Ditems%7B__typename%20id%20name%20subtitle%20bottle_size%20bottle_size_text%20stock_status%20review_details%7Breview_summary%20review_count%20__typename%7Dprice_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20currency%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7Dsku%20exploding_kit%20product_badge%20hover_image%20category_rollover%20small_image%7Burl%20__typename%7Durl_key...%20on%20ConfigurableProduct%7Bconfigurable_options%7Battribute_code%20values%7Bvalue_index%20label%20swatch_data%7Btype%20value...%20on%20ImageSwatchData%7Bthumbnail%20__typename%7D__typename%7D__typename%7D__typename%7Dvariants%7Battributes%7Bcode%20value_index%20label%20__typename%7Dproduct%7Bid%20special_price%20hover_image%20category_rollover%20small_image%7Burl%20__typename%7Dproduct_badge%20price_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7Dsku%20stock_status%20__typename%7D__typename%7D__typename%7D...%20on%20BundleProduct%7Bdynamic_sku%20dynamic_price%20dynamic_weight%20price_view%20ship_bundle_items%20exploding_kit%20items%7Boption_id%20title%20required%20type%20position%20sku%20options%7Bid%20quantity%20position%20is_default%20price%20price_type%20can_change_quantity%20label%20product%7Bid%20name%20sku%20stock_status%20price_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7D__typename%7D__typename%7D__typename%7D__typename%7D%7Dpage_info%7Btotal_pages%20__typename%7Dtotal_count%7D%7D&operationName=category&variables=%7B%22currentPage%22%3A{page}%2C%22id%22%3A{id_value}%2C%22idNum%22%3A{id_value}%2C%22pageSize%22%3A200%2C%22filter%22%3A%7B%22category_id%22%3A%7B%22eq%22%3A%22{id_value}%22%7D%2C%22exclude_product_ids%22%3A%7B%22in%22%3Afalse%7D%7D%2C%22sort%22%3A%7B%7D%2C%22onServer%22%3Atrue%7D"
            response1 = await self.request_process(url1, headers=headers, payload={})
            product_value=response1.json()
            pdp_values=product_value.get('data',{}).get('products',{}).get('items','')
            if pdp_values:
                for count,items in enumerate(product_value['data']['products']['items'],start=start_value):
            
                    products={}
                 
                    url2=f"https://uk.elemis.com/graphql?query=query%20productDetail(%24urlKey%3AString%2C%24onServer%3ABoolean!)%7BproductDetail%3Aproducts(filter%3A%7Burl_key%3A%7Beq%3A%24urlKey%7D%7D)%7Bitems%7B__typename%20id%20sku%20name%20stock_status%20faq_block%20faq_block_text%20bottle_size%20bottle_size_text%20benefits%20subtitle%20special_price%20url_key%20image%7Burl%20__typename%7Dproduct_badge%20short_description%7Bhtml%20__typename%7Dbv_product_info%7Bbv_product_id%20bv_seo%7Bbv_product_seo_content%20bv_product_seo_summary%20__typename%7D__typename%7Ddescription%7Bhtml%20__typename%7Dkey_ingredients%7Bhtml%20__typename%7Dingredients%20how_to_use%7Bhtml%20__typename%7Drecycling_instructions%7Bhtml%20__typename%7Dvideo_content%7Bhtml%20__typename%7Dbefore_after%7Bhtml%20__typename%7Dupsell_pb%7Bhtml%20__typename%7Drelated_products_pb%7Bhtml%20__typename%7Dworth%20price_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7Dmedia_gallery%7Blabel%20position%20disabled%20url...%20on%20ProductVideo%7Bvideo_content%7Bmedia_type%20video_provider%20video_url%20video_title%20video_metadata%20__typename%7D__typename%7D__typename%7Dcategories%7Bbreadcrumbs%7Bcategory_id%20category_name%20__typename%7D__typename%7D...%20on%20ConfigurableProduct%7Bconfigurable_options%7Battribute_code%20attribute_id%20id%20label%20values%7Bdefault_label%20label%20store_label%20use_default_value%20value_index%20swatch_data%7Btype%20value...%20on%20ImageSwatchData%7Bthumbnail%20__typename%7D__typename%7D__typename%7D__typename%7Dvariants%7Battributes%7Bcode%20value_index%20label%20__typename%7Dproduct%7Bid%20faq_block%20faq_block_text%20price_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7Dmedia_gallery%7Blabel%20position%20disabled%20url...%20on%20ProductVideo%7Bvideo_content%7Bmedia_type%20video_provider%20video_url%20video_title%20video_metadata%20__typename%7D__typename%7D__typename%7Dsku%20stock_status%20product_badge%20__typename%7D__typename%7D__typename%7D...%20on%20GiftCardProduct%7Ballow_open_amount%20open_amount_min%20open_amount_max%20giftcard_type%20is_redeemable%20lifetime%20allow_message%20message_max_length%20giftcard_amounts%7Bvalue_id%20website_id%20website_value%20attribute_id%20value%20__typename%7D__typename%7D...%20on%20BundleProduct%7Bdynamic_sku%20dynamic_price%20dynamic_weight%20price_view%20ship_bundle_items%20exploding_kit%20items%7Boption_id%20title%20required%20type%20position%20sku%20options%7Bid%20quantity%20position%20is_default%20price%20price_type%20can_change_quantity%20label%20product%7Bid%20name%20sku%20stock_status%20price_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7D__typename%7D__typename%7D__typename%7D__typename%7Dmeta_title%40include(if%3A%24onServer)meta_keyword%40include(if%3A%24onServer)meta_description%40include(if%3A%24onServer)canonical_url%7D__typename%7D%7D&operationName=productDetail&variables=%7B%22urlKey%22%3A%22{items['url_key']}%22%2C%22onServer%22%3Atrue%7D"
                    response2=await self.request_process( url2, headers=headers, payload={})
                    product_item=response2.json()
                    
                    for product in product_item.get('data',{}).get('productDetail',{}).get('items',[]):
                        varients=product.get('variants','')
                        if varients:
                            for varient in product['variants']:
                            
                                products_item={}
                                products_item['rank']=count
                                products_item['url']=f"https://uk.elemis.com/{product['canonical_url']}?{varient['attributes'][0]['code']}={varient['attributes'][0]['value_index']}"
                                image_url=[i['url'] for i in varient['product']['media_gallery']]
                                products_item['image_url']=[i for i in image_url if (".png" in i) or (".jpg" in i)]
                                products_item['has_video']=False
                                products_item['video']=[]
                                video=['' if (".png" in i) or (".jpg" in i) else i for i in image_url]
                                video=[i for i in video if i!='']
                                if video!=[]:
                                    products_item['video']=video
                                    products_item['has_video']=True
                                products_item['master_product_id']=varient['product']['sku']
                                # products_item['variant_id']=varient['product']['id']
                                products_item['name']=str(product['name']+f'_{varient["attributes"][0]["label"]}').strip()
                                products_item['brand']="ELEMIS"
                                products_item['price']=None
                                price=varient['product']['price_range']['minimum_price']['final_price']['value']
                                if price:
                                    products_item['price']='£'+str("%.2f" %price)
                            
                                products_item['in-stock']=True
                                in_stock=varient.get('product',{}).get('stock_status','')
                                if in_stock:
                                    if in_stock=="OUT_OF_STOCK":
                                        products_item['in-stock']=False
                                
                                products_item['price_before']=None
                                price_before=varient['product']['price_range']['minimum_price']['regular_price']['value']
                                if price_before:
                                    products_item['price_before']='£'+str("%.2f" %price_before)
                                products_item['promo_label']=None
                                promo_label=varient['product']['price_range']['minimum_price']['discount']['amount_off']
                                if promo_label>0:
                                    products_item['promo_label']=promo_label
                            
                                if products_item['price']==products_item['price_before']:
                                    products_item['price_before']=None
                                if products_item['promo_label']==None:
                                    products_item['price_before']=None
                                if not str(products_item['promo_label']).__contains__('%'):
                                    products_item['promo_label']=None
                                item['products'].append(products_item)
                        else:
                            products['rank']=count
                            products['url']=f"https://uk.elemis.com/{product['canonical_url']}"
                            image_url=[i['url'] for i in product['media_gallery'] ]
                            products['image_url']=[i for i in image_url if (".png" in i) or (".jpg" in i)]
                            products['has_video']=False
                            products['video']=[]
                            video=['' if (".png" in i) or (".jpg" in i) else i for i in image_url]
                            video=[i for i in video if i!='']
                            if video!=[]:
                                products['video']=video
                                products['has_video']=True
                            products['master_product_id']=product['sku']
                            # products['variant_id']=product['id']
                            products['name']=str(product['name']).strip()
                            products['brand']="ELEMIS"
                            products['price']=None
                            price=product['price_range']['minimum_price']['final_price']['value']
                            if price:
                                products['price']='£'+str("%.2f" %price)
                    
                            products['in-stock']=True
                            in_stock=product['stock_status']
                            if in_stock:
                                if in_stock=="OUT_OF_STOCK":
                                    products['in-stock']=False
                            products['price_before']=None
                            price_before=product['price_range']['minimum_price']['regular_price']['value']
                            if price_before:
                                products['price_before']='£'+str("%.2f" %price_before)
                            products['promo_label']=None
                            promo_label=product['price_range']['minimum_price']['discount']['amount_off']
                            if promo_label>0:
                                products['promo_label']=promo_label
                            if products['price']==products['price_before']:
                                products['price_before']=None
                            if products['promo_label']==None:
                                products['price_before']=None
                            if not str(products['promo_label']).__contains__('%'):
                                products['promo_label']=None
                            item['products'].append(products)
                        start_value=count+1
            yield item

    async def request_process(self,url,headers,payload):
        if payload=={}:
            request=scrapy.Request(url)
        else:
            request=scrapy.Request(url,method='POST',headers=headers,body=payload)
        response = await self.crawler.engine.download(request, self)
       
        return response


