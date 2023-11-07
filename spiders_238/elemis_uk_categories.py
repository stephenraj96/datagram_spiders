import os
import scrapy
from datetime import datetime

class ElemisUkCategoriesSpider(scrapy.Spider):
    name = 'elemis_uk_categories'
    start_urls = ['https://uk.elemis.com/skincare']
    CURRENT_DATETIME = datetime.now() #today's date
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d") #2023-04-25
    DATE=CURRENT_DATE.replace("-","_") #2023_04_25
    ROTATING_PROXY = os.getenv("ROTATING_PROXY")
    custom_settings={'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/elemis_uk/%(name)s_{DATE}.json": {"format": "json",}},
                     'ROTATING_PROXY_LIST' : [ROTATING_PROXY]
                     }
    async def parse(self, response):
        urls=["https://uk.elemis.com/graphql?query=query%20category(%24id%3AString!%2C%24idNum%3AInt!%2C%24pageSize%3AInt!%2C%24currentPage%3AInt!%2C%24onServer%3ABoolean!%2C%24filter%3AProductAttributeFilterInput!%2C%24sort%3AProductAttributeSortInput)%7Bcategory(id%3A%24idNum)%7Bid%20description%20name%20include_in_menu%20url_key%20systema_category_enable%20systema_category_url_key%20product_count%20display_mode%20allowed_tiles%20marketing_tiles%20marketing_tile_1%20marketing_tile_2%20marketing_tile_3%20marketing_tile_4%20marketing_tile_5%20breadcrumbs%7Bcategory_level%20category_name%20category_url_key%20include_in_menu%20__typename%7Dmeta_keywords%40include(if%3A%24onServer)meta_description%40include(if%3A%24onServer)meta_title%40include(if%3A%24onServer)canonical_url%20__typename%7DcategoryList(filters%3A%7Bids%3A%7Bin%3A%5B%24id%5D%7D%7D)%7Bchildren_count%20url_key%20description%20cms_block%7Bcontent%20identifier%20__typename%7Dchildren%7Bid%20level%20name%20path%20url_path%20url_key%20product_count%20image%20children%7Bid%20level%20name%20path%20url_path%20url_key%20__typename%7D__typename%7Dmeta_title%40include(if%3A%24onServer)meta_keywords%40include(if%3A%24onServer)meta_description%40include(if%3A%24onServer)__typename%7Dproducts(pageSize%3A%24pageSize%2CcurrentPage%3A%24currentPage%2Cfilter%3A%24filter%2Csort%3A%24sort)%7Baggregations%7Battribute_code%20count%20label%20options%7Blabel%20value%20count%20__typename%7D__typename%7D__typename%20sort_fields%7Bdefault%20options%7Blabel%20value%20__typename%7D__typename%7Ditems%7B__typename%20id%20name%20subtitle%20bottle_size%20bottle_size_text%20stock_status%20review_details%7Breview_summary%20review_count%20__typename%7Dprice_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20currency%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7Dsku%20exploding_kit%20product_badge%20hover_image%20category_rollover%20small_image%7Burl%20__typename%7Durl_key...%20on%20ConfigurableProduct%7Bconfigurable_options%7Battribute_code%20values%7Bvalue_index%20label%20swatch_data%7Btype%20value...%20on%20ImageSwatchData%7Bthumbnail%20__typename%7D__typename%7D__typename%7D__typename%7Dvariants%7Battributes%7Bcode%20value_index%20label%20__typename%7Dproduct%7Bid%20special_price%20hover_image%20category_rollover%20small_image%7Burl%20__typename%7Dproduct_badge%20price_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7Dsku%20stock_status%20__typename%7D__typename%7D__typename%7D...%20on%20BundleProduct%7Bdynamic_sku%20dynamic_price%20dynamic_weight%20price_view%20ship_bundle_items%20exploding_kit%20items%7Boption_id%20title%20required%20type%20position%20sku%20options%7Bid%20quantity%20position%20is_default%20price%20price_type%20can_change_quantity%20label%20product%7Bid%20name%20sku%20stock_status%20price_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7D__typename%7D__typename%7D__typename%7D__typename%7D%7Dpage_info%7Btotal_pages%20__typename%7Dtotal_count%7D%7D&operationName=category&variables=%7B%22currentPage%22%3A1%2C%22id%22%3A655%2C%22idNum%22%3A655%2C%22pageSize%22%3A30%2C%22filter%22%3A%7B%22category_id%22%3A%7B%22eq%22%3A%22655%22%7D%2C%22exclude_product_ids%22%3A%7B%22in%22%3Afalse%7D%7D%2C%22sort%22%3A%7B%7D%2C%22onServer%22%3Atrue%7D","https://uk.elemis.com/graphql?query=query%20category(%24id%3AString!%2C%24idNum%3AInt!%2C%24pageSize%3AInt!%2C%24currentPage%3AInt!%2C%24onServer%3ABoolean!%2C%24filter%3AProductAttributeFilterInput!%2C%24sort%3AProductAttributeSortInput)%7Bcategory(id%3A%24idNum)%7Bid%20description%20name%20include_in_menu%20url_key%20systema_category_enable%20systema_category_url_key%20product_count%20display_mode%20allowed_tiles%20marketing_tiles%20marketing_tile_1%20marketing_tile_2%20marketing_tile_3%20marketing_tile_4%20marketing_tile_5%20breadcrumbs%7Bcategory_level%20category_name%20category_url_key%20include_in_menu%20__typename%7Dmeta_keywords%40include(if%3A%24onServer)meta_description%40include(if%3A%24onServer)meta_title%40include(if%3A%24onServer)canonical_url%20__typename%7DcategoryList(filters%3A%7Bids%3A%7Bin%3A%5B%24id%5D%7D%7D)%7Bchildren_count%20url_key%20description%20cms_block%7Bcontent%20identifier%20__typename%7Dchildren%7Bid%20level%20name%20path%20url_path%20url_key%20product_count%20image%20children%7Bid%20level%20name%20path%20url_path%20url_key%20__typename%7D__typename%7Dmeta_title%40include(if%3A%24onServer)meta_keywords%40include(if%3A%24onServer)meta_description%40include(if%3A%24onServer)__typename%7Dproducts(pageSize%3A%24pageSize%2CcurrentPage%3A%24currentPage%2Cfilter%3A%24filter%2Csort%3A%24sort)%7Baggregations%7Battribute_code%20count%20label%20options%7Blabel%20value%20count%20__typename%7D__typename%7D__typename%20sort_fields%7Bdefault%20options%7Blabel%20value%20__typename%7D__typename%7Ditems%7B__typename%20id%20name%20subtitle%20bottle_size%20bottle_size_text%20stock_status%20review_details%7Breview_summary%20review_count%20__typename%7Dprice_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20currency%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7Dsku%20exploding_kit%20product_badge%20hover_image%20category_rollover%20small_image%7Burl%20__typename%7Durl_key...%20on%20ConfigurableProduct%7Bconfigurable_options%7Battribute_code%20values%7Bvalue_index%20label%20swatch_data%7Btype%20value...%20on%20ImageSwatchData%7Bthumbnail%20__typename%7D__typename%7D__typename%7D__typename%7Dvariants%7Battributes%7Bcode%20value_index%20label%20__typename%7Dproduct%7Bid%20special_price%20hover_image%20category_rollover%20small_image%7Burl%20__typename%7Dproduct_badge%20price_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7Dsku%20stock_status%20__typename%7D__typename%7D__typename%7D...%20on%20BundleProduct%7Bdynamic_sku%20dynamic_price%20dynamic_weight%20price_view%20ship_bundle_items%20exploding_kit%20items%7Boption_id%20title%20required%20type%20position%20sku%20options%7Bid%20quantity%20position%20is_default%20price%20price_type%20can_change_quantity%20label%20product%7Bid%20name%20sku%20stock_status%20price_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7D__typename%7D__typename%7D__typename%7D__typename%7D%7Dpage_info%7Btotal_pages%20__typename%7Dtotal_count%7D%7D&operationName=category&variables=%7B%22currentPage%22%3A1%2C%22id%22%3A729%2C%22idNum%22%3A729%2C%22pageSize%22%3A30%2C%22filter%22%3A%7B%22category_id%22%3A%7B%22eq%22%3A%22729%22%7D%2C%22exclude_product_ids%22%3A%7B%22in%22%3Afalse%7D%7D%2C%22sort%22%3A%7B%7D%2C%22onServer%22%3Atrue%7D"]
        for url in urls:
            headers={'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'}
            cat_req=await self.request_process(url,headers=headers,payload={})
            value=cat_req.json()
            categoryList=value['data']['categoryList'][0]['children']
            item={'name':value['data']['categoryList'][0]['url_key'],
                'url':f"https://uk.elemis.com/{value['data']['categoryList'][0]['url_key']}",
                'category_crumb':[]
                }
            for block in categoryList:
                for cat in block['children']:
                    cat_item={}
                    cat_item['name']=cat['url_path'].split('/')[-1]
                    cat_item['url']=f"https://uk.elemis.com/{cat['url_path']}"
                    cat_item['category_crumb']=[]
                    if cat_item['name']=='best-sellers':
                        continue
                    else:
                        item['category_crumb'].append(cat_item)
            yield item
            
        

    async def request_process(self,url,headers,payload):
        if payload=={}:
            request=scrapy.Request(url)
        else:
            request=scrapy.Request(url,method='POST',headers=headers,body=payload)
        response = await self.crawler.engine.download(request, self)
        return response
