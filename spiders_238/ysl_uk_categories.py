import scrapy
import os
from dotenv import load_dotenv
from pathlib import Path
import datetime

""" load env file """
try:
    load_dotenv()
except:
    env_path = Path(".env")
    load_dotenv(dotenv_path=env_path)

class YslukcategorySpider(scrapy.Spider):
    name = "ysl_uk_categories"
    
    """ Get token in env file"""
    api_token=os.getenv("api_token")

    """ save file in s3"""
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    custom_settings={
        'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/ysk_uk/%(name)s_{DATE}.json": {"format": "json",}
        }
    }

    def start_requests(self):
        """Intial main url request"""
        start_url=f"https://api.scrape.do/?token={self.api_token}&url=https://www.yslbeauty.co.uk/"
        yield scrapy.Request(start_url,callback=self.category)

    def category(self,response):
        """ Get all categories of products """
        #makeup
        makekup_item = {}
        makekup_item['name'] = "Makeup"
        makekup_item['url'] = "https://www.yslbeauty.co.uk/makeup"
        
        make_category_name_url_dict={}
        for category_name in response.css('[data-category="makeup"] .c-navigation__item.m-level-2 [role="heading"]'):
            name=category_name.css('a::text').get().strip().lower()
            url=category_name.css('a::attr(href)').get().strip()
            make_category_name_url_dict[name]=url
        
        makeup_category_crumb={}
        make_category_crumb_list=[]
        for category in response.css('[data-category="makeup"] .c-navigation__item.m-level-2  .c-navigation__list.m-level-3 li a'):
            product_name=category.css('a::text').get().strip()
            product_url=category.css('a::attr(href)').get().strip()
            
            if product_name!="View all":
                sub_category_name=product_url.replace("https://www.yslbeauty.co.uk/makeup/",'').split("/")[0]
                if product_name=="Lip Liner":
                    sub_category_name="lips"
                if product_name=="Eye Makeup Remover":
                    sub_category_name="eyes"
                
                product={}
                product['name']=product_name
                product['url']=product_url
                
                if sub_category_name in makeup_category_crumb.keys():
                    makeup_category_crumb[sub_category_name].append(product)
                else:
                    makeup_category_crumb[sub_category_name]=[product]

        for sub_categories_keys in make_category_name_url_dict.keys():
            sub_categories_dict={}
            sub_categories_dict["name"]=sub_categories_keys
            sub_categories_dict["url"]=make_category_name_url_dict.get(sub_categories_keys)
            sub_categories_dict["category_crumb"]=makeup_category_crumb.get(sub_categories_keys)
            make_category_crumb_list.append(sub_categories_dict)

        makekup_item["category_crumb"]=make_category_crumb_list
        yield makekup_item
        
        #fragrance
        fragrance_item = {}
        fragrance_item['name'] = "Fragrance"
        fragrance_item['url'] = "https://www.yslbeauty.co.uk/fragrance"
        
        fragrance_category_name_url_dict={}
        for category_name in response.css('[data-category="fragrance"] .c-navigation__item.m-level-2 [role="heading"]'):
            name=category_name.css('a::text').get().strip().lower().replace(" ","-")
            url=category_name.css('a::attr(href)').get().strip()
            fragrance_category_name_url_dict[name]=url

        fragrance_category_crumb={}
        fragrance_category_crumb_list=[]
        for category in response.css('[data-category="fragrance"] .c-navigation__item.m-level-2  .c-navigation__list.m-level-3 li a'):
            product_name=category.css('a::text').get().strip()
            product_url=category.css('a::attr(href)').get().strip()
            
            if product_name!="View all":
                sub_category_name=product_url.replace("https://www.yslbeauty.co.uk/fragrance/",'').split("/")[0]
                if product_name=="Cinema":
                    sub_category_name="for-her"
                if product_name=="In Love Again":
                    sub_category_name="for-her"
                if product_name=="Opium Pour Homme":
                    sub_category_name="fragrances-for-him"
                if product_name=="The Signature Collection":
                    sub_category_name="le-vestiaire-fragrances"
                if product_name=="The Rêvée Collection":
                    sub_category_name="le-vestiaire-fragrances"
                if product_name=="The Couture Collection":
                    sub_category_name="le-vestiaire-fragrances"
                if product_name=="Discover the Collection":
                    sub_category_name="le-vestiaire-fragrances"

                product={}
                product['name']=product_name
                product['url']=product_url
                if sub_category_name in fragrance_category_crumb.keys():
                    fragrance_category_crumb[sub_category_name].append(product)
                else:
                    fragrance_category_crumb[sub_category_name]=[product]
        
        for sub_categories_keys in fragrance_category_name_url_dict.keys():
            sub_categories_dict={}
            sub_categories_dict["name"]=sub_categories_keys
            sub_categories_dict["url"]=fragrance_category_name_url_dict.get(sub_categories_keys)
            sub_categories_dict["category_crumb"]=fragrance_category_crumb.get(sub_categories_keys)
            fragrance_category_crumb_list.append(sub_categories_dict)

        fragrance_item["category_crumb"]=fragrance_category_crumb_list
        yield fragrance_item
        
        #skincare
        skincare_item = {}
        skincare_item['name'] = "Skincare"
        skincare_item['url'] = "https://www.yslbeauty.co.uk/skincare"
        
        skincare_category_name_url_dict={}
        for category_name in response.css('[data-category="skincare"] .c-navigation__item.m-level-2 [role="heading"]'):
            name=category_name.css('a::text').get().strip().lower().replace(" ","-")
            url=category_name.css('a::attr(href)').get().strip()
            skincare_category_name_url_dict[name]=url
        
        skincare_category_crumb={}
        skincare_category_crumb_list=[]
        for category in response.css('[data-category="skincare"] .c-navigation__item.m-level-2  .c-navigation__list.m-level-3 li a'):
            product_name=category.css('a::text').get().strip()
            product_url=category.css('a::attr(href)').get().strip()
            
            if product_name!="View all":
                sub_category_name=product_url.replace("https://www.yslbeauty.co.uk/skincare/",'').split("/")[0]
                if product_name=="Firming":
                    sub_category_name="concern"
                product={}
                product['name']=product_name
                product['url']=product_url
                if sub_category_name in skincare_category_crumb.keys():
                    skincare_category_crumb[sub_category_name].append(product)
                else:
                    skincare_category_crumb[sub_category_name]=[product]

        for sub_categories_keys in skincare_category_name_url_dict.keys():
            sub_categories_dict={}
            sub_categories_dict["name"]=sub_categories_keys
            sub_categories_dict["url"]=skincare_category_name_url_dict.get(sub_categories_keys)
            sub_categories_dict["category_crumb"]=skincare_category_crumb.get(sub_categories_keys)
            skincare_category_crumb_list.append(sub_categories_dict)

        skincare_item["category_crumb"]=skincare_category_crumb_list
        yield skincare_item
        