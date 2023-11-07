import re
import os
import scrapy
import datetime
from dotenv import load_dotenv

load_dotenv()


class BobbieUKSpider(scrapy.Spider):
    name = "bobbi_brown_uk_categories"
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    ROTATING_PROXY = os.getenv("ROTATING_PROXY")

    custom_settings={'FEEDS' : {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/bobbi_brown_uk/%(name)s_{DATE}.json": {"format": "json",}},
                    'ROTATING_PROXY_LIST' : [ROTATING_PROXY]
                    }
    async def request_process(self, url):
        request = scrapy.Request(url)
        response = await self.crawler.engine.download(request, self)
        return response

    def regex_parse(self,pattern,text):
        if re.search(pattern,text,re.I):
            data = re.findall(pattern,text,re.I)
            return data[0]
        else:
            return ''

    def start_requests(self):
        urls = ['https://www.bobbibrown.co.uk/products/2321/makeup','https://www.bobbibrown.co.uk/products/14006/skincare']
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    async def parse(self, response):
        item = {}
        category_list = []
        item['name'] = response.xpath('//span[@class="breadcrumbs__level breadcrumbs__level--2"]/a/text()').get('').strip()
        item['url'] = response.url
        if '/makeup' in response.url:
            category_urls = response.xpath('//a[contains(text(),"MAKEUP")]/parent::div//a[contains(text(),"Shop All")]/@href|//a[contains(text(),"Shop Makeup Brushes")]/@href|//a[contains(text(),"Shop Fragrance")]/@href').getall()
            category_names = response.xpath('//div[@class="gnav-link-tout__header js-track-sub-category-name"]/text()').getall()
            category_list = []
            for cou,category_url in enumerate(category_urls[1:]):
                category_dict = {}
                cat_parse = await self.request_process(response.urljoin(category_url))
                if not re.search(r'product\_name\"\:\[\"',cat_parse.text):
                    category_dict['name'] = category_names[cou].strip()
                    category_dict['url'] = response.urljoin(category_url)
                    
                    sub_cating_names = [e.strip() for e in cat_parse.xpath('//div[@class="gnav-link-tout__header js-track-sub-category-name"]/text()').getall()]
                    sub_cating_names.append('<picture>')
                    regex_block = self.regex_parse(f'{sub_cating_names[cou]}<\/div>([\w\W]*?)\s*{sub_cating_names[cou+1]}<\/div>',cat_parse.text)
                    if re.search(r'<a\s*id[\w\W]*?href\=\"(.*?)\"',regex_block,re.DOTALL):
                        sub_cat_lt = []
                        sub_cat_urls = re.findall(r'<a\s*id[\w\W]*?href\=\"(.*?)\"',regex_block,re.DOTALL)
                        sub_cat_names = re.findall(r'<title>(.*?)<\/title>',regex_block,re.DOTALL)
                        if len(sub_cat_urls) > 1:
                            for count,sub_cat in enumerate(sub_cat_urls[1:]):
                                sub_cat_dict = {}
                                sub_cat_dict['name'] = sub_cat_names[count + 1]
                                sub_cat_dict['url'] = response.urljoin(sub_cat)
                                sub_cat_lt.append(sub_cat_dict)
                        else:
                            pass
                        if sub_cat_lt:
                            category_dict['category_crumb'] = sub_cat_lt
                        else:
                            pass
                    category_list.append(category_dict)
                else:
                    # category_dict['name'] = (self.regex_parse(r'product\_category\_name\"\:\[\"(.*?)\"',cat_parse.text)).title()
                    category_dict['name'] = cat_parse.xpath('//div[@class="spp-product-layout__breadcrumb"]/a[1]/text()').get('').title()
                    category_dict['url'] = response.urljoin(category_url)
                    category_list.append(category_dict)
                
            item['category_crumb'] = category_list
            yield item
        else:
            category_urls = response.xpath('//div[contains(@trackname,"Shop - SKINCARE")]//div[@class="gnav-link-tout__link"]/a/@href').getall()
            category_names = response.xpath('//div[contains(@trackname,"Shop - SKINCARE")]//div[@class="gnav-link-tout__link"]/a/text()').getall()
            for cou,category_url in enumerate(category_urls[1:]):
                category_dict = {}
                category_dict['name'] = category_names[cou+1].strip()
                category_dict['url'] = response.urljoin(category_url)
                category_list.append(category_dict)
            item['category_crumb'] = category_list
            yield item