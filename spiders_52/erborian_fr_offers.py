import re
import os
import time
import scrapy
import requests
import urllib.parse
import datetime as currentdatetime
from parsel import Selector
from dotenv import load_dotenv
from pathlib import Path

""" load env file """
try:
    load_dotenv()
except:
    env_path = Path(".env")
    load_dotenv(dotenv_path=env_path)


class ErborianFrOffersSpider(scrapy.Spider):
    name = "erborian_fr_offers"
    allowed_domains = ["erborian_fr_offers.com", "api.scrape.do"]
    """ Get token in env file"""
    api_token = os.getenv("api_token")
    CURRENT_DATETIME = currentdatetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE = CURRENT_DATE.replace("-", "_")
    dup_list = []
    custom_settings = {
        "FEEDS": {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/erborian_fr/%(name)s_{DATE}.json": {"format": "json",}},
        "DUPEFILTER_DEBUG": True,
        "DUPEFILTER_CLASS": "scrapy.dupefilters.BaseDupeFilter",
        "CONCURRENT_REQUESTS": 1,
    }
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
    }

    async def requests_process(self, url, headers):
        request = scrapy.Request(url, headers=headers, dont_filter=True)
        response = await self.crawler.engine.download(request, self)
        return response

    def retry_function_product_coll(self, url):
        retry_count = 0
        while retry_count <= 10:
            retry_response = requests.get(url)
            retry_response_text = retry_response.text
            retry_response = Selector(text=retry_response.text)
            if re.search(r"Please\s*enable\s*JS\s*and\s*disable", retry_response_text):
                retry_count += 1
            else:
                break
        return retry_response, retry_response_text

    def retry_function(self, url):
        retry_count = 0
        while retry_count <= 10:
            retry_response = requests.get(url)
            retry_response_text = retry_response.text
            if re.search(r"Please\s*enable\s*JS\s*and\s*disable", retry_response_text):
                retry_count += 1
            else:
                break
        return retry_response, retry_response_text

    def product_details(self, url, rank, block):
        product = Selector(url.text)
        item_2 = {}
        item_2["rank"] = rank
        item_2["url"] = block
        image = product.xpath('//li[@class="swiper-slide"]//img/@src|//li[@class="swiper-slide"]//img/@data-src').getall()
        item_2["image_url"] = [re.sub(r"\s", "%20", space_image.strip()) for space_image in image]
        var = None
        mater_id = None
        if re.search(r"id\:\s*\'(.*?)'\,\s*sku", url.text):
            var = re.findall(r"id\:\s*\'(.*?)'\,\s*sku", url.text)[0]
        if re.search(r"alt_id\:\s*\'(.*?)'\s*\}", url.text):
            mater_id = re.findall(r"alt_id\:\s*\'(.*?)'\s*\}", url.text)[0]
                # item_2["master_product_id"] = re.findall(r"id\:\s*\'(.*?)'\,\s*sku", url.text)[0]
        # else:
            # item_2["master_product_id"] = None
        if var:
            item_2["master_product_id"] =var
        elif mater_id:
            item_2["master_product_id"] = mater_id
        else:
            item_2["master_product_id"] = None
        # item_2["master_product_id"] = master_id_1
        # breakpoint()            
        #     breakpoint()
        #     if re.search(r"alt_id\:\s*\'(.*?)'\s*\}", url.text):
        #         master_id_1 = re.findall(r"alt_id\:\s*\'(.*?)'\s*\}", url.text)[0]
        #         item_2["master_product_id"] = master_id_1
        # if len(item_2["master_product_id"]) == 0:
        #     breakpoint()
        #     master_id_1 = re.findall(r"alt_id\:\s*\'(.*?)'\s*\}", url.text)[0]
        #     item_2["master_product_id"] = master_id_1
        if re.search('"gtin13":("\d+")', url.text):
            item_2["gtin"] = re.findall('gtin13"\:"(\d+)"', url.text)[0]
        else:
            item_2["gtin"] = None
        item_2["name"] = (
            product.xpath('//h1[@itemprop="name"]/div/text()').get("").strip()
        )
        if re.search(r"siteLocale\"\:\"fr_FR\"\,\"brand\"\:\"([^>]*?)\"\,", url.text):
            item_2["brand"] = re.findall(r"siteLocale\"\:\"fr_FR\"\,\"brand\"\:\"([^>]*?)\"\,", url.text)[0]
        else:
            item_2["brand"] = None

        stock = (product.xpath('//div[@class="m-add-to-cart-static"]/button/text()').get("").strip())
        if "Ajouter" in stock:
            item_2["in-stock"] = True
        else:
            item_2["in-stock"] = False
        if product.xpath('//div[@class="m-product-price"]/span[@class="a-discount-percentage"]'):
            price_before = "".join(product.xpath('//div[@class="m-product-price"]/span[2]/text()|//*[contains(text(),"Ancien prix:")]/parent::p/text()').getall()).strip()
            item_2["price_before"] = price_before.replace("Valeur de ", " ").strip()
            item_2["promo_label"] = (product.xpath('//div[@class="m-product-price" ]/span[@class="a-discount-percentage"]/text()').get("").strip())
        else:
            item_2["promo_label"] = None
            item_2["price_before"] = None
        if product.xpath('//p[@class="a-price-sales"]/text()'):
            item_2["price"] = (product.xpath('//p[@class="a-price-sales"]/text()').get("").strip())
        else:
            item_2["price"] = None
        video_view = product.xpath('//div[@class="m-video"]//iframe/@src').get("")
        if video_view:
            item_2["has_video"] = True
            item_2["video"] = [video_view]
        else:
            item_2["has_video"] = False
            item_2["video"] = []
        # print(item_2)
        return item_2

    def start_requests(self):
        dir_path = os.path.abspath(__file__ + "/../../../")
        supporting_files = os.path.join(dir_path, "supporting_files")
        with open(f"{supporting_files}/fr_erborian_offer_url.txt") as f:
            offer_urls = f.readlines()
        for offer_url in offer_urls:
            url = f"https://api.scrape.do/?token={self.api_token}&url={offer_url}&super=true&geoCode=fr"
            time.sleep(10)
            yield scrapy.Request(url, headers=self.headers, callback=self.parse)

    async def parse(self, response):
        link_split = response.url
        link = link_split.split("url=")[-1].replace("&super=true&geoCode=fr","").strip()
        if re.search(
            "Please\s*enable\s*JS\s*and\s*disable\s*any\s*ad\s*blocker", response.text
        ):
            time.sleep(10)
            yield scrapy.Request(
                f"https://api.scrape.do/?token={self.api_token}&url={link}&super=true&geoCode=fr",
                headers=self.headers,
                callback=self.parse,
            )
        block = ""
        count_value = response.xpath('//h1[@aria-live="assertive"]//text()|//h1/span[2]//text()').get("")
        if response.xpath("//h1"):
            item = {}
            rank = 0
            if "recherche?" in response.url:
                if count_value:
                    item["title"] = response.xpath("//h1/span/text()").get("").strip()
                    item["count"] = int(re.findall("\d+", count_value)[0])
                elif response.xpath('//h2[@class="a-product-subtitle"]'):
                    pagesurls = link
                    item["title"] = pagesurls.split("q=")[-1].replace("+", " ")
                    rank = 1
                    block = link
                    item["count"] = 1
                    if "https://fr.erborian.com/recherche?q=roller" in pagesurls:
                        item["title"] = "roller"
                else:
                    if response.xpath("//h1/strong/text()").get("").replace('"', ""):
                        item["title"] = (response.xpath("//h1/strong/text()").get("").replace('"', ""))
                        item["count"] = 0
                        item["products"] = []
                    else:
                        if "https://fr.erborian.com/recherche?q=roller" in pagesurls:
                            item["title"] = "roller"
                            rank = 1
                            block = link
                            item["count"] = 1
            else:
                item["title"] = response.xpath("//h1/span[1]//text()").get("").strip()
                item["count"] = int(re.findall("\d+", count_value)[0])
            item["page_url"] = link
            product_list = []
            item["products"] = []
            if "Aucun résultat" not in response.xpath("//h1/text()").get("").strip():
                product_link_collection = response.xpath('//*[@class="m-product-name"]/a/@href').getall()
                product_list.append(product_link_collection[1:])
                two_catgory_search = ["?srule=Category-default&sz=12&start=12&format=ajax&type=view-more&context=products-list","&srule=Name&sz=12&start=12&format=ajax&type=view-more&context=products-list",]
                for pages_cat in range(1, round(item["count"] / 12)+1):
                    if "recherche" not in link:
                        urls = f"{link}{two_catgory_search[0]}"
                        page_change = urls.replace("sz=12&start=12", f"sz=12&start={str(pages_cat * 12)}")
                        next_page_link = f"https://api.scrape.do/?token={self.api_token}&url={urllib.parse.quote(page_change)}&super=true&geoCode=fr"
                        next_page_url_cat = await self.requests_process(next_page_link, headers=self.headers)
                        time.sleep(5)
                        if re.search("Please\s*enable\s*JS\s*and\s*disable\s*any\s*ad\s*blocker",next_page_url_cat.text,):
                            time.sleep(5)
                            (next_page_url_cat,next_page_url_cat_re,) = self.retry_function_product_coll(next_page_link)
                    else:
                        urls = f"{link}{two_catgory_search[0]}"
                        page_change = urls.replace("sz=12&start=12", f"sz=12&start={str(pages_cat * 12)}")
                        next_page_link = f"https://api.scrape.do/?token={self.api_token}&url={urllib.parse.quote(page_change)}&super=true&geoCode=fr"
                        next_page_url_cat = await self.requests_process(next_page_link, headers=self.headers)
                        time.sleep(10)
                        if re.search("Please\s*enable\s*JS\s*and\s*disable\s*any\s*ad\s*blocker",next_page_url_cat.text,):
                            time.sleep(5)
                            (next_page_url_cat,next_page_url_cat_re,) = self.retry_function_product_coll(next_page_link)
                    product_link_collection_cat = next_page_url_cat.xpath('//*[@class="m-product-name"]/a/@href').getall()
                    product_list.append(product_link_collection_cat)
                product_list_collection = [product_list_link for product_link in product_list for product_list_link in product_link]
                category_crumb = []
                category = response.xpath('//div[@id="primary"]/div/script').getall()
                if [] != category:
                    category = category[0]
                    category_text = re.findall('id"\:\s*"([\w\W]*?)"\,', category)
                    category_value = re.findall(r'"name":\s*\"(.*?)\"\s*', category)[1:]

                    for keys, values in zip(category_value, category_text):
                        item_1 = {}
                        item_1["name"] = (keys.replace("&egrave;", "").replace("&amp;", "").split("-")[0])
                        item_1["url"] = values
                        category_crumb.append(item_1)
                else:
                    item["category_crumb"] = []
                item["category_crumb"] = category_crumb
                product = []
                for rank, block in enumerate(product_list_collection, 1):
                    if block:
                        block = "https://fr.erborian.com/" + block
                        product_link = f"https://api.scrape.do/?token={self.api_token}&url={urllib.parse.quote(block)}&super=true&geoCode=fr"
                        url = await self.requests_process(product_link, headers=self.headers)
                        if re.search("Please\s*enable\s*JS\s*and\s*disable\s*any\s*ad\s*blocker",url.text):
                            url, context = self.retry_function(product_link)
                            time.sleep(10)
                        product_ollection = self.product_details(url, rank, block)
                        product.append(product_ollection)
                if product:
                    item["products"] = product
                else:
                    item["products"] = [self.product_details(response, rank, block)]
            yield item
