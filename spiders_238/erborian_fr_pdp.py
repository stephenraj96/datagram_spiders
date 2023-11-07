import re
import os
import json
import time
import scrapy
import requests
import urllib.parse
from parsel import Selector
from datetime import datetime
import datetime as currentdatetime
from dotenv import load_dotenv
from pathlib import Path


""" load env file """
try:
    load_dotenv()
except:
    env_path = Path(".env")
    load_dotenv(dotenv_path=env_path)


class ErborianFrPdpSpider(scrapy.Spider):
    name = "erborian_fr_pdp"
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
    }

    """ Get token in env file"""
    api_token = os.getenv("api_token")
    CURRENT_DATETIME = currentdatetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE = CURRENT_DATE.replace("-", "_")
    custom_settings = {
        "FEEDS": {
            f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/erborian_fr/%(name)s_{DATE}.json": {
                "format": "json"
            }
        },
        "CONCURRENT_REQUESTS": 1,
    }

    def retry_function_product_collection(self, url):
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

    def review_write(self, review_response, review_list):
        review_list = []
        json_data = json.loads(review_response.text)[0]
        page_text = json_data["result"]
        selector = Selector(page_text)
        for reviews in selector.xpath(
            '//div[@class="yotpo-review-wrapper"]/parent::div/parent::div'
        ):
            review_dict = {}
            start = reviews.xpath('//span[contains(text(),"star rating")]/text()').get()
            reviews_content = reviews.xpath(
                './/div[@class="yotpo-review-wrapper"]/span/text()'
            ).get()
            review_dict["review"] = (
                reviews.xpath('.//div[@class="content-review"]/text()').get().strip()
            )
            review_dict["stars"] = start.replace("star rating", "").strip()
            review_dict["user"] = re.findall("by([\w\W].*?)\s", reviews_content)[
                0
            ].strip()
            timestamp = reviews.xpath(
                './/span[@class="y-label yotpo-review-date"]/text()'
            ).get()
            review_dict["date"] = datetime.strptime(timestamp, "%d/%m/%y").strftime(
                "%Y-%m-%d"
            )
            review_list.append(review_dict)
        return review_list

    def variant(self, variant_url, variant_url_text):
        item_1 = {}
        if re.search(
            r'type="application\/ld\+json"\>([\w\W]*?)<\/script>', variant_url_text
        ):
            price_collection = re.findall(
                r'type="application\/ld\+json"\>([\w\W]*?)<\/script>', variant_url_text
            )[0].strip()
            price_collections = json.loads(price_collection)
            item_1["sku_id"] = price_collections["sku"]
            item_1["price"] = (
                variant_url.xpath('//p[@class="a-price-sales"]/text()').get("").strip()
            )
            stock_value = price_collections["offers"]["availability"]
            if "InStock" in stock_value:
                item_1["in-stock"] = True
            else:
                item_1["in-stock"] = False
            if variant_url.xpath(
                '//div[@class="m-product-price"]/span[@class="a-discount-percentage"]'
            ):
                price_before = "".join(
                    variant_url.xpath(
                        '//div[@class="m-product-price"]/span[2]/text()|//*[contains(text(),"Ancien prix:")]/parent::p/text()'
                    ).getall()
                ).strip()
                item_1["price_before"] = price_before.replace("Valeur de ", " ").strip()
                item_1["promo_label"] = (
                    variant_url.xpath(
                        '//div[@class="m-product-price"]/span[@class="a-discount-percentage"]/text()'
                    )
                    .get("")
                    .strip()
                )
            else:
                item_1["price_before"] = None
                item_1["promo_label"] = None
            item_1["variant_url"] = price_collections["offers"]["url"]
            image = variant_url.xpath(
                '//li[@class="swiper-slide"]//img/@src|//li[@class="swiper-slide"]//img/@data-src'
            ).getall()
            item_1["image_url"] = [
                re.sub(r"\s", "%20", space_image.strip()) for space_image in image
            ]
            item_1["data_size"] = variant_url.xpath(
                '//div[contains(text(),"Volume")]/span/text()'
            ).get()
            colour_no = variant_url.xpath(
                '//li[@class="m-attribute"]/div/span/text()'
            ).get()
            if re.search(r"\d+", colour_no):
                item_1["data_color"] = None
            else:
                item_1["data_color"] = variant_url.xpath(
                    '//li[@class="m-attribute"]/div/span/text()'
                ).get("")
            item_1["gtin"] = price_collections["gtin13"]

        return item_1

    def start_requests(self):
        spider_name = ErborianFrPdpSpider.name.replace("pdp", "categories")
        current_date = currentdatetime.datetime.now().strftime("%Y-%m-%d")
        category_path = (
            os.getcwd() + f"/exports/{spider_name}/{spider_name}_{current_date}.json"
        )
        with open(category_path, "r", encoding="utf-8-sig") as f:
            json_file = f.read()
        category_names = json.loads(json_file)
        for main_cat in category_names:
            category_name = main_cat.get("name", "").title()
            category_url = main_cat.get("url", "")
            category_crumb = main_cat.get("category_crumb", "")
            category_content = {
                "category_name": category_name,
                "category_url": category_url,
                "category_crumb": category_crumb,
            }
            url = f"https://api.scrape.do/?token={self.api_token}&url={category_url}"
            time.sleep(2)
            yield scrapy.Request(
                url,
                callback=self.parse,
                headers=self.headers,
                cb_kwargs={
                    "category_content": category_content,
                },
            )

    async def requests_process(self, url, payload):
        request = scrapy.Request(url, method="POST", body=payload)
        response = await self.crawler.engine.download(request, self)
        return response

    async def requests_process_price(self, url):
        request = scrapy.Request(url, dont_filter=True)
        response = await self.crawler.engine.download(request, self)
        return response

    def parse(self, response, category_content):
        link_split = response.url
        link = link_split.split("url=")[-1]
        if re.search(r"Please\s*enable\s*JS\s*and\s*disable", response.text):
            response, response_text = self.retry_function_product_collection(
                response.url
            )
        product_link_collection = response.xpath(
            '//h2[@class="m-product-name"]/a/@href'
        ).getall()
        for block in product_link_collection:
            product_link = f"https://fr.erborian.com/{block}"
            time.sleep(2)
            yield scrapy.Request(
                f"https://api.scrape.do/?token={self.api_token}&url={urllib.parse.quote(product_link)}",
                callback=self.parse_details,
                headers=self.headers,
                dont_filter=True,
                cb_kwargs={
                    "product_link": product_link,
                    "category_content": category_content,
                },
            )
        count = 0
        count_value = response.xpath("//h1/span[2]//text()").get("").strip()
        if count_value:
            count = int(re.findall("\d+", count_value)[0])
        two_catgory_search = "?srule=Category-default&sz=12&start=12&format=ajax&type=view-more&context=products-list"
        urls = f"{link}{two_catgory_search}"
        for pages_cat in range(1, round(count / 12) + 1):
            page_change = urls.replace(
                "sz=12&start=12", f"sz=12&start={str(pages_cat * 12)}"
            )
            next_page_link = f"https://api.scrape.do/?token={self.api_token}&url={urllib.parse.quote(page_change)}"
            yield scrapy.Request(
                next_page_link,
                callback=self.parse,
                headers=self.headers,
                dont_filter=True,
                cb_kwargs={"category_content": category_content},
            )

    async def parse_details(self, response, product_link, category_content):

        if re.search(r"Please\s*enable\s*JS\s*and\s*disable", response.text):
            response, response_text = self.retry_function_product_collection(
                response.url
            )
        else:
            response_text = response.text
        if response.xpath('//h1[@itemprop="name"]/div/text()'):
            item = {}
            item["url"] = product_link
            image = response.xpath(
                '//li[@class="swiper-slide"]//img/@src|//li[@class="swiper-slide"]//img/@data-src'
            ).getall()
            item["image_url"] = [
                re.sub(r"\s", "%20", space_image.strip()) for space_image in image
            ]
            item["name"] = (
                response.xpath('//h1[@itemprop="name"]/div/text()').get("").strip()
            )
            if response.xpath('//h2[@class="a-product-subtitle"]/text()'):
                item["subtitle"] = (
                    response.xpath('//h2[@class="a-product-subtitle"]/text()')
                    .get("")
                    .strip()
                )
            else:
                item["subtitle"] = None
            item["category_crumb"] = [
                {
                    "name": category_content["category_name"],
                    "url": category_content["category_url"],
                }
            ]

            if re.search(r"siteLocale\"\:\"fr_FR\"\,\"brand\"\:\"([^>]*?)\"\,", response_text):
                item["brand"] = re.findall(
                    r"siteLocale\"\:\"fr_FR\"\,\"brand\"\:\"([^>]*?)\"\,", response_text
                )[0]
            else:
                item["brand"] = None
            if re.search(r"id\:\s*\'(.*?)'\,\s*sku", response_text):
                item["master_product_id"] = re.findall(
                    r"id\:\s*\'(.*?)'\,\s*sku", response_text
                )[0]
            else:
                item["master_product_id"] = None
            if len(item["master_product_id"]) == 0:
                master_id_1 = re.findall(r"alt_id\:\s*\'(.*?)'\s*\}", response_text)[0]
                item["master_product_id"] = master_id_1
            if re.search('"gtin13":("\d+")', response_text):
                item["gtin"] = re.findall('gtin13"\:"(\d+)"', response_text)[0]
            else:
                item["gtin"] = None
            if response.xpath(
                '//div[@class="m-product-price"]/span[@class="a-discount-percentage"]'
            ):
                price_before = "".join(
                    response.xpath(
                        '//div[@class="m-product-price"]/span[2]/text()|//*[contains(text(),"Ancien prix:")]/parent::p/text()'
                    ).getall()
                ).strip()
                item["price_before"] = price_before.replace("Valeur de ", " ").strip()
                item["promo_label"] = (
                    response.xpath(
                        '//div[@class="m-product-price"]/span[@class="a-discount-percentage"]/text()'
                    )
                    .get("")
                    .strip()
                )
            else:
                item["promo_label"] = None
                item["price_before"] = None
            if response.xpath('//p[@class="a-price-sales"]/text()'):
                item["price"] = (
                    response.xpath('//p[@class="a-price-sales"]/text()').get("").strip()
                )
            else:
                item["price"] = None
            stock = (
                response.xpath('//div[@class="m-add-to-cart-static"]/button/text()')
                .get("")
                .strip()
            )
            if "Ajouter" in stock:
                item["in-stock"] = True
            else:
                item["in-stock"] = False
            video_view = response.xpath('//div[@class="m-video"]//iframe/@src').get("")
            if video_view:
                item["has_video"] = True
                item["video"] = [video_view]
            else:
                item["has_video"] = False
                item["video"] = []
            first_varaint = []
            first_varaint = response.xpath('//a[@class="swatchanchor"]/@href').getall()
            first_varaint_colour = [x.strip() for x in first_varaint if "" != x]
            second_varaint_colour = []
            for block in first_varaint_colour:
                variant_link = urllib.parse.quote(block)
                time.sleep(3)
                price_url = await self.requests_process_price(
                    f"https://api.scrape.do/?token={self.api_token}&url={variant_link}"
                )
                first_variant_link_text = price_url.text
                if re.search(
                    "Please\s*enable\s*JS\s*and\s*disable\s*any\s*ad\s*blocker",
                    first_variant_link_text,
                ):
                    (
                        price_url,
                        first_variant_link_text,
                    ) = self.retry_function_product_collection(
                        f"https://api.scrape.do/?token={self.api_token}&url={variant_link}"
                    )

                second_varaint = price_url.xpath(
                    '//a[@class="swatchanchor"]/@href'
                ).getall()
                second_varaint_colour = [x.strip() for x in second_varaint if "" != x]
            concate_two_list = first_varaint_colour + second_varaint_colour
            contate_link = list(set(concate_two_list))
            varaint_collection = []
            if contate_link == [] and first_varaint == []:
                paths = response.xpath('//div[@class="product-variations"]')
                if paths:
                    contate_link.append(urllib.parse.unquote(product_link))
                    first_varaint.append(urllib.parse.unquote(product_link))
            else:
                first_varaint = first_varaint
                contate_link = contate_link

            if first_varaint:
                for links in contate_link:
                    link = urllib.parse.quote(links)
                    time.sleep(2)
                    price_urls = await self.requests_process_price(
                        f"https://api.scrape.do/?token={self.api_token}&url={link}"
                    )
                    price_response_text = price_urls.text
                    if re.search(
                        "Please\s*enable\s*JS\s*and\s*disable\s*any\s*ad\s*blocker",
                        price_response_text,
                    ):
                        (
                            price_urls,
                            price_response_text,
                        ) = self.retry_function_product_collection(
                            f"https://api.scrape.do/?token={self.api_token}&url={link}"
                        )
                    first_variant_link = self.variant(price_urls, price_response_text)
                    if first_variant_link:
                        varaint_collection.append(first_variant_link)
                    else:
                        second_variant_link = self.variant(response, response_text)
                        varaint_collection.append(second_variant_link)
                if varaint_collection == []:
                    third_variant_link = self.variant(response, response_text)
                    varaint_collection.append(third_variant_link)
                item["prices"] = varaint_collection
            else:
                item["prices"] = []
            des = "".join(
                response.xpath(
                    '//div[@class="row"]/div[@class="o-tabs-container col"]/div[1]/div//text()'
                ).getall()
            )
            if des != "":
                item["description"] = (
                    des.replace("Description", "").replace("\xa0", "").replace("\n", "")
                )
            else:
                item["description"] = None
            app_key = response.xpath('//div[@id="reviews"]/div/@data-appkey').get("")
            product_id = response.xpath(
                '//div[@class="o-product-ugc container"]/div/@data-product-id'
            ).get("")
            review_list_content = []
            review_list = []
            next_review_resp = ""
            if "" != product_id:
                review_url = f"https://staticw2.yotpo.com/batch/app_key/{app_key}/domain_key/{product_id}/widget/main_widget"
                payload = f"methods=%5B%7B%22method%22%3A%22main_widget%22%2C%22params%22%3A%7B%22pid%22%3A%22{product_id}%22%2C%22order_metadata_fields%22%3A%7B%7D%2C%22widget_product_id%22%3A%22{product_id}%22%7D%7D%5D&app_key={app_key}&is_mobile=false&widget_version=2023-02-16_16-17-45"
                time.sleep(2)
                review_resp = await self.requests_process(review_url, payload)
                review_lists = self.review_write(review_resp, review_list)
                review_list_content.append(review_lists)
                next_review_resp = ""
                json_data = json.loads(review_resp.text)[0]
                for i in json_data:
                    if "result" in i:
                        page_text = json_data["result"]
                        selector = Selector(page_text)
                        pages = selector.xpath(
                            '//a[@class="yotpo-page-element goTo "]/text()'
                        ).get()
                review_count = selector.xpath(
                    '//div[@class="total-reviews-search"]/@total-reviews-search'
                ).get()
                review_count_div = round(int(review_count) / 10) + 2
                for pages in range(2, review_count_div):
                    payload_1 = f"methods=%5B%7B%22method%22%3A%22reviews%22%2C%22params%22%3A%7B%22pid%22%3A%22{product_id}%22%2C%22order_metadata_fields%22%3A%7B%7D%2C%22widget_product_id%22%3A%22{product_id}%22%2C%22data_source%22%3A%22default%22%2C%22page%22%3A{pages}%2C%22host-widget%22%3A%22main_widget%22%2C%22is_mobile%22%3Afalse%2C%22pictures_per_review%22%3A10%7D%7D%5D&app_key={app_key}&is_mobile=false&widget_version=2023-02-16_16-17-45"
                    nextpage_url = f"https://staticw2.yotpo.com/batch/app_key/{app_key}/domain_key/{product_id}/widget/reviews"
                    next_review_resp = await self.requests_process(
                        nextpage_url, payload_1
                    )
                    time.sleep(3)
                    review_lists = self.review_write(next_review_resp, review_list)
                    review_list_content.append(review_lists)
            review_comments = [
                review_value
                for review_text in review_list_content
                for review_value in review_text
            ]
            if review_comments:
                item["reviews"] = review_comments
            else:
                item["reviews"] = []
            yield item
