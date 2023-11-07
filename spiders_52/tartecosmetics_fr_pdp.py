import scrapy
import json
import re
import datetime
from parsel import Selector
from datetime import datetime
import datetime as currentdatetime
import os


class TartecosmeticsFrPdpSpider(scrapy.Spider):
    name = "tartecosmetics_fr_pdp"
    CURRENT_DATETIME = currentdatetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE = CURRENT_DATE.replace("-", "_")
    custom_settings = {
        "FEEDS": {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/tartecosmetics_fr/%(name)s_{DATE}.json": {"format": "json",}},
        "DUPEFILTER_DEBUG": True,
        "DUPEFILTER_CLASS": "scrapy.dupefilters.BaseDupeFilter",
        "CONCURRENT_REQUESTS": 16,
    }
    headers = {
        "accept": "*/*",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
    }

    async def requests_process(self, url, payload):
        request = scrapy.Request(url, method="POST", body=payload)
        response = await self.crawler.engine.download(request, self)
        return response

    async def requests_process_variant(self, url, headers):
        request = scrapy.Request(url, headers=headers, dont_filter=True)
        response = await self.crawler.engine.download(request, self)
        return response

    def review_write(self, review_response, review_list):
        review_list = []
        json_data = json.loads(review_response.text)[0]
        page_text = json_data["result"]
        selector = Selector(page_text)
        for reviews in selector.xpath('//div[@class="yotpo-review-wrapper"]/parent::div/parent::div'):
            if "  " != reviews.xpath('.//div[@class="content-review"]/text()').get(""):
                review_dict = {}
                start = reviews.xpath('.//span[@class="sr-only"]/text()').getall()[0]
                reviews_content = reviews.xpath('.//div[@class="yotpo-review-wrapper"]/span/text()').get("")
                review_content = (reviews.xpath('.//div[@class="content-review"]/text()').get("").strip())
                bold_word = (reviews.xpath('.//div[@class="content-title yotpo-font-bold"]/text()').get("").strip())
                review_dict["review"] = f"[{bold_word}] [{review_content}]"
                review_dict["stars"] = start.replace("star rating", "").strip()
                review_dict["user"] = re.findall("by([\w\W].*?)\s", reviews_content)[0].strip()
                timestamp = reviews.xpath('.//span[@class="y-label yotpo-review-date"]/text()').get()
                review_dict["date"] = datetime.strptime(timestamp, "%m/%d/%y").strftime("%Y-%m-%d")
                review_list.append(review_dict)
        return review_list

    def variant_data_size(self,response,price,price_before,promo_label,in_stock,image_url,has_video,video):
        variant_data_size = {}
        if re.search(r"sku:\s\'(.*?)\'\,\s*type", response.text):
            variant_data_size["sku_id"] = re.findall(r"sku:\s\'(.*?)\'\,\s*type", response.text)[0]
        else:
            variant_data_size["sku_id"] = None
        variant_data_size["gtin"] = response.xpath('//div[@class="product-detail product-wrapper"]/@data-pid|//div[@class="product-detail bundle-product-details product-wrapper"]/@data-pid').get()
        variant_data_size["variant_url"] = response.url
        variant_data_size["price"] = price
        variant_data_size["price_before"] = price_before
        variant_data_size["promo_label"] = promo_label
        variant_data_size["in-stock"] = in_stock
        variant_data_size["image_url"] = image_url
        variant_data_size["has_video"] = video
        variant_data_size["video"] = has_video
        variant_data_size["data_color"] = None
        if "travel-size" in response.url:
            variant_data_size["data_size"] = response.xpath("//span[contains(text(),'Travel Size')]/following-sibling::span/text()|//span[contains(text(),'Travel size')]/following-sibling::span/text()").get()
        else:
            variant_data_size["data_size"] = response.xpath("//span[contains(text(),'Full Size')]/following-sibling::span/text()|//span[contains(text(),'Full size')]/following-sibling::span/text()").get()
        return variant_data_size

    def variant(self, variant_json_content, url, promo_lable):
        variants_item = {}
        if variant_json_content:
            json_data = json.loads(variant_json_content.text)
            product_id = json_data["product"]["id"]
            for link in json_data["product"]["variationAttributes"][0]["values"]:
                varaint_id = link["productID"]
                if product_id == varaint_id:
                    variants_item["sku_id"] = product_id
                    variants_item["gtin"] = product_id
                    variants_item["variant_url"] = url
                    variants_item["price"] = json_data["product"]["price"]["sales"]["formatted"]
                    variants_item["in-stock"] = json_data["product"]["available"]
                    if json_data["product"]["price"]["list"]:
                        variants_item["price_before"] = json_data["product"]["price"]["list"]["formatted"]
                    else:
                        variants_item["price_before"] = None
                    variants_item["promo_label"] = promo_lable
                    if link["displayValue"]:
                        variants_item["data_color"] = link["displayValue"]
                    else:
                        variants_item["data_color"] = None
                    data_size = json_data["product"]["customAttributes"].get("productNetWeight", "")
                    if data_size:
                        variants_item["data_size"] = data_size
                    else:
                        variants_item["data_size"] = None
        image_list = []
        for image_link in json_data["product"]["images"]["small"]:
            image_list.append(image_link["url"])
        image = image_list
        variants_item["image_url"] = [re.sub(r"\s", "%20", space_image.strip()) for space_image in image]
        video_view = json_data["product"]["customAttributes"].get("videoAltImage", "")
        if video_view:
            variants_item["has_video"] = True
            variants_item["video"] = [video_view]
        else:
            variants_item["has_video"] = False
            variants_item["video"] = []
        return variants_item

    def start_requests(self):
        spider_name = TartecosmeticsFrPdpSpider.name.replace("pdp", "categories")
        current_date = currentdatetime.datetime.now().strftime("%Y-%m-%d")
        category_path = (
            os.getcwd() + f"/exports/{spider_name}/{spider_name}_{current_date}.json"
        )
        with open(category_path, "r", encoding="utf-8-sig") as f:
            json_file = f.read()
        category_names = json.loads(json_file)
        contents = category_names
        for main_cat in contents:
            category_name = main_cat.get("name", "").title()
            category_url = main_cat.get("url", "")
            for main_sub_cat in main_cat["category_crumb"]:
                if "category_crumb" in (main_sub_cat).keys():
                    main_category = main_sub_cat.get("name", "").title()
                    main_category_url = main_sub_cat.get("url", "")
                    for sub_cat in main_sub_cat["category_crumb"]:
                        sub_category = sub_cat.get("name", "").title()
                        sub_cat_url = sub_cat.get("url", "")
                        if "https://tartecosmetics.com/EU/en_FR/makeup/eyes/mascara/" in sub_cat_url:
                            yield scrapy.Request(url=(sub_cat_url),callback=self.parse,dont_filter=True,cb_kwargs={"category_name": category_name,"category_url": category_url,"main_category": main_category,"main_category_url": main_category_url,"sub_category": sub_category,"sub_cat_url": sub_cat_url,},)
                else:
                    main_category = main_sub_cat.get("name", "").title()
                    main_category_url = main_sub_cat.get("url", "")
                    sub_category = ""
                    sub_cat_url = ""
                    yield scrapy.Request(url=(main_category_url),callback=self.parse,dont_filter=True,cb_kwargs={"category_name": category_name,"category_url": category_url,"main_category": main_category,"main_category_url": main_category_url,"sub_category": sub_category,"sub_cat_url": sub_cat_url,},)

    def parse(self,response,category_name,category_url,main_category,main_category_url,sub_category,sub_cat_url):
        category_crumb = []
        main_cat = {}
        main_cat["name"] = category_name
        main_cat["url"] = category_url
        category_crumb.append(main_cat)
        sub_main_cat = {}
        sub_main_cat["name"] = main_category
        sub_main_cat["url"] = main_category_url
        category_crumb.append(sub_main_cat)
        if sub_category and sub_cat_url:
            sub_cat = {}
            sub_cat["name"] = sub_category
            sub_cat["url"] = sub_cat_url
            category_crumb.append(sub_cat)
        category_crumb = category_crumb
        product_Link = response.xpath('//div[@itemid="#product"]/div/div[@class="product"]/div/a/@href').getall()
        if product_Link == []:
            product_Link = response.xpath('//div[@class="product"]/div/a/@href').getall()
        for product_links in product_Link:
            yield scrapy.Request(response.urljoin(product_links),callback=self.parse_details,headers=self.headers,dont_filter=True,cb_kwargs={"category_crumb": category_crumb},)
        cgid = (response.xpath('//div[@class="page"]/@data-querystring').get("").replace("cgid=", ""))
        count = 0
        count_value = response.xpath('//span[@class="items-count"]/text()').get("")
        if count_value:
            count = int(re.findall("\d+", count_value)[0])
        for page_count in range(1, round(count / 12) + 1):
            page_change = str(page_count * 12)
            next_page_link = f"https://tartecosmetics.com/on/demandware.store/Sites-tarteRedesign-Site/en_FR/Search-UpdateGrid?cgid={cgid}&prefn1=excludedFromLocale&prefv1=false&start={page_change}&sz=12&selectedUrl=https%3A%2F%2Ftartecosmetics.com%2Fon%2Fdemandware.store%2FSites-tarteRedesign-Site%2Fen_FR%2FSearch-UpdateGrid%3Fcgid%3D{cgid}%26prefn1%3DexcludedFromLocale%26prefv1%3Dfalse%26start%3D24%26sz%3D12&loadMoreAjaxCall=true"
            yield scrapy.Request(next_page_link,callback=self.parse,headers=self.headers,dont_filter=True,cb_kwargs={"category_name": category_name,"category_url": category_url,"main_category": main_category,"main_category_url": main_category_url,"sub_category": sub_category,"sub_cat_url": sub_cat_url,},)

    async def parse_details(self, response, category_crumb):       
        item = {}
        item["url"] = response.url
        image = response.xpath('//img[@itemprop="image"]/parent::div/parent::div[contains(@class,"desktop-zoom")]/div/img/@src').getall()
        item["image_url"] = [re.sub(r"\s", "%20", space_image.strip()) for space_image in image]
        video_view = response.xpath('//figure[@class="video-container-vimeo"]/video/@data-src').get("")
        if video_view:
            item["has_video"] = True
            item["video"] = [video_view]
        else:
            item["has_video"] = False
            item["video"] = []
        item["name"] = response.xpath("//h1/text()").get("")
        sub_title = response.xpath('//div[@data-componentid="pdp-right-section"]/div[1]/div/div[@class="product-description"]/span/text()').get("")
        if sub_title:
            item["subtitle"] = sub_title
        else:
            item["subtitle"] = None
        if re.search('\"brand\"\:\{\"\@type\"\:\"[^>]*?\"\,\"name\"\:\"([^>]*?)\"\}',response.text):
                item["brand"] = re.findall('\"brand\"\:\{\"\@type\"\:\"[^>]*?\"\,\"name\"\:\"([^>]*?)\"\}',response.text)[0]
        item["gtin"] = response.xpath('//div[@class="product-detail product-wrapper"]/@data-pid|//div[@class="product-detail bundle-product-details product-wrapper"]/@data-pid').get()
        if re.search(r"id\:\s*\'(.*?)'\,\s*sku", response.text):
            item["master_product_id"] = re.findall(r"id\:\s*\'(.*?)'\,\s*sku", response.text)[0]
        else:
            item["master_product_id"] = None
        if len(item["master_product_id"]) == 0:
            master_id_1 = re.findall(r"alt_id\:\s*\'(.*?)'\s*\}", response.text)[0]
            item["master_product_id"] = master_id_1
        stock = response.xpath('//button[@data-eventname="add_to_cart"]/text()').get("")
        if "Add" in stock:
            item["in-stock"] = True
        else:
            item["in-stock"] = False
        item["price"] = (response.xpath('//div[@class="price"]/span/span/span/text()').get("").strip())
        before_price = response.xpath('//div[@class="prices-add-to-cart-actions"]//span[@class="valuePrice"]/text()').get('').replace('(','').replace(')','').replace('Value','').strip()
        if before_price == '':
            before_price = response.xpath('//div[@class="prices-add-to-cart-actions"]//span[@class="strike-through list  "]//span[contains(text(),"Price reduced from")]/following::text()[1]').get('').strip()
        if before_price:
            item["price_before"] = before_price
        else:
            item["price_before"] = None
        promo = response.xpath('//div[@class="primary-images"]//span[@class="savings-badge"]/span/text()').get("")
        if promo:
            item["promo_label"] = promo.strip()
        else:
            item["promo_label"] = None
        item["category_crumb"] = category_crumb
        description_details = " ".join(response.xpath("//button[contains(text(),'Details')]/parent::div/parent::div/div//text()").getall())
        description_Ingredients = " ".join(response.xpath("//button[contains(text(),'Skinvigorating™ Ingredients')]/parent::div/parent::div/div//text()").getall())
        description = f"{description_details} {description_Ingredients}"
        if description:
            item["description"] = re.sub("\s+", " ", description)
        else:
            item["description"] = None
        variant_content = []
        variant_link = response.xpath('//div[contains(@class,"has-shade-filter")]/div[@data-attr="color"]/div//div[contains(@class,"product-Color")]/button/@data-url|//div[contains(@class,"has-shade-filter")]/div[@data-attr="color"]/div//div[contains(@class,"product-color")]/button/@data-url').getall()
        two_product_link = response.xpath('//div[@class="bundle-items-header"]/text()').get("")
        variant = [block for block in variant_link if "null" not in block and "color=multi" not in block]
        if variant:
            if two_product_link == "":
                for url in variant:
                    variant_url = await self.requests_process_variant(url, headers=self.headers)
                    variant_text = self.variant(variant_url, item["url"], item["promo_label"])
                    variant_content.append(variant_text)
        elif response.xpath('//a[@data-event="Size Selector"]'):
            variant_text = variant_content.append(self.variant_data_size(response,item["price"],item["price_before"],item["promo_label"],item["in-stock"],item["image_url"],item["video"],item["has_video"]))   
        if bool(response.xpath('//div[@class="bundle-items-header"]/text()').get()):
            item["prices"] = []
        else:
            item["prices"] = variant_content
        app_key = response.xpath('//div[@class="yotpo yotpo-main-widget"]/@data-appkey').get("")
        product_id = response.xpath('//div[@class="yotpo yotpo-main-widget"]/@data-product-id').get("")
        review_list_content = []
        review_list = []
        if "" != product_id:
            review_url = f"https://staticw2.yotpo.com/batch/app_key/{app_key}/domain_key/{product_id}/widget/reviews"
            payload = f"methods=%5B%7B%22method%22%3A%22reviews%22%2C%22params%22%3A%7B%22pid%22%3A%22{product_id}%22%2C%22order_metadata_fields%22%3A%7B%7D%2C%22widget_product_id%22%3A%22{product_id}%22%2C%22data_source%22%3A%22default%22%2C%22page%22%3A1%2C%22host-widget%22%3A%22main_widget%22%2C%22is_mobile%22%3Afalse%2C%22pictures_per_review%22%3A10%7D%7D%5D&app_key={app_key}"
            review_resp = await self.requests_process(review_url, payload)
            review_lists = self.review_write(review_resp, review_list)
            review_list_content.append(review_lists)
            next_review_resp = ""
            json_data = json.loads(review_resp.text)[0]
            for i in json_data:
                if "result" in i:
                    page_text = json_data["result"]
                    selector = Selector(page_text)
                    pages = selector.xpath('//a[@class="yotpo-page-element goTo "]/text()').get()
            review_count = selector.xpath('//div[@class="total-reviews-search"]/@total-reviews-search').get("")
            review_count_div = round(int(review_count) / 5) + 2
            for pages in range(2, review_count_div + 1):
                payload_1 = f"methods=%5B%7B%22method%22%3A%22reviews%22%2C%22params%22%3A%7B%22pid%22%3A%22{product_id}%22%2C%22order_metadata_fields%22%3A%7B%7D%2C%22widget_product_id%22%3A%22{product_id}%22%2C%22data_source%22%3A%22default%22%2C%22page%22%3A{pages}%2C%22host-widget%22%3A%22main_widget%22%2C%22is_mobile%22%3Afalse%2C%22pictures_per_review%22%3A10%7D%7D%5D&app_key={app_key}"
                nextpage_url = f"https://staticw2.yotpo.com/batch/app_key/{app_key}/domain_key/{product_id}/widget/reviews"
                next_review_resp = await self.requests_process(nextpage_url, payload_1)
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
