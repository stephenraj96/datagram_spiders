import re
import os
import time
import scrapy
import datetime as currentdatetime


class TartecosmeticsFrOffersSpider(scrapy.Spider):
    name = "tartecosmetics_fr_offers"
    CURRENT_DATETIME = currentdatetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE = CURRENT_DATE.replace("-", "_")
    custom_settings = {
        "FEEDS": {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/tartecosmetics_fr/%(name)s_{DATE}.json": {"format": "json",}},
        "DUPEFILTER_DEBUG": True,
        "DUPEFILTER_CLASS": "scrapy.dupefilters.BaseDupeFilter",
        "CONCURRENT_REQUESTS": 3,
    }

    def product_collectio(self, product_content, url, rank):
        product_item = {}
        product_item["rank"] = rank
        product_item["url"] = url
        product_item["name"] = product_content.xpath("//h1/text()").get("")
        image= product_content.xpath('//img[@itemprop="image"]/parent::div/parent::div[contains(@class,"desktop-zoom")]/div/img/@src').getall()
        product_item["image_url"] = [re.sub(r"\s", "%20", space_image.strip()) for space_image in image]
        if re.search(r"id\:\s*\'(.*?)'\,\s*sku", product_content.text):
            product_item["master_product_id"] = re.findall(r"id\:\s*\'(.*?)'\,\s*sku", product_content.text)[0]
        else:
            product_item["master_product_id"] = None
        if len(product_item["master_product_id"]) == 0:
            master_id_1 = re.findall(r"alt_id\:\s*\'(.*?)'\s*\}", product_content.text)[0]
            product_item["master_product_id"] = master_id_1
        if re.search('\"brand\"\:\{\"\@type\"\:\"[^>]*?\"\,\"name\"\:\"([^>]*?)\"\}',product_content.text):
            product_item["brand"] = re.findall('\"brand\"\:\{\"\@type\"\:\"[^>]*?\"\,\"name\"\:\"([^>]*?)\"\}',product_content.text)[0]
        product_item["gtin"] = product_content.xpath('//div[@class="product-detail product-wrapper"]/@data-pid|//div[@class="product-detail bundle-product-details product-wrapper"]/@data-pid').get()
        product_item["name"] = product_content.xpath("//h1/text()").get("")
        product_item["price"] = (product_content.xpath('//div[@class="price"]/span/span/span/text()').get("").strip())
        stock = product_content.xpath('//button[@data-eventname="add_to_cart"]/text()').get("")
        if "Add" in stock:
            product_item["in-stock"] = True
        else:
            product_item["in-stock"] = False
        video_view = product_content.xpath(
            '//figure[@class="video-container-vimeo"]/video/@data-src'
        ).get("")
        if video_view:
            product_item["has_video"] = True
            product_item["video"] = [video_view]
        else:
            product_item["has_video"] = False
            product_item["video"] = []
        before_price = product_content.xpath('//div[@class="prices-add-to-cart-actions"]//span[@class="valuePrice"]/text()').get('').replace('(','').replace(')','').replace('Value','').strip()
        if before_price == '':
            before_price = product_content.xpath('//div[@class="prices-add-to-cart-actions"]//span[@class="strike-through list  "]//span[contains(text(),"Price reduced from")]/following::text()[1]').get('').strip()
        if before_price:
            product_item["price_before"] = before_price
            promo_label1 = product_item["price"].replace('€','')
            promo_label2 = before_price.replace('€','')
            promo_label = int(promo_label2) - int(promo_label1)
            product_item["promo_label"] = 'Save'+' '+'€'+ str(promo_label)    
        else:
            product_item["price_before"] = None
            product_item["promo_label"] = None
        return product_item

    headers = {
        "accept": "*/*",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
    }

    def start_requests(self):
        dir_path = os.path.abspath(__file__ + "/../../../")
        supporting_files = os.path.join(dir_path, "supporting_files")
        with open(f"{supporting_files}/terta_fr_input_urls.txt", "r") as f:
            data = f.readlines()
        for url in data:
            yield scrapy.Request(url.strip(), callback=self.parse)

    async def requests_process(self, url, headers):
        request = scrapy.Request(url, headers=headers)
        response = await self.crawler.engine.download(request, self)
        return response

    async def parse(self, response):
        item = {}
        rank = 0
        count_value = response.xpath('//span[@class="items-count"]/text()').get("")
        if "search/?" in response.url:
            if count_value:
                item["title"] = (response.xpath("//h1/text()").get("").replace("Results for", "").replace('"', "").replace('\n','').strip())
                item["count"] = int(re.findall("\d+", count_value)[0])
            else:
                item["title"] = (response.xpath('//span[@class="search-keywords"]/text()').get("").replace('\n','').replace('"', "").strip())
                item["count"] = 0
                item["products"] = []
        else:
            item["title"] = response.xpath("//h1/text()").get("").replace('\n','').strip()
            item["count"] = int(re.findall("\d+", count_value)[0])
        item["page_url"] = response.url
        breadcrumb_list = []
        for block in response.xpath('//div[@aria-label="Breadcrumb"]/ol/li/a')[1:]:
            breadcrumb_nag = {}
            breadcrumb_nag["name"] = block.xpath(".//text()").get("").strip()
            breadcrumb_nag["url"] = response.urljoin(block.xpath(".//@href").get(""))
            breadcrumb_list.append(breadcrumb_nag)
        if breadcrumb_list:
            item["category_crumb"] = breadcrumb_list
        product_link_list = []
        product_Link = response.xpath('//div[@itemid="#product"]/div/div[@class="product"]/div/a/@href').getall()
        product_link_list.append(product_Link)
        cgid = (response.xpath('//div[@class="page"]/@data-querystring').get("").replace("cgid=", ""))
        if item["count"] >= 12:
            for page_count in range(1, round(item["count"] / 12) + 1):
                page_change = str(page_count * 12)
                next_page_link = f"https://tartecosmetics.com/on/demandware.store/Sites-tarteRedesign-Site/en_FR/Search-UpdateGrid?cgid={cgid}&prefn1=excludedFromLocale&prefv1=false&start={page_change}&sz=12&selectedUrl=https%3A%2F%2Ftartecosmetics.com%2Fon%2Fdemandware.store%2FSites-tarteRedesign-Site%2Fen_FR%2FSearch-UpdateGrid%3Fcgid%3D{cgid}%26prefn1%3DexcludedFromLocale%26prefv1%3Dfalse%26start%3D12%26sz%3D12&loadMoreAjaxCall=true"
                next_page_url_category = await self.requests_process(next_page_link, headers=self.headers)
                product_link_collection_category = next_page_url_category.xpath('//div[@class="product-tile"]/a/@href').getall()
                product_link_list.append(product_link_collection_category)
        product_list_collection = [
            product_list_link
            for product_link in product_link_list
            for product_list_link in product_link
        ]
        product = []
        for rank, url in enumerate(product_list_collection, 1):
            product_url = await self.requests_process(response.urljoin(url), headers=self.headers)
            product_content = self.product_collectio(product_url, response.urljoin(url), rank)
            product.append(product_content)
        item["products"] = product
        yield item
