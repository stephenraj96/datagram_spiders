import scrapy
import datetime as currentdatetime


class TartecosmeticsFrCategoriesSpider(scrapy.Spider):
    name = "tartecosmetics_fr_categories"
    allowed_domains = ["tartecosmetics_fr_categories.com"]
    CURRENT_DATETIME = currentdatetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE = CURRENT_DATE.replace("-", "_")
    custom_settings = {
        "FEEDS": {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/tartecosmetics_fr/%(name)s_{DATE}.json": {"format": "json",}},
    }

    start_urls = ["https://tartecosmetics.com/EU/en_FR/home/"]

    def parse(self, response):

        item = {}
        item["name"] = response.xpath('//a[@id="makeup"]/text()').get("").strip()
        item["url"] = response.urljoin(
            response.xpath('//a[@id="makeup"]/@href').get("")
        )
        category_first = response.xpath(
            '//a[@data-category="Makeup"]/parent::li/div/div/div/ul/li'
        )[1:]
        make_category_name_url_dict = {}
        for category_name in category_first:
            name = category_name.css("a::text").get().strip().lower()
            url = category_name.css("a::attr(href)").get().strip()
            make_category_name_url_dict[name] = url
        makeup_category_crumb = {}
        make_category_crumb_list = []
        for category in response.xpath(
            '//a[@data-category="Makeup"]/parent::li/div/div/div/ul/li/ul/li'
        ):
            product_name = category.css("a::text").get().strip()
            product_url = category.css("a::attr(href)").get().strip()
            sub_category_names = product_url.replace("/EU/en_FR/makeup/", "").split(
                "/"
            )[0]
            if "https:" in product_url:
                sub_category_name = product_url.replace(
                    "https://tartecosmetics.com/EU/en_FR/", ""
                ).split("/")[0]
            else:
                sub_category_name = sub_category_names
            product = {}
            product["name"] = product_name
            product["url"] = response.urljoin(product_url)
            if sub_category_name in makeup_category_crumb.keys():
                makeup_category_crumb[sub_category_name].append(product)
            else:
                makeup_category_crumb[sub_category_name] = [product]
        for sub_categories_keys in make_category_name_url_dict.keys():
            sub_categories_dict = {}
            sub_categories_dict["name"] = sub_categories_keys
            sub_categories_dict["url"] = response.urljoin(
                make_category_name_url_dict.get(sub_categories_keys)
            )
            category_crumb = makeup_category_crumb.get(sub_categories_keys)
            if category_crumb:
                sub_categories_dict["category_crumb"] = category_crumb
            make_category_crumb_list.append(sub_categories_dict)
        item["category_crumb"] = make_category_crumb_list
        yield item
        skincare_item = {}
        skincare_item["name"] = (
            response.xpath('//a[@id="skincare"]/text()').get("").strip()
        )
        skincare_item["url"] = response.urljoin(
            response.xpath('//a[@id="skincare"]/@href').get("")
        )
        skincare_category_name_url_dict = {}
        make_category_crumb_list = []
        for category_name in response.xpath(
            '//a[@id="skincare"]/parent::li/div/div/div/ul/li'
        ):
            name = category_name.css("a::text").get("")
            if "shop all skincare" not in name and "awake" not in name:
                name = category_name.css("a::text").get("").strip()
                url = category_name.css("a::attr(href)").get().strip()
                skincare_category_name_url_dict[name] = response.urljoin(url)
        for sub_categories_keys in skincare_category_name_url_dict.keys():
            sub_categories_dict = {}
            sub_categories_dict["name"] = sub_categories_keys
            sub_categories_dict["url"] = skincare_category_name_url_dict.get(
                sub_categories_keys
            )
            make_category_crumb_list.append(sub_categories_dict)
        skincare_item["category_crumb"] = make_category_crumb_list
        yield skincare_item
