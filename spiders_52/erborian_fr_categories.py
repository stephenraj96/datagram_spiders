import scrapy
import datetime


class ErborianFrCategoriesSpider(scrapy.Spider):
    name = "erborian_fr_categories"
    """ save file in s3"""
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE = CURRENT_DATE.replace("-", "_")
    custom_settings = {
         "FEEDS": {
            f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/erborian_fr/%(name)s_{DATE}.json": {
                 "format": "json",
            }
        }
     }

    def start_requests(self):
        urls = ["https://fr.erborian.com/"]
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
        }
        for url in urls:
            yield scrapy.Request(
                url=url,
                headers=headers,
                callback=self.parse,
            )

    def parse(self, response):
        item = {}
        for block in response.xpath(
            '//ul[@class="o-level-3-container o-mobile-container"]/li/a'
        ):
            name = block.xpath(".//text()").get("")
            if "-" in name:
                item["name"] = name.replace("- tous les produits", "").strip()
            else:
                item["name"] = name.strip()
            item["url"] = block.xpath(".//@href").get()
            item["category_crumb"] = []
            yield item
