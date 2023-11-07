import scrapy
import json
import datetime
import os
from dotenv import load_dotenv
from pathlib import Path

""" load env file """
try:
    load_dotenv()
except:
    env_path = Path(".env")
    load_dotenv(dotenv_path=env_path)


class TakaSpider(scrapy.Spider):
    name = "tadadrinks_keywords"

    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    ROTATING_PROXY = os.getenv("ROTATING_PROXY_ZA")
    custom_settings = {
        "ROTATING_PROXY":[ROTATING_PROXY],
        "CONCURRENT_REQUESTS":1,
        'DOWNLOAD_DELAY':3,
        "FEEDS": {f"s3://scraping-external-feeds-lapis-data/{CURRENT_DATE}/tadadrinks/%(name)s_{DATE}.json": {
        "format": "json",
                }
            }
        }

    def start_requests(self):
        url = "https://prod-za.tadadelivery.io/auth/token-generator/"

        payload = json.dumps({"client": "android", "version": "28"})
        headers = {
            "Host": "prod-za.tadadelivery.io",
            "Accept": "application/json, text/plain, */*",
            "X-Api-Key": "W9cgXyfAaiacmRT5rZ9TAaOA8McAq9R047vXn6qg",
            "Content-Type": "application/json",
            "Accept-Encoding": "gzip, deflate",
            "User-Agent": "okhttp/4.9.2",
        }
        yield scrapy.Request(
            url,
            method="POST",
            callback=self.session,
            headers=headers,
            body=payload,
        )

    def session(self, response):
        app_token = json.loads(response.text).get("appToken", "")
        url = "https://prod-za.tadadelivery.io/graphql/gateway"
        self.headers = {
            "Host": "prod-za.tadadelivery.io",
            "Accept": "*/*",
            "Apollographql-Client-Name": "Android",
            "Apollographql-Client-Version": "4.8.0.13",
            "Authorization": "undefined",
            "Apptoken": app_token,
            "Content-Type": "application/json",
            "Accept-Encoding": "gzip, deflate",
            "User-Agent": "okhttp/4.9.2",
        }
        payload = json.dumps(
            {
                "operationName": "getSellersSesionData",
                "variables": {
                    "location": {"latitude": -25.9978662, "longitude": 27.9861488}
                },
                "query": "query getSellersSesionData($location: Coordinates!) {\n  getSellersAndSegmentByLocation(location: $location) {\n    stores\n    segment\n    countryCode\n    cultureInfo\n    currencyCode\n    currencySymbol\n    __typename\n  }\n}\n",
            }
        )
        yield scrapy.Request(
            url,
            method="POST",
            callback=self.parse,
            headers=self.headers,
            body=payload
        )

    def parse(self, response):
        url = "https://prod-za.tadadelivery.io/graphql/gateway"
        json_data = json.loads(response.text)
        stores = (
            json_data.get("data", {})
            .get("getSellersAndSegmentByLocation", {})
            .get("stores", [])
        )
        dir_path=os.path.abspath(__file__ + "/../../../")
        supporting_files=os.path.join(dir_path,"supporting_files")
        for store in stores:
            with open(f'{supporting_files}/tada_keywords.txt') as f:
                lines = f.readlines()
            for line in lines:
                word = line.strip()
                payload = json.dumps(
                    {
                        "operationName": "getSellerProductListV2",
                        "variables": {
                            "data": {
                                "nameFilter": word,
                                "pagination": {"from": 0, "to": 50},
                            },
                            "stores": ["zafdtcrandburgkings"],
                        },
                        "query": "query getSellerProductListV2($data: productsFilterAndSorters, $stores: [ID!]) {\n  getSellerProductList(stores: $stores, data: $data, version: 5) {\n    products {\n      id\n      name\n      price\n      listPrice\n      isCombo\n      pricePerUnitMeasure\n      unitMeasure\n      images\n      sku\n      discount\n      stock\n      skuSpecifications {\n        field {\n          id\n          name\n          isActive\n          position\n          type\n          __typename\n        }\n        values {\n          id\n          name\n          position\n          __typename\n        }\n        __typename\n      }\n      variants {\n        id\n        name\n        images\n        discount\n        sku\n        listPrice\n        variations\n        pricePerUnitMeasure\n        nameComplete\n        price\n        stock\n        isReturnable\n        __typename\n      }\n      brand\n      brandId\n      categoryId\n      categoriesIds\n      categories\n      __typename\n    }\n    paginationInfo {\n      currentPage\n      haveNextPage\n      totalResults\n      __typename\n    }\n    __typename\n  }\n}\n",
                    }
                )
                yield scrapy.Request(
                    url,
                    method="POST",
                    callback=self.parse_1,
                    headers=self.headers,
                    body=payload,
                    cb_kwargs={"keyword": word},
                )

    def parse_1(self, response, keyword=""):
        json_data = json.loads(response.text)
        blocks = (
            json_data.get("data", {})
            .get("getSellerProductList", {})
            .get("products", [])
        )
        count = (
            json_data.get("data", {})
            .get("getSellerProductList", {})
            .get("paginationInfo", {})
            .get("totalResults", "")
        )
        data = {}
        data["keyword"] = keyword
        data["count"] = count
        data["products"] = []
        for count, block in enumerate(blocks, start=1):
            item = {}
            item["name"] = block.get("name", "")
            item["price"] = str(block.get("price", ""))
            if block.get("images", []):
                item["image_url"] = block.get("images", "")[0]
            else:
                item["image_url"] = ""
            item["rank"] = str(count)
            item["currency"] = "R"
            item["source"] = "ta da"
            item["product_id"] = block.get("sku", "")
            item["in-stock"] = True
            if block.get("discount", 0) == 0:
                item["price_before"] = None
            else:
                item["price_before"] = str(block.get("listPrice", 0))
            if [i.replace("/", "") for i in block.get("categories", [])]:
                item["categoryLabel"] = (
                    [i.replace("/", "") for i in block.get("categories", [])[:1]][0]
                    .replace("-", " ")
                    .strip()
                )
            else:
                item["categoryLabel"] = ""
            item["category_id"] = block.get("categoryId", "")
            item["manufacturer"] = block.get("brand", "")
            data["products"].append(item.copy())
        yield data
