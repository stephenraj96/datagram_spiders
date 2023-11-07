import scrapy
import json
import os
import datetime
from dotenv import load_dotenv
from pathlib import Path

""" load env file """
try:
    load_dotenv()
except:
    env_path = Path(".env")
    load_dotenv(dotenv_path=env_path)

class TakaSpider(scrapy.Spider):
    name = "tadadrinks_products"
    CURRENT_DATETIME = datetime.datetime.now()
    CURRENT_DATE = CURRENT_DATETIME.strftime("%Y-%m-%d")
    DATE=CURRENT_DATE.replace("-","_")
    ROTATING_PROXY = os.getenv("ROTATING_PROXY_ZA")
    custom_settings = {
        "ROTATING_PROXY_LIST":[ROTATING_PROXY],
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
            body=payload
        )

    def session(self, response):
        app_token = json.loads(response.text).get("appToken", "")
        url = "https://prod-za.tadadelivery.io/graphql/gateway"
        self.headers_prod = {
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

        url = "https://za-tada.cdn.prismic.io/api/v2?access_token=MC5Zb0tKVUJNQUFDTUFhWWNn.dO-_vRtn77-9Uh4pZjtXeu-_vUfvv73vv73vv71dDu-_ve-_ve-_ve-_ve-_ve-_ve-_ve-_vWLvv70H77-9Sw"
        headers = {
            "Host": "za-tada.cdn.prismic.io",
            "Accept-Encoding": "gzip, deflate",
            "User-Agent": "okhttp/4.9.2",
            "Connection": "close",
        }
        yield scrapy.Request(
            url,
            method="GET",
            headers=headers,
            callback=self.session_ref
        )

    def session_ref(self, response):
        json_data = json.loads(response.text)
        if json_data.get("refs", []):
            json_data = json_data.get("refs", [])[0]
            ref = json_data.get("ref", "")
            self.headers_prismic = {
                "Host": "za-tada.cdn.prismic.io",
                "Prismic-Ref": ref,
                "Authorization": "Token MC5Zb0tKVUJNQUFDTUFhWWNn.dO-_vRtn77-9Uh4pZjtXeu-_vUfvv73vv73vv71dDu-_ve-_ve-_ve-_ve-_ve-_ve-_ve-_vWLvv70H77-9Sw",
                "Accept": "*/*",
                "Content-Type": "application/json",
                "Accept-Encoding": "gzip, deflate",
                "User-Agent": "okhttp/4.9.2",
            }
        url = f"https://za-tada.cdn.prismic.io/graphql?query=%7BproductCategories%3A+allProductCategorys%7Bedges%7Bnode%7Bcategories%7Bid+title+type+image+iscervezas+__typename%7D__typename%7D__typename%7D__typename%7D%7D&variables=%7B%7D&ref={ref}"
        yield scrapy.Request(
            url,
            method="GET",
            headers=self.headers_prismic,
            callback=self.parse,
            cb_kwargs={"ref": ref}
        )

    def parse(self, response, ref):
        json_data = json.loads(response.text)
        if json_data.get("data", {}).get("productCategories", {}).get("edges", {}):
            json_data = (
                json_data.get("data", {})
                .get("productCategories", {})
                .get("edges", {})[0]
            )
            for category in json_data.get("node", {}).get("categories", []):
                title = category.get("title", {})[0].get("text", "")
                category_id = category.get("id", {})[0].get("text", "").replace("/", "")
                if category.get("title", {})[0].get("text", "") == "Beers":
                    url = f"https://za-tada.cdn.prismic.io/graphql?query=%7BlogoCarousels%3A+allLogoCarousels%28where%3A%7Btype_fulltext%3A+%22cervezas%22%7D%29%7Bedges%7Bnode%7Btitle+content%7Bid+title+type+image+__typename%7D__typename%7D__typename%7D__typename%7D%7D&variables=%7B%7D&ref={ref}"
                    yield scrapy.Request(
                        url,
                        method="GET",
                        headers=self.headers_prismic,
                        callback=self.parse_beer,
                        cb_kwargs={"title": title, "category_id": category_id}
                    )
                elif title == "Coolers and Ciders":
                    url = "https://prod-za.tadadelivery.io/graphql/gateway"
                    payload = json.dumps(
                        {
                            "operationName": "getSellerProductListV2",
                            "variables": {
                                "data": {
                                    "collectionFilter": "144",
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
                        callback=self.parse_cool,
                        headers=self.headers_prod,
                        body=payload,
                        cb_kwargs={"title": title, "category_id": category_id}
                    )
                elif title == "Wine":
                    url = "https://prod-za.tadadelivery.io/graphql/gateway"
                    payload = json.dumps(
                        {
                            "operationName": "getCategoryByIdData",
                            "variables": {
                                "input": {
                                    "sellerId": "zafdtcrandburgkings",
                                    "categoryId": 4,
                                }
                            },
                            "query": "query getCategoryByIdData($input: GetCategoryByIdInput) {\n  categoryTree: getCategoryById(input: $input) {\n    id\n    name\n    slug\n    titleTag\n    hasChildren\n    children {\n      id\n      name\n      slug\n      titleTag\n      __typename\n    }\n    __typename\n  }\n}\n",
                        }
                    )
                    yield scrapy.Request(
                        url,
                        method="POST",
                        callback=self.parse_wine,
                        headers=self.headers_prod,
                        body=payload,
                        cb_kwargs={"title": title, "category_id": category_id},
                    )
                elif title == "Spirits":
                    url = "https://prod-za.tadadelivery.io/graphql/gateway"
                    payload = json.dumps(
                        {
                            "operationName": "getCategoryByIdData",
                            "variables": {
                                "input": {
                                    "sellerId": "zafdtcrandburgkings",
                                    "categoryId": 1,
                                }
                            },
                            "query": "query getCategoryByIdData($input: GetCategoryByIdInput) {\n  categoryTree: getCategoryById(input: $input) {\n    id\n    name\n    slug\n    titleTag\n    hasChildren\n    children {\n      id\n      name\n      slug\n      titleTag\n      __typename\n    }\n    __typename\n  }\n}\n",
                        }
                    )
                    yield scrapy.Request(
                        url,
                        method="POST",
                        callback=self.parse_wine,
                        headers=self.headers_prod,
                        body=payload,
                        cb_kwargs={"title": title, "category_id": category_id},
                    )
                elif title == "Soft Drinks":
                    url = "https://prod-za.tadadelivery.io/graphql/gateway"
                    payload = json.dumps(
                        {
                            "operationName": "getSellerProductListV2",
                            "variables": {
                                "data": {
                                    "categoryFilter": "/3/",
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
                        callback=self.parse_cool,
                        headers=self.headers_prod,
                        body=payload,
                        cb_kwargs={"title": title, "category_id": category_id},
                    )
                else:
                    url = "https://prod-za.tadadelivery.io/graphql/gateway"
                    payload = json.dumps(
                        {
                            "operationName": "getSellerProductListV2",
                            "variables": {
                                "data": {
                                    "collectionFilter": "138",
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
                        callback=self.parse_cool,
                        headers=self.headers_prod,
                        body=payload,
                        cb_kwargs={"title": title, "category_id": category_id},
                    )

    async def parse_beer(self, response, title, category_id):
        json_data = json.loads(response.text)
        if json_data.get("data", {}).get("logoCarousels", {}).get("edges", []):
            data = {}
            data["title"] = title
            data["category_id"] = category_id
            data["count"] = 0
            data["products"] = []
            rank = 0
            for main in (
                json_data.get("data", {}).get("logoCarousels", {}).get("edges", [])
            ):
                for cat in main.get("node", {}).get("content", []):
                    id = cat.get("id", {})[0].get("text", "")
                    url = "https://prod-za.tadadelivery.io/graphql/gateway"
                    payload = json.dumps(
                        {
                            "operationName": "getSellerProductListV2",
                            "variables": {
                                "data": {
                                    "brandFilter": id,
                                    "pagination": {"from": 0, "to": 50},
                                },
                                "stores": ["zafdtcrandburgkings"],
                            },
                            "query": "query getSellerProductListV2($data: productsFilterAndSorters, $stores: [ID!]) {\n  getSellerProductList(stores: $stores, data: $data, version: 5) {\n    products {\n      id\n      name\n      price\n      listPrice\n      isCombo\n      pricePerUnitMeasure\n      unitMeasure\n      images\n      sku\n      discount\n      stock\n      skuSpecifications {\n        field {\n          id\n          name\n          isActive\n          position\n          type\n          __typename\n        }\n        values {\n          id\n          name\n          position\n          __typename\n        }\n        __typename\n      }\n      variants {\n        id\n        name\n        images\n        discount\n        sku\n        listPrice\n        variations\n        pricePerUnitMeasure\n        nameComplete\n        price\n        stock\n        isReturnable\n        __typename\n      }\n      brand\n      brandId\n      categoryId\n      categoriesIds\n      categories\n      __typename\n    }\n    paginationInfo {\n      currentPage\n      haveNextPage\n      totalResults\n      __typename\n    }\n    __typename\n  }\n}\n",
                        }
                    )
                    request = scrapy.Request(
                        url, headers=self.headers_prod, method="POST", body=payload
                    )
                    response = await self.crawler.engine.download(request, self)
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
                    if data["count"] == 0:
                        data["count"] = int(count)
                    else:
                        data["count"] = data["count"] + int(count)
                    for count, block in enumerate(blocks, start=1):
                        rank = rank + 1
                        item = {}
                        item["name"] = block.get("name", "")
                        item["price"] = str(block.get("price", ""))
                        if block.get("images", []):
                            item["image_url"] = block.get("images", "")[0]
                        else:
                            item["image_url"] = ""
                        item["rank"] = str(rank)
                        item["currency"] = "R"
                        item["source"] = "ta da"
                        item["product_id"] = block.get("sku", "")
                        item["in-stock"] = True
                        if block.get("discount", 0) == 0:
                            item["price_before"] = None
                        else:
                            item["price_before"] = str(block.get("listPrice", 0))
                        item["categoryLabel"] = title
                        item["category_id"] = category_id
                        item["manufacturer"] = block.get("brand", "")
                        data["products"].append(item.copy())
            yield data

    def parse_cool(self, response, title, category_id):
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
        data["title"] = title
        data["category_id"] = category_id
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
            item["categoryLabel"] = title
            item["category_id"] = category_id

            item["manufacturer"] = block.get("brand", "")
            data["products"].append(item.copy())
        yield data

    async def parse_wine(self, response, title, category_id):
        json_data = json.loads(response.text)
        data = {}
        data["title"] = title
        data["category_id"] = category_id
        data["count"] = 0
        data["products"] = []
        rank = 0
        if title == "Wine":
            json_data_list = (
                json_data.get("data", {})
                .get("categoryTree", {})
                .get("children", [])[2:6]
            )
        else:
            json_data_list = (
                json_data.get("data", {}).get("categoryTree", {}).get("children", [])
            )
        for cat in json_data_list:
            second_category = f"/{title.lower()}/" + cat.get("slug", "")
            url = "https://prod-za.tadadelivery.io/graphql/gateway"
            payload = json.dumps(
                {
                    "operationName": "getSellerProductListV2",
                    "variables": {
                        "data": {
                            "categoryFilter": second_category,
                            "pagination": {"from": 0, "to": 50},
                        },
                        "stores": ["zafdtcrandburgkings"],
                    },
                    "query": "query getSellerProductListV2($data: productsFilterAndSorters, $stores: [ID!]) {\n  getSellerProductList(stores: $stores, data: $data, version: 5) {\n    products {\n      id\n      name\n      price\n      listPrice\n      isCombo\n      pricePerUnitMeasure\n      unitMeasure\n      images\n      sku\n      discount\n      stock\n      skuSpecifications {\n        field {\n          id\n          name\n          isActive\n          position\n          type\n          __typename\n        }\n        values {\n          id\n          name\n          position\n          __typename\n        }\n        __typename\n      }\n      variants {\n        id\n        name\n        images\n        discount\n        sku\n        listPrice\n        variations\n        pricePerUnitMeasure\n        nameComplete\n        price\n        stock\n        isReturnable\n        __typename\n      }\n      brand\n      brandId\n      categoryId\n      categoriesIds\n      categories\n      __typename\n    }\n    paginationInfo {\n      currentPage\n      haveNextPage\n      totalResults\n      __typename\n    }\n    __typename\n  }\n}\n",
                }
            )
            request = scrapy.Request(
                url, headers=self.headers_prod, method="POST", body=payload
            )
            response = await self.crawler.engine.download(request, self)
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
            if data["count"] == 0:
                data["count"] = int(count)
            else:
                data["count"] = data["count"] + int(count)
            for count, block in enumerate(blocks, start=1):
                rank = rank + 1
                item = {}
                item["name"] = block.get("name", "")
                item["price"] = str(block.get("price", ""))
                if block.get("images", []):
                    item["image_url"] = block.get("images", "")[0]
                else:
                    item["image_url"] = ""
                item["rank"] = str(rank)
                item["currency"] = "R"
                item["source"] = "ta da"
                item["product_id"] = block.get("sku", "")
                item["in-stock"] = True
                if block.get("discount", 0) == 0:
                    item["price_before"] = None
                else:
                    item["price_before"] = str(block.get("listPrice", 0))
                item["categoryLabel"] = title
                item["category_id"] = category_id
                item["manufacturer"] = block.get("brand", "")
                data["products"].append(item.copy())
        yield data