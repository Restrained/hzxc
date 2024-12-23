#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/16 14:55
# @Author  : AllenWan
# @File    : data_output.py
# @Desc    ：
import base64
import csv
import time
from typing import List

import loguru
from pymongo import MongoClient, collection


from config.config_info import MongoConfig

class DataOutput:
    def __init__(self):
        self.mongo_client = MongoClient(
            "mongodb://" + base64.b64decode(MongoConfig.username).decode("utf-8") + ":" + base64.b64decode(
                MongoConfig.password).decode("utf-8") + "@" + MongoConfig.host + ":" + str(
                MongoConfig.port) + "/" + str(MongoConfig.auth_database))
        self._collection = ''
        self._database = ''
        self.output_file = ''

    @property
    def database(self):
        # 1. 连接 MongoDB
        return self._database

    @database.setter
    def database(self, value):
        self._database = self.mongo_client[value]

    @property
    def collection(self) -> collection:
        return self._collection

    @collection.setter
    def collection(self, value):
        self._collection = self.database[value]

    def find(self, query: dict, projection:dict) -> List[dict]:
        documents = self.collection.find(query, projection=projection)
        processed_data = self.type_conversion(documents)
        return processed_data

    @staticmethod
    def type_conversion(documents):
        # 转换列表字段
        processed_data = []
        for doc in documents:
            if "values" in doc and isinstance(doc["values"], list):
                doc["values"] = ",".join(map(str, doc["values"]))  # 将列表转为字符串
            processed_data.append(doc)
        return processed_data

    @staticmethod
    def save_csv(data: List, output_file):
        # 导出到 CSV
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

class TableColumns:
    article_info = [
        "article_id",
        "article_title",
        "en_article_title",
        "doi",
        "authors",
        "source",
        "abstracts",
        "en_abstracts",
        "keywords",
        "en_keywords",
        "lang",
        "status",
        "type",
        "publish_date"
    ]
    author_info = [
        "author_id",
        "author_name",
        "en_author_name",
        "email",
        "about_author",
        "orc_id"
    ]
    fund_info = [
        "fund_id",
        "fund_name",
        "en_name",
    ]
    institution_info = [
        "id",
        "institution_id",
        "aff_name",
        "en_aff_name",
        "location",
    ]
    venue_info = [   
        "journal_id",
        "journal_title",
        "en_name",
        "type"
    ]
    publish = [
        "author_id",
        "achievement_id",
        "order_of_authors",
        "corresponding_author",
    ]
    published_in = [
        "achievement_id",
        "journal_id",
        "year",
        "volume",
        "period",
        "page_range",
    ]
    claimed_by = [
        "achievement_id",
        "institution_id",
    ]
    supported_by =  [ 
        "achievement_id",
        "fund_id",
    ]
    work_at = [
        "author_id",
        "institution_id",
    ]
    
    
def main():
    dout = DataOutput()
    dout.database = "spider"

    table_info_list = [
        {"table_name": "founder_article_info", "columns": TableColumns.article_info},
        {"table_name": "founder_author_info", "columns": TableColumns.author_info},
        {"table_name": "founder_fund_info", "columns": TableColumns.fund_info},
        {"table_name": "founder_institution_info", "columns": TableColumns.institution_info},
        {"table_name": "founder_venue", "columns": TableColumns.venue_info},
        {"table_name": "founder_publish", "columns": TableColumns.publish},
        {"table_name": "founder_published_in", "columns": TableColumns.published_in},
        {"table_name": "founder_claimed_by", "columns": TableColumns.claimed_by},
        {"table_name": "founder_supported_by", "columns": TableColumns.supported_by},
        {"table_name": "founder_work_at", "columns": TableColumns.work_at},
    ]

    count = 1
    for table_info in table_info_list:
        start_time = time.time()
        dout.collection = table_info["table_name"]
        query = {}
        projection = {item: 1 for item in table_info["columns"]}
        processed_data = dout.find(query, projection=projection)
        dout.save_csv(processed_data, f"{table_info['table_name']}.csv")
        finish_time = time.time()
        loguru.logger.info(f"{count}. 表{table_info['table_name']}导出完成，耗时{finish_time - start_time:.2f}s")







if __name__ == '__main__':
    main()
