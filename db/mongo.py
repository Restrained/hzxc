#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/27 10:05
# @Author  : AllenWan
# @File    : mongo.py
import base64
from typing import Optional, List, Iterable

from bricks.db.mongo import Mongo
from pymongo import MongoClient

from config.config_info import MongoConfig


class MetaClient:
    host = MongoConfig.host
    port = MongoConfig.port
    auth_database = MongoConfig.auth_database
    username = base64.b64decode(MongoConfig.username).decode("utf-8")
    password = base64.b64decode(MongoConfig.password).decode("utf-8")
    client  = MongoClient(f"mongodb://{username}:{password}@{host}:{port}/{auth_database}")



class MongoInfo(Mongo):
    """

    Mongo 工具类

    """
    def __init__(
            self,
            host="127.0.0.1",
            username=None,
            password=None,
            port=27017,
            database='admin',
            auth_database="admin",
    ):
        """

        :param host:
        :param username:
        :param password:
        :param port:
        :param database:
        :param auth_database:
        """
        # 创建uri，用于父类初始化
        auth_database = auth_database or 'admin'

        self.database = database
        self._db = None
        self._caches = set()
        super().__init__(host, username, password,port, database, auth_database)


    def batch_data(self,
                   collection: str,
                   query:Optional[dict] = None,
                   projection:Optional[dict] = None,
                   database: str = None,
                   group: Optional[dict] = None,
                   sort:Optional[list[tuple]] = None,
                   skip: int = 0,
                   count: int = 1000
                   ) -> Iterable[List[dict]]:
        database = database if database else self.database

        # 确定排序规则
        sort_condition = {"_id": 1}
        if sort:
            sort_condition.update({k:v for k,v in sort})

        # 获取创建要跳过数据量前一条的_id
        last_id = None
        if skip:
            skip_result = self[database][collection].find_one(
                filter=query,
                skip=skip-1,
                sort=list(sort_condition.items()))
            if not skip_result:
                return
            last_id = skip_result["_id"]

        while True:
            # 利用聚合操作查询，利用生成器逐段返回
            _pipeline: list[dict] = []
            query and _pipeline.append({"$match": query})
            group and _pipeline.append({"$group": group})
            sort_condition and _pipeline.append({"$sort": sort_condition})
            projection and _pipeline.append({"$project": projection})
            last_id and _pipeline.append({"$match": {"_id": {"$gt": last_id}}})
            _pipeline.append({"$limit": count})

            data: List[dict] = list(self[database][collection].aggregate(_pipeline, allowDiskUse=True))

            if not data:
                return

            last_id = data[-1]["_id"]
            yield data


# if __name__ == "__main__":
#     mongo = Mongo(
#         host="192.168.0.41",
#         port=37017,
#         username="spider",
#         password="<password>",
#         database='spider',
#         auth_database='spider'
#     )
#
#     iterator_result = mongo.batch_data(
#         collection='journal_info',
#         query={},
#         projection={"journal_title": 1, "domain": 1},
#         database='spider',
#         skip=0,
#         count=10
#     )
#
#     for item in iterator_result:
#         print(item)