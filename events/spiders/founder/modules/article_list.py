#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/19 16:56
# @Author  : AllenWan
# @File    : article_list.py
# @Desc    ：
import base64
from http.client import responses
from typing import List

from bricks import const
from bricks.core import signals
from bricks.core.signals import Success
from bricks.db.redis_ import Redis
from bricks.lib.queues import RedisQueue
from bricks.plugins import scripts
from bricks.plugins.storage import to_mongo
from bricks.spider import template
from bricks.spider.template import Config, Context
from jinja2.lexer import Failure

from config.config_info import RedisConfig, MongoConfig
from db.mongo import MongoInfo


class ArticleList(template.Spider):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 种子、存储配置定义
        self.redis = Redis(
            host=RedisConfig.host,
            port=RedisConfig.port,
            password=base64.b64decode(RedisConfig.password).decode("utf-8"),
            database=RedisConfig.database,
        )
        self.mongo = MongoInfo(
            host=MongoConfig.host,
            port=MongoConfig.port,
            username=base64.b64decode(MongoConfig.username).decode("utf-8"),
            password=base64.b64decode(MongoConfig.password).decode("utf-8"),
            database=MongoConfig.database,
            auth_database=MongoConfig.auth_database
        )

    @property
    def config(self) -> Config:
        return Config(
            init=[
                template.Init(
                    func=self._init_seeds,
                ),

            ],
            download=[
                template.Download(
                    url="https://zgtl.crj.com.cn/rc-pub/front/front-article/getArticlesByPeriodicalIdGroupByColumn/{archive_id}",
                    params={
                        "showCover": "false",
                        "timestamps": "1734579551931"
                    },
                    headers={
                        'accept': 'application/json, text/plain, */*',
                        'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                        'cookie': '_pk_testcookie..undefined=1; _pk_testcookie.1.1ef0=1; _pk_ses.1.1ef0=1; language=zh; _pk_id.1.1ef0=ed67df978575e6a0.1734578604.1.1734579552.1734578604.',
                        'language': 'zh',
                        'priority': 'u=1, i',
                        'referer': 'https://zgtl.crj.com.cn/academic?columnId=3145&code=latestArticle&activeName=176880&type=journal&index=2&year=2024&date=1734579548515&lang=zh',
                        'sec-ch-ua': '"Microsoft Edge";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"Windows"',
                        'sec-fetch-dest': 'empty',
                        'sec-fetch-mode': 'cors',
                        'sec-fetch-site': 'same-origin',
                        # 'siteid': '12',
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0'
                    },
                    ok={"response.status_code == 500": signals.Pass}
                )
            ],
            parse=[
                template.Parse(
                    func=self._parse,

                )
            ],
            pipeline=[
                template.Pipeline(
                    func=to_mongo,
                    kwargs={
                        "path": "article_list_founder",
                        "conn": self.mongo
                    },
                    success=True
                )
            ],
            events={
                const.AFTER_REQUEST: [
                    template.Task(
                        func=self.is_success,

                    )
                ],
            }
        )

    def _parse(self, context: Context) -> List[dict]:
        result = []
        response = context.response
        data_list = response.get("csv_data")
        for data in data_list:
            content = data.get("content")
            for item in content:
                item["article_id"] = item.pop("id")
                del item["attach"]  #
                result.append(item)
        return result

    def _init_seeds(self):

        iter_data = self.mongo.batch_data(
            collection="archive_viewing_founder",
            query={},
            projection={
                "archive_id": 1,
            }
        )
        return iter_data

    def is_success(self, context: Context):
        response = context.response
        if response.status_code == 500:
            raise Success
        elif response.get("message") == "操作成功":
            return True

        raise Failure


if __name__ == '__main__':
    proxy = {
        'ref': "bricks.lib.proxies.RedisProxy",
        'key': 'proxy_21',
        'options': {'host': '1.14.193.46', 'password': 'sandalwood_SWA_2021-06-01!'},
        "threshold": 10,
        # "scheme": "socks5"

        # 'ref': "bricks.lib.proxies.CustomProxy",
        # # 'key': "115.223.31.91:34428",
        # 'key': "127.0.0.1:7897",
        # # "threshold": 10,
        # # "scheme": "socks5"
    }

    spider = ArticleList(
        concurrency=20,
        # downloader=requests_.Downloader(),
        **{"init.queue.size": 5000000},
        task_queue=RedisQueue(
            host=RedisConfig.host,
            port=RedisConfig.port,
            password=base64.b64decode(RedisConfig.password).decode("utf-8"),
            database=RedisConfig.database,
        ),
        proxy=proxy
    )
    spider.run(task_name='spider')
    # spider.survey({
    #     "archive_id": 23746,
    # })
