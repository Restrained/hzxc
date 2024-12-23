#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/19 11:57
# @Author  : AllenWan
# @File    : journal_info.py
# @Desc    ：
import base64

from bricks import const
from bricks.core import signals
from bricks.core.signals import Success
from bricks.db.redis_ import Redis
from bricks.lib.queues import RedisQueue
from bricks.plugins import scripts
from bricks.plugins.storage import to_mongo
from bricks.spider import template
from bricks.spider.template import Config, Context
from celery.app.trace import SUCCESS
from jinja2.lexer import Failure

from config.config_info import RedisConfig, MongoConfig
from db.mongo import MongoInfo


class JournalInfo(template.Spider):
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

                )
            ],
            download=[
                template.Download(
                    url="https://steelpro.tongji.edu.cn/rc-pub/front/front-period/getFirstTwoPeriods",
                    params={
                        "publicationIndexId": "{publication_id}",
                        "timestamps": "17345778233889"
                    },
                    headers={
                        'accept': 'application/json, text/plain, */*',
                        'accept-language': 'zh-CN,zh;q=0.9',
                        'cookie': 'language=zh; _pk_testcookie..undefined=1; _pk_testcookie.1.6ce4=1; _pk_ses.1.6ce4=1; _pk_id.1.6ce4=5bbbfc8db5c878f0.1734578019.1.1734578020.1734578019.',
                        'language': 'zh',
                        'priority': 'u=1, i',
                        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"Windows"',
                        'sec-fetch-dest': 'empty',
                        'sec-fetch-mode': 'cors',
                        'sec-fetch-site': 'same-origin',
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
                    },
                    ok={"response.status_code == 500": signals.Pass}
                )
            ],
            parse=[
                template.Parse(
                    func="json",
                    kwargs={
                        "rules": {
                            "csv_data[0]": {
                                "site_id": "siteId",
                                "journal_name": "publicationName",
                                "publication_index_id": "publicationIndexId",
                                "rc_period_lib_id": "rcPeriodLibId",

                            }
                        }

                    }
                )
            ],
            pipeline=[
                template.Pipeline(
                    func=to_mongo,
                    kwargs={
                        "path": "journal_info_founder",
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
                ]
            }
        )

    def _init_seeds(self):

        seeds = []
        for publication_id in range(1, 2500):
            seeds.append(
                {
                    "publication_id": publication_id,
                }
            )
        return seeds

    def is_success(self, context: Context):
        response = context.response
        if response.status_code==500 :
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

    spider = JournalInfo(
        concurrency=10,
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
    #     "publication_id": 8
    # })