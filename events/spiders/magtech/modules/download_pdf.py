#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/20 17:42
# @Author  : AllenWan
# @File    : download_pdf.py
#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@Project ：ShenyangProject
@File    ：download.py
@IDE     ：PyCharm
@Author  ：Allen.Wan
@Date    ：2024/8/21 上午10:33
@explain : 文件说明
"""
import base64
import os

from bricks import Request, const, Response
from bricks.core import signals
from bricks.core.signals import Success, Failure
from bricks.db.mongo import Mongo
from bricks.db.redis_ import Redis
from bricks.downloader import requests_
from bricks.lib.queues import RedisQueue
from bricks.plugins import scripts
from bricks.plugins.make_seeds import by_redis, by_csv, by_mongo
from bricks.plugins.storage import to_csv, to_mongo
from bricks.spider import air, template
from bricks.core.context import Context
from bricks.spider.template import Config
from urllib.parse import unquote, urlparse

from bricks.utils.fake import user_agent
from bs4 import BeautifulSoup
from loguru import logger

from config.config_info import RedisConfig, MongoConfig
from db.mongo import MongoInfo


class DownloadPDF(template.Spider):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 需自行配置 redis
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
                    url="{pdf_link}",
                    timeout=600,
                    max_retry=2,
                    headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                        'Cache-Control': 'max-age=0',
                        'Proxy-Connection': 'keep-alive',
                        'Upgrade-Insecure-Requests': '1',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0',

                    },

                ),

            ],
            parse=[
                template.Parse(
                    func=self._parse,
                ),

            ],

            pipeline=[
                template.Pipeline(
                    func=to_mongo,
                    kwargs={
                        "path": "download_pdf_log_magtech",
                        "conn": self.mongo,

                    },
                    success=True
                )
            ],
            events={
                const.AFTER_REQUEST: [

                    template.Task(
                        func=self.storage_file,

                    )
                ],

            }
        )

    def storage_file(self, context: template.Context):
        def calculate_size_in_kb(content):
            """
            计算下载内容的实际大小，并以 KB 为单位返回字符串格式。

            :param content: bytes，下载的内容
            :return: str，格式化的大小字符串，如 "12.34 KB"
            """
            size_in_kb = len(content) / 1024  # 将字节大小转换为 KB
            return f"{size_in_kb:.2f} KB"

        save_dir = r"D:\NAS盘\期刊文献"
        seeds = context.seeds
        response = context.response
        directory = os.path.join(save_dir, seeds['publishers'], str(seeds['year']), str(seeds['period']))

        if not response.content:
            raise Success

        # 确保目录存在
        os.makedirs(directory, exist_ok=True)  # 如果目录不存在会创建，不会抛出异常

        filename = f"{seeds['achievement_id']}.pdf"

        file_size = calculate_size_in_kb(response.content)

        save_path = os.path.join(directory, filename)
        seeds['save_path'] = save_path
        seeds['file_size'] = file_size

        # 将内容写入到本地文件
        with open(save_path, 'wb') as file:
            if not response.content:
                raise Failure()
            file.write(response.content)
        print(f"PDF 已保存到 {save_path}")


    def _parse(self, context: template.Context):
        seeds = context.seeds
        result = [
            seeds
        ]
        return result

    def _init_seeds(self, context: Context):
        result_table_id_list = [doc["achievement_id"]  for batch_list in self.mongo.batch_data(
            collection="download_pdf_log_magtech",
            query={},
            projection={
                "achievement_id": 1,
            }
        ) for doc in batch_list]

        iter_data = self.mongo.batch_data(
            collection="achievement_info",
            query={
                "achievement_id": {"$nin": result_table_id_list}
            },

            projection={
                "achievement_id": 1,
                "title": 1,
                "doi": 1,
                "year": 1,
                "period": 1,
                "publishers": 1,
                "batch_id": 1,
                "pdf_link": 1,
            }
        )
        return iter_data


if __name__ == '__main__':
    # proxy = {
    #     'ref': "bricks.lib.proxies.RedisProxy",
    #     'key': 'proxy_set',
    #
    # }

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
    #
    # spider = Comment(
    #     concurrency=50,
    #     downloader=requests_.Downloader(),
    #     task_queue=RedisQueue(),
    #     # proxy=proxy,
    # )
    #

    spider = DownloadPDF(
        concurrency=100,
        # downloader=requests_.Downloader(),
        **{"init.queue.size": 5000000},
        task_queue=RedisQueue(
            host=RedisConfig.host,
            port=RedisConfig.port,
            password=base64.b64decode(RedisConfig.password).decode("utf-8"),
            database=RedisConfig.database,
        ),
        # proxy=proxy
    )
    spider.run(task_name='spider')
    # spider.survey({'article_id': '20140710', 'doi': '10.13224/j.cnki.jasp.2014.07.010', 'issue': '7', 'journal_abbrev': 'hkdlxb', 'journal_title': '煤炭学报', 'pdf_link': 'https://www.mtxb.com.cn/article/exportPdf?id=87fb43ac-239d-4e1b-af05-bed01063b2d1', 'title': '基于Taguchi方法的转子系统动力学容差设计', 'year': '2014', '$config': 0})
