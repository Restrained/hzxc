#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/7 11:40
# @Author  : AllenWan
# @File    : article_info.py
# @Desc    ：
import base64
from typing import List, Dict

from bricks import const
from bricks.core.signals import Success, Failure
from bricks.db.mongo import Mongo
from bricks.db.redis_ import Redis
from bricks.lib.queues import RedisQueue
from bricks.plugins.storage import to_mongo
from bricks.spider import template
from bricks.spider.template import Config, Context
from bricks.utils.fake import user_agent
from bs4 import BeautifulSoup

from config.config_info import RedisConfig, MongoConfig
from db.mongo import MongoInfo
from events.spiders.magtech.modules.object import JournalParseRole, Achievement, Author, Publish, WorkAt, ClaimedBy


class ArticleInfo(template.Spider):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
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
                    func=self._init_seeds
                )
            ],
            download=[
                template.Download(
                    url='{article_url}',
                    headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                        'Cache-Control': 'max-age=0',
                        'Connection': 'keep-alive',
                        # 'Cookie': 'Hm_lvt_f26eea3c0883be4c444c00f18f6746c0=1731462846; JSESSIONID=B4457272BB5AEBA1EEF71689719FDE03; wkxt3_csrf_token=ff2b0186-232a-4d4e-bd60-2598950cfc71',
                        'Sec-Fetch-Dest': 'document',
                        'Sec-Fetch-Mode': 'navigate',
                        'Sec-Fetch-Site': 'none',
                        'Sec-Fetch-User': '?1',
                        'Upgrade-Insecure-Requests': '1',
                        'User-Agent': user_agent.chrome(),
                        'sec-ch-ua': '"Microsoft Edge";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"Windows"'
                    },
                    max_retry=2,
                    timeout=20,
                )
            ],
            parse=[
                template.Parse(
                    func=self._parse
                )
            ],
            pipeline=[
                template.Pipeline(
                    func=to_mongo,
                    kwargs={
                        "path": "achievement_info",
                        "conn": self.mongo
                    },
                    success=True
                )
            ],
            events={
                const.AFTER_REQUEST: [
                    template.Task(
                        func=self.is_success
                    )
                ]
            }

        )

    def _parse(self, context: Context):
        seeds = context.seeds
        response = context.response
        soup = BeautifulSoup(response.text, 'html.parser')
        achievement = Achievement()
        ach_role = JournalParseRole(achievement, seeds, response)
        achievement_list, author_list, institution_list, publish_list, claimed_by_list, work_at_list, venue_list, fund_list, supported_by_list, reference_list = ach_role.get_parse_info(soup)


        self.self_pipeline(author_list, "author_info")
        self.self_pipeline(institution_list, "institution_info")
        self.self_pipeline(publish_list, "publish")
        self.self_pipeline(claimed_by_list, "claimed_by")
        self.self_pipeline(work_at_list, "work_at")
        self.self_pipeline(venue_list, "venue")
        self.self_pipeline(fund_list, "fund")
        self.self_pipeline(supported_by_list, "supported_by")
        self.self_pipeline(reference_list, "reference_info")
        self.self_pipeline(author_list, "author_info")

        return achievement_list


    def self_pipeline(self, items: List[Dict], table_name, row_keys=None):
        # 写自己的存储逻辑
        to_mongo(
                items=items,
                path=table_name,
                conn=self.mongo,
                row_keys=row_keys,
        )



    def _init_seeds(self, context: Context):
        iter_data = self.mongo.batch_data(
            collection="article_list_magtech",
            query={},
            projection={
                "article_url": 1,
                "batch_id": 1,
                "journal_title": 1,
                "year": 1,
                "issue": 1
            }
        )
        return iter_data

    def is_success(self, context: template.Context):
        seeds = context.seeds
        response = context.response

        # if  response.status_code == 404:
        #     if seeds['article_url'].startswith('https://'):
        #         context.submit({**context.seeds, "article_url": seeds['article_url'].replace("https", "http")})
        #     raise Success
        # elif response.status_code == 403 or not response.content:
        #     raise Success
        #
        # soup = BeautifulSoup(response.text, 'lxml')
        # # 获取 <title> 标签的文本内容
        # title = soup.title.string
        # if title == "Error 404":
        #     raise Success

        if any([
            'DC.Title' in response.text,
            'apple-mobile-web-app-title' in response.text,
        ]):
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

    spider = ArticleInfo(
        concurrency=100,
        **{"init.queue.size": 1000000},
        # downloader=requests_.Downloader(),
        task_queue=RedisQueue(
            host=RedisConfig.host,
            port=RedisConfig.port,
            password=base64.b64decode(RedisConfig.password).decode("utf-8"),
            database=RedisConfig.database,
        ),
        # proxy=proxy
    )

    spider.run(task_name='spider')
    # spider.survey({"_id": "675685eb2b58475b0e7deeed",
    #                "article_url": "https://www.xdhg.com.cn/CN/Y2004/V24/I13/0",
    #                "batch_id": "2024-12-01", "journal_title": "食品科学",
    #                "year": "2024", "volume": "11", "issue": "6"
    #                })
