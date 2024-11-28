#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/22 09:11
# @Author  : AllenWan
# @File    : citation.py

from bricks import const
from bricks.core.signals import Success, Failure
from bricks.db.mongo import Mongo
from bricks.downloader import requests_
from bricks.lib.queues import RedisQueue
from bricks.plugins import scripts
from bricks.plugins.make_seeds import by_csv
from bricks.plugins.storage import to_mongo
from bricks.spider import template
from bricks.db.redis_ import Redis

from redis import Redis
from bricks.spider.template import Config, Context


class Citation(template.Spider):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.redis = Redis()
        self.mongo = Mongo(
            database="modules",
        )

    @property
    def config(self) -> Config:
        return Config(
            init=[
                template.Init(
                    func=by_csv,
                    kwargs={
                        "path": r"D:\pyProject\hzcx\renHeHuiZhi\chineseoptics\seeds\citation_seeds.csv",
                        "query": "select article_id, doi, journal_title, issn, domain as officialWebsite from <TABLE>",
                        "batch_size": 5000
                    }
                )
            ],
            download=[
                template.Download(
                    url="{officialWebsite}/article/getCitationStr",
                    method="POST",
                    body="ids={article_id}&fileType=2&pageType=cn",
                    headers={
                        'Accept': '*/*',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                        'Connection': 'keep-alive',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0',
                        'X-Requested-With': 'XMLHttpRequest',
                        'Cookie': 'JSESSIONID=E084838449D8F088949104AE8A69AD8D'
                    },
                    timeout=20,
                    max_retry=2
                )
            ],
            parse=[
                template.Parse(
                    func="json",
                    kwargs={
                        "rules": {
                            "citation_cn": "citationCn",
                            "citation_en": "citationEn",
                        }
                    },
                    layout=template.Layout(
                        default={
                            "article_id": "{article_id}",
                            "doi": "{doi}",
                            "journal_title": "{journal_title}",
                            "issn": "{issn}",
                        }
                    )
                )
            ],
            pipeline=[
                template.Pipeline(
                    func=to_mongo,
                    kwargs={
                        "path": "citation_info_v2",
                        "conn": self.mongo,
                    },
                    success=True
                )
            ],
            events={
                const.AFTER_REQUEST: [
                    template.Task(
                        func=self.is_success,
                        kwargs={
                            "match": [
                                "context.response.get('citationCn')"
                            ]
                        }
                    )
                ]
            }
        )

    def is_success(self, context: Context):
        if context.response.get('citationCn'):
            return True
        elif "citationCn" in context.response.text and context.response.get('citationCn') == "":
            raise Success
        else:
            raise Failure
if __name__ == "__main__":
    spider = Citation(
        concurrency=1,
        **{"init.queue.size": 1000000},
        download=requests_.Downloader,
        task_queue=RedisQueue()
    )

    spider.run(task_name='spider')
    # spider.survey({'article_id': '5fa4fb60f4d791802438c1ef', 'doi': '10.3969/j.issn.1003-501X.2017.01.011', 'issn': '1003-501X', 'journal_title': '光电工程', 'officialWebsite': 'https://cn.oejournal.org', '$config': 0})