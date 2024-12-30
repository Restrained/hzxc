#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/6 09:16
# @Author  : AllenWan
# @File    : article_incremental.py
# @Desc    ：

import base64
import re
import time
from collections import defaultdict

from typing import List

import loguru
from bricks import const
from bricks.core import signals
from bricks.core.signals import Success, Failure
from bricks.db.redis_ import Redis
from bricks.downloader import requests_
from bricks.lib.queues import RedisQueue

from bricks.plugins.storage import to_mongo
from bricks.spider import template
from bricks.spider.template import Config, Context
from bs4 import BeautifulSoup

from config.config_info import RedisConfig, MongoConfig, SPECIAL_JOURNAL_LIST
from db.mongo import MongoInfo

from utils.batch_info import BatchProcessor


class ArticleIncrementalCrawler(template.Spider):
    """
    该模块旨在爬取期刊的当期列表，解析出文章列表的url，以及当期年份、期数等数据
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # 种子、存储配置定义
        self.redis = Redis(
            host=RedisConfig.host,
            port=RedisConfig.port,
            password=base64.b64decode(RedisConfig.password).decode("utf-8"),
            database=RedisConfig.database,
        )
        self.task_queue=RedisQueue(
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
        ctx = template.Context.get_context()
        return Config(
            init=[
                template.Init(
                    func=self._init_seeds,
                    layout=template.Layout(
                        factory={
                            "batch_id": lambda x: BatchProcessor.get_batch_id("daily"),
                            "journal_abbrev": lambda x: x.lower() if x else '',

                        }
                    )
                ),

            ],
            download=[
                template.Download(
                    url="{domain}/cn/article/current",
                    headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                        'Accept-Language': 'zh-CN,zh;q=0.9',
                        'Connection': 'keep-alive',
                        'Sec-Fetch-Dest': 'document',
                        'Sec-Fetch-Mode': 'navigate',
                        'Sec-Fetch-Site': 'none',
                        'Sec-Fetch-User': '?1',
                        'Upgrade-Insecure-Requests': '1',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"Windows"'
                    },
                    ok={
                        "response.status_code in (404, 500)": signals.Pass,
                    },
                    max_retry=2,
                    timeout=10,
                    use_session=True
                ),
                template.Download(
                    url="{domain}/{journal_abbrev}/cn/article/current",
                    headers={
                        'Accept': 'application/json, text/plain, */*',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                        'Connection': 'keep-alive',
                        'Content-Length': '0',
                        'Sec-Fetch-Site': 'same-origin',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0'
                    },
                    ok={
                        "response.status_code in (404, 500)": signals.Pass,
                    },
                    max_retry=2,
                    timeout=10,
                ),
                template.Download(
                    # url="{domain}/csv_data/article/current-csv_data",
                    url="{domain}/data/article/current-data",
                    method="POST",
                    params={"publisherId": "{journal_abbrev}"},
                    headers={
                        'Accept': 'application/json, text/plain, */*',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                        'Connection': 'keep-alive',
                        'Content-Length': '0',
                        'Cookie': '_sowise_user_sessionid_=4e898c50-dbc9-4f6b-b1f3-e34e8ff8cbdf; JSESSIONID=83122BCBFC31DE6FC6E0C909C9253CD5',
                        'Origin': '{domain}',
                        'Referer': '{domain}/{journal_abbrev}',
                        'Sec-Fetch-Dest': 'empty',
                        'Sec-Fetch-Mode': 'cors',
                        'Sec-Fetch-Site': 'same-origin',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
                        'sec-ch-ua': '"Microsoft Edge";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"Windows"'
                    },
                    ok={"response.status_code == 403": signals.Pass},
                    max_retry=2,
                    timeout=10,
                )
            ],
            parse=[
                template.Parse(
                    func=self._parse,
                ),
                template.Parse(
                    func=self._parse,
                ),
                template.Parse(
                    func="json",
                    kwargs={
                        "rules": {
                            "data.articles": {
                                "article_id": "id",
                                "article_url": "doi",
                                "year": "year",
                                "volume": "volume",
                                "issue": "issue",
                                "article_title": "titleCn",
                            }
                        }
                    },
                    layout=template.Layout(
                        default={
                            "journal_title": '{journal_title}',
                            "domain": '{domain}',
                            "batch_id": '{batch_id}',
                            "category": "{category}",
                            "time_stamp": int(time.time() * 1000)
                        },
                        factory={
                            "article_url": lambda x: ctx.seeds["domain"] + '/article/doi/' + x if x else ""
                        }
                    )
                )
            ],
            pipeline=[
                template.Pipeline(
                    func=to_mongo,
                    kwargs={
                        "path": "article_list_incremental",
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
        iter_data = self.mongo.batch_data(
            collection='journal_info',
            query={"category": "仁和汇智"},
            projection={
                "issn": 1,
                "journal_title": 1,
                "journal_abbrev": 1,
                "domain": 1,
                "category": 1
            },
        )
        return iter_data

    def _parse(self, context: Context) -> List[dict]:
        def extract_year_issue(text) -> tuple:
            nonlocal year, volume, issue

            normal_pattern = re.compile(r"(\d{4}),\s*?(\d+)\s*\((\w+|[\u4e00-\u9fa5]+)\)")
            specific_pattern = re.compile(r"(\d{4})[,\s]*?\((\w+|[\u4e00-\u9fa5]+)\):")
            normal_match = re.search(normal_pattern, text)
            specific_match = re.search(specific_pattern, text)
            if normal_match:
                year, volume, issue = normal_match.groups()
                return year, volume, issue
            elif specific_match:
                year, issue = specific_match.groups()
                return year, '', issue

        result = []

        response = context.response
        seeds = context.seeds
        soup = BeautifulSoup(response.text, "html.parser")

        articles_tag = soup.find("div", id="issueList")
        if not articles_tag:
            articles_tag = soup.find("div", class_=["articleListBox", "main-right"])
        article_list_tag = articles_tag.find_all("div", class_="article-list")
        for article_tag in article_list_tag:
            article_list_title = article_tag.find("div", class_="article-list-title")
            article_list_time = article_tag.find("div", class_="article-list-time")
            if article_list_time:
                year, volume, issue = extract_year_issue(article_list_time.text)
            else:
                year, volume, issue = '', '', ''
                loguru.logger.info(f"{article_list_title.text} 没有期刊信息")
            a_tag = article_list_title.find('a')
            if not a_tag:
                continue
            href = a_tag['href'] if a_tag else ''
            if not href or href.startswith("http"):
                article_url = href
            elif href.startswith("//"):
                article_url = f"https:{href}"
            elif href.startswith("/"):
                article_url = seeds['domain'] + href
            else:
                article_url = f"https://{href}"

            article_title = a_tag.text.strip()
            result.append({
                "article_id": "",
                "year": year,
                "issue": issue,
                "volume": volume,
                "journal_title": seeds['journal_title'],
                "domain": seeds['domain'],
                "category": seeds['category'],
                "article_url": article_url,
                "article_title": article_title,
                "time_stamp": int(time.time() * 1000),
                "batch_id": seeds['batch_id'],
            })
        return result

    def is_success(self, context: Context) -> bool:
        seeds = context.seeds
        response = context.response
        journal_title = seeds["journal_title"]

        if journal_title in SPECIAL_JOURNAL_LIST and seeds["$config"] != 2:
            context.submit({**seeds, "$config": 2})
            raise Success

        if seeds["$config"] in (0, 1):
            if any([
                "404" in response.url,
                response.status_code == 404,
                "errorMsgData" in response.text
            ]):
                context.submit({**seeds, "$config": seeds["$config"] + 1})
                raise Success

            if 'article-list' in response.text:
                return True
        else:
            if response.get("result") == "success":
                return True
        raise Failure


def get_incremental_seed():
    mongo = MongoInfo(
        host=MongoConfig.host,
        port=MongoConfig.port,
        username=base64.b64decode(MongoConfig.username).decode("utf-8"),
        password=base64.b64decode(MongoConfig.password).decode("utf-8"),
        database=MongoConfig.database,
        auth_database=MongoConfig.auth_database
    )

    # incremental_table = mongo.batch_data(
    #     collection='article_list_incremental',
    #     query={"issue": {"$ne": ""}},
    #     projection={
    #         "year": 1,
    #         "issue": 1,
    #         "domain": 1,
    #         "article_url": 1,
    #         "article_id": 1,
    #     }
    # )

    origin_table = mongo.batch_data(
        collection='article_list',
        query={},
        projection={
            "year": 1,
            "issue": 1,
            "nameOfTheJournal": 1
        }
    )

    # 转换为列表
    # 创建一个 defaultdict 来按 nameOfTheJournal 分组
    grouped_data = defaultdict(list)

    # 遍历 generator 数据
    for chunk in origin_table:
        for item in chunk:
            name_of_journal = item.get('nameOfTheJournal')
            year = item.get('year')
            issue = item.get('issue')

            # 确保 year 和 issue 是整数，非整数则跳过
            if isinstance(year, int) and isinstance(issue, int):
                grouped_data[name_of_journal].append((year, issue))
    # 创建结果字典，存储每个 journal 的最大 year 和 issue
    result = {}

    # 计算每个 group 的最大值
    for journal, values in grouped_data.items():
        # 取出最大 year 和 issue
        max_year, max_issue = max(values, key=lambda x: (x[0], x[1]))
        result[journal] = {"max_year": max_year, "max_issue": max_issue}

    # 输出最终结果
    for journal, data in result.items():
        print(f"Journal: {journal}, Max Year: {data['max_year']}, Max Issue: {data['max_issue']}")


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
    spider = ArticleIncrementalCrawler(
        concurrency=1,
        **{"init_queue_size": 1000000},
        queue_name="article_incremental",
        downloader=requests_.Downloader(),

        # proxy=proxy

    )

    spider.run(task_name="init")


    # get_incremental_seed()
    # spider.survey({"_id": "674ff6cc9cb944477dc09e00", "batch_id": "2024-12-27", "category": "仁和汇智", "domain": "http://www.trtb.net", "issn": "0564-3945", "journal_abbrev": "trtb", "journal_title": "土壤通报"})
