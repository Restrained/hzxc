#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/6 09:16
# @Author  : AllenWan
# @File    : article_incremental.py
# @Desc    ：
import argparse
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
                    url="{domain}/csv_data/article/current-csv_data",
                    method="POST",
                    params={"publisherId": "{journal_abbrev}"},
                    headers={
                        'Accept': 'application/json, text/plain, */*',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                        'Connection': 'keep-alive',
                        'Content-Length': '0',
                        'Cookie': '_sowise_user_sessionid_=4e898c50-dbc9-4f6b-b1f3-e34e8ff8cbdf; JSESSIONID=2938706F8D43AF5F247B24CBB69C426E',
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
                            "csv_data.articles": {
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
            pattern = re.compile(r"(\d{4}),\s*?(\d+)\s*\((\w+|[\u4e00-\u9fa5]+)\)")
            match = re.search(pattern, text)
            if match:
                year, volume, issue = match.groups()
                return year, volume, issue

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
        if seeds["$config"] in (0, 1):
            if any([
                "404" in response.url,
                response.status_code == 404,
                "errorMsgData" in response.text
                ]):
                context.submit({**seeds, "$config": 1})
                raise Success
            elif journal_title in SPECIAL_JOURNAL_LIST:
                context.submit({**seeds, "$config": 2})
                raise Success

            if 'article-list' in response.text:
                return True
        else:
            if response.get("result") == "success":
                return True
        raise Failure

    # # 定义调度任务
    # def schedule_crawler_task(task_name, concurrency, init_queue_size):
    #     downloader = "dummy_downloader"  # 这里可以根据需求换成实际的下载器
    #     task_queue = "dummy_task_queue"  # 这里可以换成实际的队列实现
    #
    #     spider = ArticleIncrementalCrawler(
    #         concurrency=concurrency,
    #         init_queue_size=init_queue_size,
    #         downloader=downloader,
    #         task_queue=task_queue
    #     )
    #
    #     # 调用爬虫的 run 方法执行任务
    #     spider.run(task_name)

    # 设置定时任务
    # def setup_schedule(task_name, concurrency, init_queue_size, interval_minutes):
    #     schedule.every(interval_minutes).minutes.do(schedule_crawler_task, task_name=task_name, concurrency=concurrency,
    #                                                 init_queue_size=init_queue_size)
    #
    #     print(f"Scheduled task '{task_name}' to run every {interval_minutes} minutes.")
    #
    #     # 循环保持任务运行
    #     while True:
    #         schedule.run_pending()
    #         time.sleep(1)
    #
    # # 解析命令行参数
    # def parse_arguments():
    #     parser = argparse.ArgumentParser(description="Run the article crawler with dynamic parameters.")
    #     parser.add_argument("--task_name", type=str, required=True, help="Name of the crawling task.")
    #     parser.add_argument("--concurrency", type=int, default=1, help="Number of concurrent requests.")
    #     parser.add_argument("--init_queue_size", type=int, default=1000000, help="Initial queue size.")
    #     parser.add_argument("--interval", type=int, default=60, help="Interval in minutes for running the crawler.")
    #
    #     return parser.parse_args()

    # if __name__ == "__main__":
    #     # 获取命令行参数
    #     args = parse_arguments()
    #
    #     # 设置定时任务
    #     setup_schedule(args.task_name, args.concurrency, args.init_queue_size, args.interval)

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
    spider = ArticleIncrementalCrawler(
        concurrency=1,
        **{"init_queue_size": 1000000},
        downloader=requests_.Downloader(),
        task_queue=RedisQueue(
            host=RedisConfig.host,
            port=RedisConfig.port,
            password=base64.b64decode(RedisConfig.password).decode("utf-8"),
            database=RedisConfig.database,
        ),  # 定义种子来源

    )

    # spider.run(task_name="all")
    get_incremental_seed()
    # spider.survey({"$config": 2, "_id": "674ff6cc9cb944477dc09db4", "batch_id": "2024-12-06", "category": "仁和汇智",
    #                "domain": "https://www.whuhzzs.com", "issn": "2096-7993", "journal_abbrev": "lceh",
    #                "journal_title": "临床耳鼻咽喉头颈外科杂志"})
