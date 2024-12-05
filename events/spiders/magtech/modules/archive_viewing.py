#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/4 14:30
# @Author  : AllenWan
# @File    : archive_viewing.py
# @Desc    ：
import base64
import re
import time
from sre_parse import parse
from typing import List

from bricks import const
from bricks.core import signals
from bricks.core.signals import Success, Failure
from bricks.db.redis_ import Redis
from bricks.downloader import requests_
from bricks.lib.queues import RedisQueue
from bricks.plugins.storage import to_mongo
from bricks.spider import template
from bricks.spider.template import Config, Context
from bs4 import BeautifulSoup, Tag

from config.config_info import RedisConfig, MongoConfig
from db.mongo import MongoInfo
from utils.batch_info import BatchProcessor
from utils.url import get_base_url


class ArchiveViewing(template.Spider):
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
                    layout=template.Layout(
                        factory={
                            "batch_id": lambda x: BatchProcessor.get_batch_id("weekly"),
                            "domain": lambda x: get_base_url(x)
                        }
                    )
                )
            ],
            download=[
                template.Download(
                    url="{domain}/CN/archive_by_years",
                    params={"forwardJsp": "simple"},
                    headers={
                        'Accept': 'text/html, */*; q=0.01',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                        'Connection': 'keep-alive',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'Origin': '{domain}',
                        'Referer': '{domain}/CN/home',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
                        'X-Requested-With': 'XMLHttpRequest'
                    },
                    ok={
                        "response.status_code == 404": signals.Pass,
                        "response.status_code == 403": signals.Pass,
                        },
                    max_retry=2,
                    timeout=20
                ),
                template.Download(
                    url="{domain}/CN/article/showOldVolumn.do",
                    headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                        'Connection': 'keep-alive',
                        'Sec-Fetch-Dest': 'document',
                        'Sec-Fetch-Mode': 'navigate',
                        'Sec-Fetch-Site': 'same-origin',
                        'Sec-Fetch-User': '?1',
                        'Upgrade-Insecure-Requests': '1',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
                        'sec-ch-ua': '"Microsoft Edge";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"Windows"'
                    },
                    ok={
                        "response.status_code == 404": signals.Pass,
                        "response.status_code == 403": signals.Pass,
                    },
                    max_retry=2,
                    timeout=20
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
                      "path": "journal_issue_list_magtech",
                       "conn": self.mongo,
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
            collection="journal_info",
            query={"category": "玛格泰克"},
            projection={"journal_title": 1, "domain": 1, "journal_abbrev": 1},
            # count=10
        )
        return iter_data

    def _parse(self, context: Context) -> List[dict]:
        def extract_year_volume(text: str) -> tuple[str, str]:
            nonlocal year
            nonlocal volume
            patterns = [re.compile(r"(\d{4})\s*Vol\.\s*(\d+)"), re.compile(r"(\d{4})年\s*(\d+)卷")]
            for pattern in patterns:
                match = re.search(pattern, text)
                if match:
                    year = match.group(1)  # 提取出年份
                    volume = match.group(2)  # 提取出卷号
                    return year, volume

            year_pattern = re.compile('(\d{4})')
            match = re.search(year_pattern, text)
            year = match.group(1) if match else ""
            if year:
                return year, ''
            raise ValueError("匹配获取年份及卷数失败")

        def extract_issue(text: str) -> str:
            nonlocal issue
            patterns = [
                re.compile(r"No\.\s*(\w+|[\u4e00-\u9fa5]+)"),
                re.compile(r"第(\d+)\s*期")
            ]
            for pattern in patterns:
                match = re.search(pattern, text)
                if match:
                    issue = match.group(1)  # 提取出期号
                    return issue
            raise ValueError("匹配获取期数失败")

        def normalize_href(url: str) -> str:
            if url.startswith(".."):
                url = url.replace("..", f"{seeds['domain']}/CN")
            return url

        def navigate_table(soup: BeautifulSoup) -> Tag:
            table = soup.find("div", class_="table-responsive")
            if not table:
                content_div = soup.find("div", class_=["content_nr", 'c_nr', "main"])
                if content_div:
                    table = content_div.find_all("table")[-1]
                else:
                    body = soup.find("body")
                    if body:
                        middle_table = body.find_all("table", recursive=False)[-1]
                        table = middle_table.find_all("table")[-1]

            if table:
                return table
            raise ValueError("获取Table标签失败")


        result = []
        seeds = context.seeds
        response = context.response
        html = response.text
        soup = BeautifulSoup(html, "html.parser")
        info_table = navigate_table(soup)
        # if info_table is None:

        """<table class="table table-striped table-hover table-bordered text-center" style="width:99%">"""
        tr_list = info_table.find_all("tr")
        year_title, year, volume = '', '', ''
        for tr_tags in tr_list:
            if not tr_tags.text.strip():
                continue
            td_list = tr_tags.find_all("td")
            th_tag = tr_tags.find("th")

            # 根据返回内容，判断具体从哪个标签中获取元文本
            if th_tag:
                year_title = th_tag.text
            elif td_list[0].text:
                tag_b = td_list[0].find("b")
                if tag_b:
                    year_title = td_list[0].text
                    td_list = td_list[1:]
            else:
                td_list = td_list[1:]

            # 如果有才提取，否则会保持为上一次执行的内容
            if year_title:
                year, volume = extract_year_volume(year_title)

            for td_tags in td_list:
                tag_a = td_tags.find("a")
                if not tag_a:
                    continue
                href = normalize_href(tag_a.get("href"))
                issue_text = td_tags.text
                issue = extract_issue(issue_text)
                result.append({
                    'year': year,
                    'issue': issue,
                    "volume": volume,
                    'raw_issue': re.sub(r'\s+', '', issue_text),
                    'issue_href': href,
                    'journal_title': seeds['journal_title'],
                    "publisher_id": seeds['journal_abbrev'],
                    'domain': seeds['domain'],
                    "time_stamp": int(time.time() * 1000),
                    "batch_id": seeds["batch_id"]
                })
        return result

    def is_success(self, context: Context) -> bool:
        seeds = context.seeds
        request_config = seeds["$config"]
        response = context.response

        if seeds["journal_abbrev"] and seeds["journal_abbrev"] not in seeds["domain"]:
            seeds["domain"] = seeds["domain"] + f"/{seeds['journal_abbrev']}"

        if any([
            request_config == 0 and ("http404" in response.text or "http500" in response.text),
            request_config == 0 and response.status_code == 404,
            seeds["journal_abbrev"] and seeds["journal_abbrev"] in seeds["domain"] and response.status_code == 404
        ]) :
            context.submit({**context.seeds, "$config": 1})
            raise Success
        if any([
            response.status_code in (403, 404),
        ]):

            if seeds["domain"].startswith("http://"):
                domain = seeds["domain"].replace("http://", "https://")
            else:
                domain = seeds["domain"].replace("https://", "http://")
            context.submit({**context.seeds, "domain": domain})
            raise Success

        if any([
            "过刊" in response.text,
            "content_nr" in response.text,
            "c_nr" in response.text,
            "table-responsive" in response.text,
        ]):
            return True



        raise Failure


if __name__ == '__main__':
    spider = ArchiveViewing(
        concurrency=5,
        # downloader=requests_.Downloader(),
        **{"init.queue.size": 1000000},
        task_queue=RedisQueue(
            host=RedisConfig.host,
            port=RedisConfig.port,
            password=base64.b64decode(RedisConfig.password).decode("utf-8"),
            database=RedisConfig.database,
        )
    )

    spider.run(task_name="all")
    # spider.survey({'$config': 0, '_id': '674ff6cc9cb944477dc09e2f', 'batch_id': '2024-12-01', 'domain': 'http://xbzk.cqjtu.edu.cn', 'journal_abbrev': None, 'journal_title': '重庆交通大学学报.  自然科学版'})
