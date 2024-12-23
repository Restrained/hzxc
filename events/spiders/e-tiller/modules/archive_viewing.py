#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/11 16:55
# @Author  : AllenWan
# @File    : archive_viewing.py
# @Desc    ：

import base64
import json
import re
import time
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
from bricks.utils.fake import user_agent
from bs4 import BeautifulSoup

from config.config_info import RedisConfig, MongoConfig
from db.mongo import MongoInfo
from utils.batch_info import BatchProcessor


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

                        }
                    )
                )
            ],
            download=[
                template.Download(

                    url="{domain}/ch/reader/issue_browser.aspx",
                    headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                        'Cache-Control': 'max-age=0',
                        'Connection': 'keep-alive',
                        'Cookie': 'guardret=UFJT; ASP.NET_SessionId=czwadizjg0hwiuz3jq2mvgwp; guardok=fw7kGRbjWnXhzidsUXwH+Q9zikiZGhISaZFbV72fGiheySFQLKXCBydFLtWQ08CByQsheFjtgdgGEVWV621q4Q==',
                        'Sec-Fetch-Dest': 'document',
                        'Sec-Fetch-Mode': 'navigate',
                        'Sec-Fetch-Site': 'none',
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
                ),
                template.Download(

                    url="{domain}/issue/browser",
                    headers={
                        'Accept': 'text/html, */*; q=0.01',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                        'Connection': 'keep-alive',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        "cookie": "guardret=UFJT; ASP.NET_SessionId=czwadizjg0hwiuz3jq2mvgwp; guardok=fw7kGRbjWnXhzidsUXwH+Q9zikiZGhISaZFbV72fGiheySFQLKXCBydFLtWQ08CByQsheFjtgdgGEVWV621q4Q==",
                        'Origin': '{domain}',
                        'Referer': '{domain}}/ch/index.aspx',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
                        'X-Requested-With': 'XMLHttpRequest'
                    },
                    # ok={
                    #     "response.status_code == 404": signals.Pass,
                    #     "response.status_code == 403": signals.Pass,
                    #     },
                    max_retry=2,
                    timeout=20
                ),

            ],
            parse=[
                template.Parse(
                    func=self._parse
                ),
                template.Parse(
                    func=self._parse_v2
                )
            ],
            pipeline=[
                template.Pipeline(
                    func=to_mongo,
                    kwargs={
                        "path": "journal_issue_list_etiller",
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
            query={"category": "勤云"},
            projection={
                "domain": 1,
                "journal_title": 1,
                "journal_abbrev": 1,
            }
        )
        return iter_data

    def _parse(self, context: Context) -> List[dict]:

        def extract_year_volume(text: str) -> tuple[str, str]:
            nonlocal year
            nonlocal volume
            patterns = [re.compile(r"(\d{4})\s*Vol\.\s*(\d+)"), re.compile(r"(\d{4})年\s*第?(\d+)卷")]
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
            else:
                return '', ''
            # raise ValueError("匹配获取年份及卷数失败")

        def is_english_or_digit(text):
            return bool(re.match(r"^[a-zA-Z0-9]+$", text))

        def extract_issue(text: str) -> str:
            if any([
                "增刊" in text,
                "在线优先" in text,
                is_english_or_digit(text),
            ]):
                return text
            nonlocal issue
            patterns = [
                re.compile(r"No\.\s*(\w+|[\u4e00-\u9fa5]+)"),
                re.compile(r"第\s*([\w/-]+|[\u4e00-\u9fa5]+)\s*期")
            ]
            for pattern in patterns:
                match = re.search(pattern, text)
                if match:
                    issue = match.group(1)  # 提取出期号
                    return issue
            raise ValueError("匹配获取期数失败")

        result = []
        seeds = context.seeds
        response = context.response
        html = response.text
        soup = BeautifulSoup(html, "html.parser")
        if "strAllIssueJson" in response.text:
            return self._parse_v2(context)
        else:
            outer_table_tag = soup.find("table", attrs={"cellspacing": "1"})
            if not outer_table_tag:
                year, volume = "", ""
                outer_table_tag = soup.find("table", id="QueryUI")
                inner_table_list = outer_table_tag.find_all("table")
                for index, table in enumerate(inner_table_list):

                    if index % 2 == 0:
                        td_tag = table.find("td")
                        year, volume = extract_year_volume(td_tag.text)
                    else:
                        ul_tag = table.find("ul")
                        li_tag_list = ul_tag.find_all("li")
                        for li_tag in li_tag_list:
                            p_tag = li_tag.find("p")
                            a_tag = p_tag.find("a")
                            raw_issue = a_tag.text
                            href = a_tag.get("href")
                            issue = extract_issue(raw_issue)
                            result.append({
                                'year': year,
                                'issue': issue,
                                "volume": volume,
                                'raw_issue': re.sub(r'\s+', '', raw_issue),
                                'issue_href': href,
                                'journal_title': seeds['journal_title'],
                                "publisher_id": seeds["journal_abbrev"],
                                "request_url": response.url,
                                'domain': seeds['domain'],
                                "time_stamp": int(time.time() * 1000),
                                "batch_id": seeds["batch_id"]
                            })



            inner_table = outer_table_tag.find("table") or outer_table_tag.find("div")

            tr_tag_list = inner_table.find_all("tr") or inner_table.find_all("ul")

            for tr_tag in tr_tag_list:
                if not tr_tag.find_all("td") and not tr_tag.find_all("li"):
                    continue
                year_tag, *td_list = tr_tag.find_all("td") or tr_tag.find_all("li")
                year_str = year_tag.text
                year, volume = extract_year_volume(year_str)
                for items in td_list:
                    tag_a = items.find("a")
                    if tag_a:
                        href = tag_a.get("href")
                        issue_str = tag_a.text
                        issue = extract_issue(issue_str)
                        result.append({
                            'year': year,
                            'issue': issue,
                            "volume": volume,
                            'raw_issue': re.sub(r'\s+', '', issue_str),
                            'issue_href': href,
                            'journal_title': seeds['journal_title'],
                            "publisher_id": seeds["journal_abbrev"],
                            "request_url": response.url,
                            'domain': seeds['domain'],
                            "time_stamp": int(time.time() * 1000),
                            "batch_id": seeds["batch_id"]
                        })

            return result

    def _parse_v2(self, context: Context) -> List[dict]:
        def extract_info(txt):
            pattern = re.compile("(\d{4})年第(\d*)卷第(\w+)期")
            match = re.search(pattern, txt)
            year, volume, issue = match.groups()
            return year, volume, issue

        result = []
        seeds = context.seeds
        response = context.response
        html = response.text
        soup = BeautifulSoup(html, "html.parser")
        json_pattern = re.compile(r"strAllIssueJson\s*=\s*\"(.*?)\";")
        json_match = re.search(json_pattern, html)
        if json_match:
            json_txt = json_match.group(1)
            json_content = json.loads(json_txt.replace('\\', ''))
            year_list = json_content["years"]
            for year_item in year_list:
                year = year_item["year_id"]
                volume = year_item["volume"]
                issue_list = year_item["issues"]
                for issue_item in issue_list:
                    issue = issue_item["issue_id"]
                    raw_issue = issue_item["cn_name"]
                    issue_url = issue_item["issue_url"]

                    result.append({
                        'year': year,
                        'issue': issue,
                        "volume": volume,
                        'raw_issue': raw_issue,
                        'issue_href': issue_url,
                        'journal_title': seeds['journal_title'],
                        "publisher_id": seeds["journal_abbrev"],
                        "request_url": response.url,
                        'domain': seeds['domain'],
                        "time_stamp": int(time.time() * 1000),
                        "batch_id": seeds["batch_id"]
                    })

            return result
        elif "slideBox_nr" in response.text:
            tag_list = soup.find_all("div", class_="slideBox_nr")
            for tag_item in tag_list:
                tag_a = tag_item.find("a")
                href = tag_a.get("href")
                text = tag_a.text
                year, volume, issue = extract_info(text)
                result.append({
                    'year': year,
                    'issue': issue,
                    "volume": volume,
                    'raw_issue': text,
                    'issue_href': href,
                    'journal_title': seeds['journal_title'],
                    "publisher_id": seeds["journal_abbrev"],
                    "request_url": response.url,
                    'domain': seeds['domain'],
                    "time_stamp": int(time.time() * 1000),
                    "batch_id": seeds["batch_id"]
                })
            return result
        raise ValueError("parse archive have an error")

    def is_success(self, context: Context) -> bool:
        seeds = context.seeds
        response = context.response
        if response.status_code > 200 or response.url.__contains__("404"):
            context.submit({**seeds, "$config": 1})
            raise Success

        if any([
            'class="zhong"' in response.text,
            'class="center"' in response.text,
            'class="Erji-right-box"' in response.text,
            'id="rong-qi"' in response.text,
            '过刊浏览' in response.text,

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

    spider = ArchiveViewing(
        concurrency=5,
        downloader=requests_.Downloader(),
        **{"init.queue.size": 1000000},
        task_queue=RedisQueue(
            host=RedisConfig.host,
            port=RedisConfig.port,
            password=base64.b64decode(RedisConfig.password).decode("utf-8"),
            database=RedisConfig.database,
        ),
        # proxy=proxy
    )

    spider.run(task_name="all")
    # spider.survey({'batch_id': '2024-12-01', 'domain': 'http://zgsydw.cnjournals.com/zgbjyxzz', 'journal_abbrev': None,
    #                'journal_title': '中国环境监测'})
#
