#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/18 14:59
# @Author  : AllenWan
# @File    : article_list.py
# @Desc    ：
#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/5 17:32
# @Author  : AllenWan
# @File    : article_list.py
# @Desc    ：
import base64
import re
import time
from typing import List, Union, Literal
from urllib.parse import urlparse, urlunparse

from bricks import const
from bricks.core.signals import Failure, Success
from bricks.db.redis_ import Redis
from bricks.downloader import requests_
from bricks.lib.queues import RedisQueue
from bricks.plugins import scripts
from bricks.plugins.storage import to_mongo
from bricks.spider import template
from bricks.spider.template import Config, Context
from bs4 import BeautifulSoup, Tag

from config.config_info import RedisConfig, MongoConfig
from db.mongo import MongoInfo


class ArticleListSpider(template.Spider):
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
            init=[template.Init(
                func=self._init_seeds
            )
            ],
            download=[
                template.Download(
                    url="{url}",
                    headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                        'Cache-Control': 'max-age=0',
                        'Connection': 'keep-alive',
                        'Cookie': 'ASP.NET_SessionId=czwadizjg0hwiuz3jq2mvgwp; guardok=ZbFyXFxrQWq9Z9cTUmFucNS6zWCM8EIaH0NTGaqQGZvmiAnfaNlfEK5DgFdbcZQHyBGWgG4+kBwezMteSEqlJw==; guard=07790404gyWQ77',
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
                    max_retry=2,
                    timeout=20
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
                        "path": "article_list_etiller",
                        "conn": self.mongo
                    },
                    success=True
                )],
            events={
                const.BEFORE_PUT_SEEDS: [
                    template.Task(
                        func=self.get_url,
                    )
                ],
                const.AFTER_REQUEST: [
                    template.Task(
                        func=self.is_success
                    )
                ]
            }
        )

    def _init_seeds(self):
        iter_data = self.mongo.batch_data(
            collection="journal_issue_list_etiller",
            query={},
            projection={
                "year": 1,
                "issue": 1,
                "volume": 1,
                "issue_href": 1,
                "journal_title": 1,
                "request_url": 1,
                "domain": 1,
                "batch_id": 1
            },
        )

        return iter_data

    def _parse(self, context: Context) -> List[dict]:
        result = []
        seeds = context.seeds
        response = context.response

        soup = BeautifulSoup(response.text, "html.parser")
        if 'class="article_list"' in response.text or "class='article_list'" in response.text:
            article_list_tag = soup.find("div", class_="article_list")
            article_line_list = article_list_tag.find_all("li", class_="article_line")
            if not article_line_list:
                article_line_list = soup.find_all("div", class_="article_list_right")

            for article_item in article_line_list:
                article_title_tag = article_item.find("div", class_=["article_title", "article_list_title"])
                article_title = article_title_tag.find("a").text.strip()
                article_url = article_title_tag.find("a").get("href")

                article_toolbar = article_item.find("div", class_="article_toolbar")
                if not article_toolbar:
                    btn_pdf = article_item.find("font", class_="pdf_url")
                    pdf_link = self.modify_url(seeds["domain"], btn_pdf.find("a").get("href"))
                else:
                    btn_pdf = article_toolbar.find("a", class_="btn_pdf")
                    pdf_link = self.modify_url(seeds["domain"], btn_pdf.get("href"))
                result.append(
                    {
                        'year': seeds['year'],
                        'issue': seeds['issue'],
                        "volume": seeds['volume'],
                        'journal_title': seeds['journal_title'],
                        "article_title": article_title,
                        'article_url': article_url,
                        'authors': "",
                        "pdf_link": pdf_link,
                        "time_stamp": int(time.time() * 1000),
                        "batch_id": seeds["batch_id"]
                    }
                )
        else:
            outer_table = soup.find("table", attrs={"align": "center"})
            if not outer_table:
                outer_table = soup.find("table", id=["table1", "table3"])
            table_list = outer_table.find_all("table", id="table24")
            article_title, article_url, pdf_link = "", "", ""

            if not table_list:
                table_list = outer_table.find_all("table", id="table4")

            for table in table_list:
                # 跳过开头选择框部分
                form = table.find("FORM", id="merge_abstract")
                if form:
                    continue
                tbody = table.find("tbody")
                if tbody:
                    tag_a_list = tbody.select('tr td a')

                    if not tag_a_list:
                        tag_a_list = tbody.select('tr td table tr td a')
                else:
                    tag_a_list = table.select('tr td li a')

                for tag_a in tag_a_list:
                    tag_b = tag_a.find("b")
                    title_txt = tag_b.text if tag_b else tag_a.text

                    if title_txt == '摘要'or title_txt == "HTML":
                        continue
                    elif title_txt.__contains__("PDF"):
                        pdf_link = seeds['request_url'].replace("issue_browser.aspx", tag_a.get("href"))
                        result.append(
                            {
                                'year': seeds['year'],
                                'issue': seeds['issue'],
                                "volume": seeds['volume'],
                                'journal_title': seeds['journal_title'],
                                "article_title": article_title,
                                'article_url': article_url,
                                'authors': "",
                                "pdf_link": pdf_link,
                                "time_stamp": int(time.time() * 1000),
                                "batch_id": seeds["batch_id"]
                            }
                        )
                    else:
                        article_title = title_txt.strip().replace('\n', '')
                        article_url = tag_a.get("href")

        return result

    @staticmethod
    def modify_url(url: str, new_path: str) -> Literal[b""]:
        # 解析 URL
        parsed_url = urlparse(url)

        # 获取路径部分并去除开头的斜杠
        path_parts = parsed_url.path.lstrip('/').split('/')

        # 如果路径有多个部分，保留第一个部分，其余部分用新路径替换
        if len(path_parts) > 1:
            # 拼接第一个部分和新的路径
            new_full_path = '/' + path_parts[0] + '/' + new_path
        else:
            # 如果只有一个部分，直接替换为新的路径
            new_full_path = new_path
            # 重新构建 URL
        modified_url = urlunparse(parsed_url._replace(path=new_full_path))

        return modified_url

    def get_url(self, context: Context):
        seeds = context.seeds

        for data_list in seeds:
            if data_list["request_url"].__contains__("issue_browser.aspx"):
                data_list['url'] = data_list["request_url"].replace("issue_browser.aspx", data_list['issue_href'])
            else:
                data_list['url'] = self.modify_url(data_list['domain'], data_list['issue_href'])

    def is_success(self, context: Context) -> bool:
        response = context.response
        if any([
            'id="table24"' in response.text,
            'id="table24"' in response.text,
            'id="table4"' in response.text,
            'class="article_list"' in response.text,
            "class='article_list'" in response.text,
        ]):
            return True
        if "merge_abstract" in response.text or "搜索出现意外错误" in response.text:
            raise Success
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

    spider = ArticleListSpider(
        concurrency=100,
        **{"init.queue.size": 1000000},
        downloader=requests_.Downloader(),
        task_queue=RedisQueue(
            host=RedisConfig.host,
            port=RedisConfig.port,
            password=base64.b64decode(RedisConfig.password).decode("utf-8"),
            database=RedisConfig.database,
        ),
        proxy=proxy
    )

    spider.run(task_name='spider')
    # spider.survey({"_id": "676275f5430bc091baa5fde0", "batch_id": "2024-12-15", "domain": "https://www.geojournals.cn/dzxb/dzxb", "issue": "1", "issue_href": "dzxb/article/issue/96_1", "journal_title": "地质学报", "request_url": "https://www.geojournals.cn/dzxb/dzxb/issue/browser", "url": "https://www.geojournals.cn/dzxb/dzxb/article/issue/96_1", "volume": "", "year": "96"})
