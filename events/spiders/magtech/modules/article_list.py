#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/5 17:32
# @Author  : AllenWan
# @File    : article_list.py
# @Desc    ：
import base64
import re
import time
from typing import List, Union

from bricks import const
from bricks.db.redis_ import Redis
from bricks.lib.queues import RedisQueue
from bricks.plugins import scripts
from bricks.plugins.storage import to_mongo
from bricks.spider import template
from bricks.spider.template import Config, Context
from bricks.utils.fake import user_agent
from bs4 import BeautifulSoup, Tag
from jinja2.lexer import Failure


from config.config_info import RedisConfig, MongoConfig
from db.mongo import MongoInfo
from utils.url import get_base_url


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
                    url="{issue_href}",
                    headers={
                        'Accept': 'text/html, */*; q=0.01',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                        'Connection': 'keep-alive',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'Origin': '{domain}',
                        'Referer': '{domain}/CN/home',
                        'User-Agent': user_agent.chrome(),
                        'X-Requested-With': 'XMLHttpRequest'
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
                        "path": "article_list_magtech",
                        "conn": self.mongo
                    },
                    success=True
            )],
            events={
                const.AFTER_REQUEST: [
                    template.Task(
                    func=self.is_success
                )
                ]
            }
        )


    def _init_seeds(self):
        iter_data = self.mongo.batch_data(
            collection="journal_issue_list_magtech",
            query={"journal_title": "岩石力学与工程学报"},
            projection={
                "year": 1,
                "issue": 1,
                "volume": 1,
                "issue_href": 1,
                "journal_title": 1,
                "batch_id": 1
            },
        )
        return iter_data

    def _parse(self, context: Context) -> List[dict]:
        def find_tag_article_outer(element: Tag) -> Union[Tag, None]:
            rules = [
                lambda x: x.find("div", class_="articles"),
                lambda x: x.find("div", class_="j-content"),
                lambda x: x.find("div", class_="c_nr"),
                lambda x: x.find("div", class_="content_nr"),
                lambda x: x.find("form", id="AbstractList"),
                lambda x: x.find("form", attrs={'name': "AbstractList"}),
                lambda x: x.find("ul", class_="article-list"),
                lambda x: x.find("ul", class_="journal-article"),
                lambda x: x.find("div", class_="main"),
            ]
            for rule in rules:
                match_result = rule(element)
                if match_result:
                    return match_result
            return None


        def find_tag_article_list(element: Tag) -> Union[List[Tag], None]:
            pattern = re.compile(r'^art\d+$')

            rules = [
                lambda x: x.find_all("div", class_="wenzhang"),
                lambda x: x.find_all("div", class_="article-l"),
                lambda x: x.find_all("ul", class_="lunwen"),
                lambda x: x.find_all("div", id=pattern),
            ]

            for rule in rules:
                match_result = rule(element)
                if match_result:
                    return match_result

            if element.name == 'ul' and "journal-article" in element.get("class", []):
                match_result = element.find_all("ul")
                if match_result:
                    return match_result
            return None


        def find_tag_a(element: Tag ) -> Union[Tag, None]:
            rules = [
                lambda x: x.find("a", class_="biaoti"),
                lambda x: x.find("a", class_="txt_biaoti"),
                lambda x: x.find("dd", class_="timu").find("a") if x.find("dd", class_="timu") else None,
                lambda x: x.find("li", class_="biaoti").find("a") if x.find("li", class_="biaoti") else None,
                lambda x: x.find("div", class_="j-title-1").find("a") if x.find("div", class_="j-title-1") else None,
                lambda x: x.find("li", class_="article-title").find("a") if x.find("li", class_="article-title") else None,
            ]

            # 遍历规则，返回第一个匹配的结果
            for rule in rules:
                match_result = rule(element)
                if match_result:
                    return match_result
            return None  # 如果没有匹配到，返回 None

        def find_tag_author(element: Tag ) -> Union[Tag, None]:
            # 定义所有定位规则
            rules = [
                lambda x: x.find("dd", class_="zuozhe"),
                lambda x: x.find("div", class_="j-author"),
                lambda x: x.find("li", class_="zuozhe"),
                lambda x: x.find("div", class_="authorList"),
                lambda x: x.find("li", class_="article-author"),
            ]

            # 遍历规则，返回第一个匹配的结果
            for rule in rules:
                match_result = rule(element)
                if match_result:
                    return match_result
            return None  # 如果没有匹配到，返回 None

        def normalize_href(url: str) -> str:

            if url.startswith(".."):
                url = url.replace("..", f"{get_base_url(seeds['issue_href'])}/CN")
            return url


        result = []
        seeds = context.seeds
        response = context.response

        soup = BeautifulSoup(response.text, "html.parser")

        article_list_tag = find_tag_article_outer(soup)
        if not article_list_tag:
            tag_tds = soup.find_all("td", {"width": "400"})
            for tag_td in tag_tds:
                tag_a = tag_td.find("a")
                font = tag_td.find("font")
                article_title = tag_a.text
                authors = font.text
                article_url = tag_a.get('href')
                article_url = normalize_href(article_url)
                result.append(
                    {
                        'year': seeds['year'],
                        'issue': seeds['issue'],
                        "volume": seeds['volume'],
                        'journal_title': seeds['journal_title'],
                        "article_title": article_title,
                        'article_url': article_url,
                        'authors': authors,
                        "time_stamp": int(time.time() * 1000),
                        "batch_id": seeds["batch_id"]
                    }
                )
        else:
            article_tags = find_tag_article_list(article_list_tag)
            article_title, article_url, authors = '', '', ''
            if article_tags:
                for item in article_tags:
                    tag_a = find_tag_a(item)
                    article_title = tag_a.text
                    article_url = tag_a.get("href")
                    tag_authors = find_tag_author(item)
                    authors = tag_authors.text.strip()

                    result.append(
                        {
                            'year': seeds['year'],
                            'issue': seeds['issue'],
                            "volume": seeds['volume'],
                            'journal_title': seeds['journal_title'],
                            "article_title": article_title,
                            'article_url': article_url,
                            'authors': authors,
                            "time_stamp": int(time.time() * 1000),
                            "batch_id": seeds["batch_id"]
                        }
                    )
            else:
                table_list = article_list_tag.select("table>tr>td>table>tr>td>table")
                tag_div = article_list_tag.find("div")
                if not tag_div:

                    for tag_table in table_list:
                        tag_td_list = tag_table.find_all("td", class_="J_VM")
                        if tag_td_list:
                            """
                             这类网页很容易遇到只有一个标题，没有作者的情况，像这类authors为空的情况是容许的
                            """
                            article_title = tag_td_list[1].find('b').text
                            authors = tag_td_list[3].text
                            article_url = tag_td_list[-1].find('a').get('href')
                            article_url = normalize_href(article_url)
                            result.append(
                                {
                                    'year': seeds['year'],
                                    'issue': seeds['issue'],
                                    "volume": seeds['volume'],
                                    'journal_title': seeds['journal_title'],
                                    "article_title": article_title,
                                    'article_url': article_url,
                                    'authors': authors,
                                    "time_stamp": int(time.time() * 1000),
                                    "batch_id": seeds["batch_id"]
                                }
                            )
                        else:

                            tag_td_list = tag_table.find_all("td", {"valign": "center"})
                            if tag_td_list:
                                article_title = tag_td_list[2].find('b').text
                                authors = tag_td_list[3].text
                                tag_a = tag_td_list[2].find('a')
                                if not tag_a:
                                    tag_a = tag_td_list[-1].find('a')
                                article_url = normalize_href(tag_a.get('href'))
                                result.append(
                                    {
                                        'year': seeds['year'],
                                        'issue': seeds['issue'],
                                        "volume": seeds['volume'],
                                        'journal_title': seeds['journal_title'],
                                        "article_title": article_title,
                                        'article_url': article_url,
                                        'authors': authors,
                                        "time_stamp": int(time.time() * 1000),
                                        "batch_id": seeds["batch_id"]
                                    }
                                )

                else:
                    tag_table_list = tag_div.find_all("table", recursive=False)
                    for tag_td_set in tag_table_list:
                        tds = tag_td_set.find_all("td", {"valign": "center"})
                        article_title = tds[2].find('b').text
                        authors = tds[3].text
                        tag_a = tds[2].find('a')
                        if not tag_a:
                            tag_a = tds[-1].find('a')
                        article_url = normalize_href(tag_a.get('href'))
                        result.append(
                            {
                                'year': seeds['year'],
                                'issue': seeds['issue'],
                                "volume": seeds['volume'],
                                'journal_title': seeds['journal_title'],
                                "article_title": article_title,
                                'article_url': article_url,
                                'authors': authors,
                                "time_stamp": int(time.time() * 1000),
                                "batch_id": seeds["batch_id"]
                            }
                        )
        return result


    def is_success(self, context: Context) -> bool:
        response = context.response
        if "article" in response.text:
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

    spider = ArticleListSpider(
        concurrency=20,
        **{"init.queue.size": 1000000},
        task_queue=RedisQueue(
            host=RedisConfig.host,
            port=RedisConfig.port,
            password=base64.b64decode(RedisConfig.password).decode("utf-8"),
            database=RedisConfig.database,
        ),
        proxy=proxy
    )

    # spider.run(task_name='spider')
    spider.survey({"_id": "6751724d326da18d1d51296d", "batch_id": "2024-12-01", "issue": "5", "issue_href": "https://www.jsjkx.com/CN/volumn/volumn_384.shtml", "journal_title": "生态科学", "volume": "32", "year": "2013"})