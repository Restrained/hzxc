#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/14 10:11
# @Author  : AllenWan
# @File    : list_of_journals.py
import re
import time

import loguru
from bricks import const
from bricks.core import signals
from bricks.core.signals import Success, Failure
from bricks.lib.queues import RedisQueue
from bricks.plugins import scripts
from bricks.plugins.make_seeds import by_csv
from bricks.plugins.storage import to_mongo
from bricks.spider import template
from bricks.db.redis_ import Redis
from bricks.db.mongo import Mongo
from bricks.spider.template import Config
from bricks.utils.fake import user_agent
from bs4 import BeautifulSoup



class JournalList(template.Spider):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.redis = Redis()
        self.mongo = Mongo(
            database='modules',
        )

    @property
    def config(self) -> Config:
        ctx = template.Context.get_context()
        return Config(
            init=[
                template.Init(
                    func=by_csv,
                    kwargs={
                        "path": r"D:\pyProject\hzcx\renHeHuiZhi\chineseoptics\seeds\archive_viewing_list.csv",
                        "query": "select year, issue, issue_href, nameOfTheJournal, officialWebsite, publisher_id  from <TABLE> where year !='' ",
                        "batch_size": 2000
                    }

                ),

            ],

            download=[
                template.Download(
                    url="{issue_href}",
                    method="GET",
                    headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                        'Connection': 'keep-alive',
                        'Upgrade-Insecure-Requests': '1',
                        'User-Agent': user_agent.chrome()
                    },
                    timeout=15,
                    max_retry=2,
                    ok={"response.status_code == 500": signals.Pass, "response.status_code == 404": signals.Pass}
                ),
                template.Download(
                    url="{officialWebsite}/data/article/archive-article-data",
                    method="POST",
                    params={
                        "issue": "{issue}",
                        "publisherId": "{publisher_id}",
                        "volume": "",
                        "year": "{year}"
                    },
                    headers={
                        'Accept': 'application/json, text/plain, */*',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                        'Content-Length': '0',
                        'Origin': '{officialWebsite}',
                        'Proxy-Connection': 'keep-alive',
                        'Referer': '{issue_href}',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0'
                    },
                    timeout=30,
                    max_retry=2
                )
            ],
            parse=[
                template.Parse(
                    func=self._parse
                ),
                template.Parse(
                    func="json",
                    kwargs={
                        "rules": {
                            "data.articles": {
                                "article_id": "id",
                                "article_url": "doi",
                                "year": "year",
                                "issue": "issue",
                                "article_title": "titleCn",
                            }
                        }
                    },
                    layout=template.Layout(
                        default={
                            "nameOfTheJournal": '{nameOfTheJournal}',
                            "officialWebsite": '{officialWebsite}',},
                        factory={
                            "article_url": lambda x: ctx.seeds["officialWebsite"]  +'/article/doi/' + x if x else ""
                        }
                    )
                )
            ],
            pipeline=[
                template.Pipeline(
                    func=to_mongo,
                    kwargs={
                        "path": "journal_list",
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

    def is_success(self, context: template.Context):
        seeds = context.seeds
        response = context.response
        json_api_file_path = r"/events/modules/rhhz/input/json_api_list"

        with open(json_api_file_path, "r", encoding="utf-8") as f:
            json_api_list = list(rows.strip() for rows in f.readlines())

        if seeds['$config'] == 0:
            # 假设网站在特定名单中，走第二个接口请求
            if seeds['nameOfTheJournal'] in json_api_list:
                context.submit({**seeds, "$config": 1})
                raise Success
            soup = BeautifulSoup(response.text, 'lxml')
            # 获取 <title> 标签的文本内容
            title = soup.title.string
            if title == "Error 404":
                raise Success
            elif any(feature in context.response.text for feature in ['article-list-title', 'articleListBox'] ):
                return True
            else:
                raise Failure
        if response.get("result") == "success":
            return True
        raise Failure

    def _parse(self, context: template.Context):
        result = []
        seeds = context.seeds
        response = context.response
        # 解析html
        soup = BeautifulSoup(response.text, 'html.parser')

        article_list_box = soup.find("div", class_="articleListBox")
        if not article_list_box:
            article_list_box = soup.find("div", class_="main-right")
        article_list = article_list_box.find_all("div", class_="article-list")
        if article_list:
            for items in article_list:
                article_list_title = items.find("div", class_="article-list-title")
                a_tag = article_list_title.find('a')
                if not a_tag:
                    continue
                href = a_tag['href'] if a_tag else ''
                if not href or href.startswith("http"):
                    article_url = href
                elif href.startswith("//"):
                    article_url = f"https:{href}"
                elif href.startswith("/"):
                    article_url =seeds['officialWebsite'] + href
                else:
                    article_url = f"https://{href}"

                p_text = re.sub(r"\s+", "", a_tag.text)
                result.append({
                    "article_id": "",
                    "year": seeds['year'],
                    "issue": seeds['issue'],
                    "nameOfTheJournal": seeds['nameOfTheJournal'],
                    "officialWebsite": seeds['officialWebsite'],
                    "article_url": article_url,
                    "article_title": p_text,
                })
        else:
            loguru.logger.info(f"{seeds['nameOfTheJournal']} {seeds['year']} {seeds['issue']} 的{seeds['issue_href']} 请求结果为空")
            raise Success
            # time.sleep(3000)

        return result


if __name__ == '__main__':
    spider = JournalList(
        concurrency=100,
        **{"init.queue.size": 1000000},
        task_queue=RedisQueue(),
    )
    spider.run( task_name='all')
    # spider.survey({"issue": "2", "issue_href": "https://www.hjgcjsxb.org.cn/cn/article/2021/2", "nameOfTheJournal": "环境工程技术学报", "officialWebsite": "https://www.hjgcjsxb.org.cn", "publisher_id": "", "year": "2021"})
    # {'issue': '3', 'issue_href': 'http://engineeringmechanics.cn/cn/article/2011/3', 'nameOfTheJournal': '工程力学', 'officialWebsite': 'http://engineeringmechanics.cn/', 'publisher_id': '', 'year': '2011', '$config': 0}
    # {'issue': '4', 'issue_href': 'http://ycxb.tobacco.org.cn/cn/article/2018/4', 'nameOfTheJournal': '中国烟草学报', 'officialWebsite': 'http://ycxb.tobacco.org.cn/', 'publisher_id': '', 'year': '2018', '$config': 0}

    # {'issue': '3', 'issue_href': 'https://www.cjss.ac.cn/cn/article/2024/3', 'nameOfTheJournal': '空间科学学报', 'officialWebsite': 'https://www.cjss.ac.cn/', 'publisher_id': '', 'year': '2024', '$config': 0}