"""
archive_viewing -

Author: AllenWan
Date: 2024/11/14
"""
import base64
import re
import time
from typing import List

import loguru
from bricks import const
from bricks.core import signals
from bricks.core.signals import Success, Failure
from bricks.lib.queues import RedisQueue
from bricks.plugins.storage import to_mongo
from bricks.spider import template
from bricks.db.redis_ import Redis
from bricks.spider.template import Config
from bricks.utils.fake import user_agent
from bs4 import BeautifulSoup

from config.config_info import MongoConfig, RedisConfig
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
            username= base64.b64decode(MongoConfig.username).decode("utf-8"),
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
                    kwargs={

                    },
                    layout=template.Layout(
                        factory={"batch_id": lambda x: BatchProcessor.get_batch_id(batch_key="Weekly"),
                                 "journal_abbrev": lambda x: x.lower() if x else x,
                                 }
                    )
                ),

            ],
            download=[
                template.Download(
                    url="{domain}/archive_list.htm",
                    headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                        'Connection': 'keep-alive',
                        'Referer': '{domain}',
                        'Upgrade-Insecure-Requests': '1',
                        'User-Agent': user_agent.chrome()
                    },
                    timeout=10,
                    max_retry=2,
                    ok={"response.status_code == 303": signals.Pass, "response.status_code == 404": signals.Pass,
                        "response.status_code == 500": signals.Pass}

                ),
                template.Download(
                    url="{domain}/{journal_abbrev}/archive_list.htm",
                    headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                        'Connection': 'keep-alive',
                        'Referer': '{domain}',
                        'Upgrade-Insecure-Requests': '1',
                        'User-Agent': user_agent.chrome()
                    },
                    timeout=10,
                    max_retry=2,
                    ok={"response.status_code == 303": signals.Pass, "response.status_code == 404": signals.Pass,
                        "response.status_code == 500": signals.Pass}

                ),
                template.Download(
                    url="{domain}/archive",
                    headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                        'Connection': 'keep-alive',
                        'Referer': '{domain}',
                        'Upgrade-Insecure-Requests': '1',
                        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0'
                    },
                    timeout=10,
                    max_retry=2,
                    ok={"response.status_code == 303": signals.Pass, "response.status_code == 404": signals.Pass,
                        "response.status_code == 500": signals.Pass}
                ),
                template.Download(
                    url="{domain}/{journal_abbrev}/archive",
                    headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                        'Connection': 'keep-alive',
                        'Referer': '{domain}',
                        'Upgrade-Insecure-Requests': '1',
                        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0'
                    },
                    timeout=10,
                    max_retry=2,
                    ok={"response.status_code == 303": signals.Pass, "response.status_code == 404": signals.Pass,
                        "response.status_code == 500": signals.Pass}
                ),
                template.Download(
                    url="{domain}/data/article/archive-list-data",
                    method="POST",
                    params={
                        "publisherId": "{journal_abbrev}"
                    },
                    headers={
                      'Accept': 'application/json, text/plain, */*',
                      'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                      'Connection': 'keep-alive',
                      'Content-Length': '0',
                      'Origin': 'https://cjm.dmu.edu.cn',
                      'Referer': 'https://cjm.dmu.edu.cn/zgwstxzz/archive',
                      'Sec-Fetch-Site': 'same-origin',
                      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0'
                    },
                    timeout=10,
                )
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
                        "path": "journal_issue_list",
                        "conn": self.mongo,
                        "row_keys": ["year", "issue", "issue_href", "journal_title"]
                    },
                    success=True
                )
            ],
            events={
                const.AFTER_REQUEST: [
                    template.Task(
                        func=self.is_success,

                    )
                ],

            },


        )

    @staticmethod
    def is_success(context: template.Context):
        seeds = context.seeds
        response = context.response
        api_file_path = r"D:\pyProject\hzcx\events\spiders\rhhz\input\json_api_list"

        # 读取文件并生成api_site_list
        with open(api_file_path, "r", encoding="utf-8") as f:
            api_site_list = [line.strip() for line in f.readlines()]

        if seeds['$config'] != 4 :
            if "第{{catalog.issue}}期" in response.text or seeds['journal_title'] in api_site_list:
                context.submit(
                    {**context.seeds, 'domain': seeds["domain"], 'publisher_id': seeds["journal_abbrev"],
                     "$config": 4})
                raise Success
            if response.status_code == 303 or response.status_code == 404 or "errorMsgData" in response.text:
                context.submit({**context.seeds, "$config": seeds['$config'] + 1})
                raise Success

        if (any(features in context.response.text for features in ['list-text', 'guokan-con-tab', 'phone-archive'])
                or seeds['$config'] == 4 and response.get("result") == "success"):
            return True

        raise Failure


    def _parse(self, context: template.Context) -> List:
        seeds = context.seeds
        response = context.response

        if seeds['$config'] == 4:
            return self._parse_json(context)
        html = response.text
        # 解析HTML
        soup = BeautifulSoup(html, 'html.parser')

        # 定位到特定div
        archive = soup.find('div', id='archive') or \
                  soup.find('div', class_='guokan-con-tab') or \
                  soup.find('ul', class_='archives_list')


        if not archive:
            loguru.logger.info(f"{seeds['journal_title']},"
                               f"{seeds['domain']}无法找到搜索年份选择框")
            assert False

        # 根据找到的元素选择不同的查询方式
        if 'archive' in archive.get('id', []):
            archive_list = archive.find_all('div', class_='arci-t')
        elif 'guokan-con-tab' in archive.get('class', []):
            archive_list = archive.find_all('td')
        else:
            archive_list = archive.find_all('p', class_='panelText')

        result = []
        issue_pattern = re.compile("([0-9a-zA-Z]+)期")
        year_pattern = re.compile(r"(article|custom)/(\d{4})/")

        for items in archive_list:
            # 获取所有<option>标签的值和文本
            a_tag = items.find('a')
            href = a_tag['href'] if a_tag else ''
            p_text = re.sub(r"\s+", "", items.text)

            # 只进行一次正则匹配，并存储匹配结果
            year_match  = re.search(year_pattern, href)
            issue_match = re.search(issue_pattern, p_text)

            year = year_match.group(2) if year_match else ''
            issue = issue_match.group(1) if issue_match else ''

            if not year:
                continue

            # 拼接 href
            issue_href = href if href.startswith('http') else seeds['domain'] + href if href.startswith("/") else f'https://{href}'

            result.append({
                'year': year,
                'issue': issue,
                'raw_issue': p_text,
                'issue_href': issue_href,
                'journal_title': seeds['journal_title'],
                "publisher_id": seeds['journal_abbrev'],
                'domain': seeds['domain'],
                "time_stamp": int(time.time() * 1000),
                "batch_id": seeds["batch_id"]
            })

        return result


    @staticmethod
    def _parse_json(context: template.Context) -> List:
        seeds = context.seeds
        response = context.response
        data = response.get('data', [])

        if not data:
            return []


        return [
            {
                'year': item['year'],
                'issue': item['issue'],
                'raw_issue': '',  # 固定为空字符串
                'issue_href': f"{seeds['domain']}/{seeds['journal_abbrev']}/article/{item['year']}/{item['issue']}",
                'journal_title': seeds['journal_title'],
                "publisher_id": seeds['journal_abbrev'],
                'domain': seeds['domain'],
                "time_stamp": int(time.time() * 1000),
                "batch_id": seeds["batch_id"]
            }
            for item in data
        ]

    def _init_seeds(self):
        iter_data = self.mongo.batch_data(
            collection= "journal_info",
            query={},
            projection={"journal_title": 1, "domain": 1, "journal_abbrev": 1},
            # count=10
        )
        return iter_data


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
        # "threshold": 10,
        # "scheme": "socks5"
    }

    spider = ArchiveViewing(
        concurrency=1,
        # download=requests_.Downloader,  # 这里是为了强制使用requests下载器
        **{"init.queue.size": 1000000},  # 设置redis最大投放数量
        task_queue=RedisQueue(
            host=RedisConfig.host,
            port=RedisConfig.port,
            password=base64.b64decode(RedisConfig.password).decode("utf-8"),
            database=RedisConfig.database,
        ),  # 定义种子来源
        # proxy=proxy  # 设置代理来源
    )

    spider.run(task_name='init')
    # spider.survey({"_id": "6747bfd6447b8e44e1a21f78", "batch_id": "2024-11-22", "domain": "http://journal.bit.edu.cn", "journal_abbrev": "zr", "journal_title": "北京理工大学学报自然版"})