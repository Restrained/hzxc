"""
archive_viewing -

Author: AllenWan
Date: 2024/11/14
"""
import re
from typing import List
from urllib.parse import urlparse

import loguru
from bricks import const
from bricks.core import signals
from bricks.core.signals import Success, Failure
from bricks.db.mongo import Mongo
from bricks.lib.queues import RedisQueue
from bricks.plugins import scripts
from bricks.plugins.make_seeds import by_csv
from bricks.plugins.storage import to_mongo
from bricks.spider import template
from bricks.db.redis_ import Redis
from bricks.spider.template import Config
from bricks.downloader import requests_
from bricks.utils.fake import user_agent
from bs4 import BeautifulSoup


from utils.url import normalize_url, get_base_url


class ArchiveViewing(template.Spider):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 种子、存储配置定义
        self.redis = Redis()
        self.mongo = Mongo(
            database='renHeHuiZhi',
        )

    @property
    def config(self) -> Config:
        return Config(
            init=[
                template.Init(
                    func=by_csv,
                    kwargs={
                        'path': r"D:\pyProject\hzcx\journalClassification\output\output_v3_redirect.csv",
                        "query": "select nameOfTheJournal,redirectedUrl as officialWebsite from <TABLE> where 第三方名称 = '北京仁和汇智信息技术有限公司'",
                        'batch_size': 5000
                    },
                    layout=template.Layout(
                        factory={"officialWebsite": lambda x: x if x.endswith('/') else x + '/'},

                    )
                ),

            ],
            download=[
                template.Download(
                    url="{officialWebsite}archive_list.htm",
                    headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                        'Connection': 'keep-alive',
                        'Referer': '{officialWebsite}',
                        'Upgrade-Insecure-Requests': '1',
                        'User-Agent': user_agent.chrome()
                    },
                    timeout=10,
                    max_retry=2,
                    ok={"response.status_code == 303": signals.Pass, "response.status_code == 404": signals.Pass}

                ),
                template.Download(
                    url="{officialWebsite}archive",
                    headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                        'Connection': 'keep-alive',
                        'Referer': '{officialWebsite}',
                        'Upgrade-Insecure-Requests': '1',
                        # 'User-Agent': user_agent.chrome()
                        "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0'
                    },
                    timeout=10,
                    max_retry=2,
                    ok={"response.status_code == 303": signals.Pass, "response.status_code == 404": signals.Pass}
                ),
                template.Download(
                    url="{officialWebsite}/data/article/archive-list-data",
                    method="POST",
                    params={
                        "publisherId": "{path}"
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
                        "path": "archive_viewing_list_v3",
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
                                "any(features in context.response.text for features in ['list-text', 'guokan-con-tab', 'phone-archive'])"
                            ]
                        }
                    )
                ],

            },


        )

    def set_href(self, context: template.Context):
        items = context.items

        for item in items:
            item['issue_href'] = f"/{item['issue_href']}/article/{item['year']}/{item['year']}"

        return items

    def is_success(self, context: template.Context):
        seeds = context.seeds
        response = context.response
        api_file_path = r"D:\pyProject\hzcx\renHeHuiZhi\chineseoptics\input\json_api_list"

        # 读取文件并生成api_site_list
        with open(api_file_path, "r", encoding="utf-8") as f:
            api_site_list = [line.strip() for line in f.readlines()]

        # 处理$config为0或1的情况
        if seeds['$config'] in {0, 1}:
            if "第{{catalog.issue}}期" in response.text or seeds['nameOfTheJournal'] in api_site_list:
                # 获取域名和路径
                parsed_url = urlparse(seeds['officialWebsite'])
                domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
                path = parsed_url.path.strip('/')

                # 提交新的context数据并抛出Success
                context.submit({**context.seeds, 'officialWebsite': domain, 'path': path, "$config": 2})
                raise Success

            if response.status_code == 303 or response.status_code == 404:
                context.submit({**context.seeds, "$config": 1})
                raise Success

            if any(features in context.response.text for features in ['list-text', 'guokan-con-tab', 'phone-archive']):
                return True

            raise Failure

        # 处理$config不为0或1的情况，如果不满足条件则抛出Failure
        if response.get("result") != "success":
            raise Failure

        return True


    def _parse(self, context: template.Context) -> List:
        seeds = context.seeds
        response = context.response

        if seeds['$config'] == 2:
            return self._parse_json(context)
        html = response.text
        # 解析HTML
        soup = BeautifulSoup(html, 'html.parser')

        # 定位到特定div
        archive = soup.find('div', id='archive') or \
                  soup.find('div', class_='guokan-con-tab') or \
                  soup.find('ul', class_='archives_list')


        if not archive:
            loguru.logger.info(f"{seeds['nameOfTheJournal']},"
                               f"{seeds['officialWebsite']}无法找到搜索年份选择框")
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

        base_url = get_base_url(seeds['officialWebsite'])  # 提前提取 base_url

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
            issue_href = href if href.startswith(('http', 'https')) else base_url + href if href.startswith("/") else f'https://{href}'

            result.append({
                'year': year,
                'issue': issue,
                'raw_issue': p_text,
                'issue_href': issue_href,
                'nameOfTheJournal': seeds['nameOfTheJournal'],
                'officialWebsite': seeds['officialWebsite']
            })

        return result



    def _parse_json(self, context: template.Context) -> List:
        seeds = context.seeds
        response = context.response
        data = response.get('data', [])

        if not data:
            return []

        base_url = get_base_url(seeds['officialWebsite'])

        return [
            {
                'year': item['year'],
                'issue': item['issue'],
                'raw_issue': '',  # 固定为空字符串
                'issue_href': f"{base_url}/{item['publisherId']}/article/{item['year']}/{item['issue']}",
                'nameOfTheJournal': seeds['nameOfTheJournal'],
                'officialWebsite': seeds['officialWebsite']
            }
            for item in data
        ]


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
        task_queue=RedisQueue(),  # 定义种子来源
        # proxy=proxy  # 设置代理来源
    )

    spider.run(task_name='all')
