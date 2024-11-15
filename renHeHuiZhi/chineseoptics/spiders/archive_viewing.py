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
from bricks.core.signals import Success
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
                        "path": "archive_viewing_list",
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
        with open(api_file_path, "r", encoding="utf-8") as f:
            lines  = f.readlines()
        api_site_list = [line.strip() for line in lines]
        if seeds['$config'] in (0,1):
            # if any(journal_name == seeds['nameOfTheJournal'] for journal_name in ['中国微生态学杂志', '环境化学', '生态毒理学报']) :
            if "第{{catalog.issue}}期" in response.text or any(journal_name == seeds['nameOfTheJournal'] for journal_name in api_site_list):
                # 解析 URL
                url = seeds['officialWebsite']
                parsed_url = urlparse(url)

                # 获取域名部分
                domain = parsed_url.scheme + "://" + parsed_url.netloc

                # 获取路径部分并去掉结尾的斜杠
                path = parsed_url.path.strip('/')

                context.submit({**context.seeds,'officialWebsite': domain, 'path': path, "$config": 2})
                raise Success

            if response.status_code == 303 or response.status_code == 404:
                context.submit({**context.seeds, "$config": 1})
                raise Success

            if any(features in context.response.text for features in ['list-text', 'guokan-con-tab', 'phone-archive']):
                return True
            else:
                return False
        else:
            status_code = response.get("result")
            if status_code == "success":
                return True
            else:
                return False

    def _parse(self, context: template.Context) -> List:
        result = []
        seeds = context.seeds
        response = context.response

        if seeds['$config'] == 2:
            return self._parse_v4(context)
        html = response.text
        # 解析HTML
        soup = BeautifulSoup(html, 'html.parser')

        # 定位到特定div
        archive = soup.find('div', id='archive')


        if not archive:
            return self._parse_v2(context)

        # 定位到<select id="s1" class="search_year form-control">
        archive_list = archive.find_all('div', class_='arci-t')


        if not archive_list:
            loguru.logger.info(f"{seeds['nameOfTheJournal']},"
                               f"{seeds['officialWebsite']}无法找到搜索年份选择框")
            return result
        for items in archive_list:
            # 获取所有<option>标签的值和文本
            a_tag = items.find('a')
            href = a_tag['href'] if a_tag else None
            p_text = items.text.strip()
            pattern = re.compile("(\d+)年(.+)期")
            year, issue = re.search(pattern, p_text).groups()
            base_url = get_base_url(seeds['officialWebsite'])
            issue_href = base_url + href
            result.append({
                'year': year,
                'issue': issue,
                'raw_issue': p_text,
                'issue_href': issue_href,
                'nameOfTheJournal': seeds['nameOfTheJournal'],
                'officialWebsite': seeds['officialWebsite']
            })

        return result

    def _parse_v2(self, context: template.Context) -> List:
        result = []
        seeds = context.seeds
        response = context.response
        html = response.text
        # 解析HTML
        soup = BeautifulSoup(html, 'html.parser')

        # 定位到特定div
        archive = soup.find('div', class_='guokan-con-tab')

        if not archive:
            return self._parse_v3(context)

        # 定位到<select id="s1" class="search_year form-control">
        archive_list = archive.find_all('td')
        if not archive_list:
            loguru.logger.info(f"{seeds['nameOfTheJournal']},"
                               f"{seeds['officialWebsite']}无法找到搜索年份选择框")
            return result
        for items in archive_list:
            # 获取所有<option>标签的值和文本
            a_tag = items.find('a')
            href = a_tag['href'] if a_tag else ''
            p_text = re.sub(r"\s+", "", items.text)
            issue_pattern = re.compile("([0-9a-zA-Z]+)期")
            year_pattern = re.compile(r"(article|custom)/(\d+)/")
            if href:
                year_result = re.search(year_pattern, href)
                if year_result:
                    year = year_result.group(2)
                else:
                    year = ''
            else:
                year = ''
            issue_result = re.search(issue_pattern, p_text)
            if issue_result:
                issue = issue_result.group(1)
            else:
                issue = ''
            base_url = get_base_url(seeds['officialWebsite'])
            issue_href = base_url + href
            result.append({
                'year': year,
                'issue': issue,
                'raw_issue': p_text,
                'issue_href': issue_href,
                'nameOfTheJournal': seeds['nameOfTheJournal'],
                'officialWebsite': seeds['officialWebsite']
            })

        return result

    def _parse_v3(self, context: template.Context) -> List:
        result = []
        seeds = context.seeds
        response = context.response
        html = response.text
        # 解析HTML
        soup = BeautifulSoup(html, 'html.parser')

        # 定位到特定div
        archive = soup.find('ul', class_='archives_list')

        if not archive:
            return result

        # 定位到<select id="s1" class="search_year form-control">
        archive_list = archive.find_all('p', class_='panelText')
        if not archive_list:
            loguru.logger.info(f"{seeds['nameOfTheJournal']},"
                               f"{seeds['officialWebsite']}无法找到搜索年份选择框")
            return result
        for items in archive_list:
            # 获取所有<option>标签的值和文本
            a_tag = items.find('a')
            href = a_tag['href'] if a_tag else None
            p_text = re.sub(r"\s+", "", items.text)
            issue_pattern = re.compile("([0-9a-zA-Z]+)期")
            year_pattern = re.compile("(article|custom)/(\d+)/")
            year_result = re.search(year_pattern, href)
            if year_result:
                year = re.search(year_pattern, href).group(2)
            else:
                year = ''
            issue_result = re.search(issue_pattern, p_text)
            if issue_result:
                issue = issue_result.group(1)
            else:
                issue = ''
            base_url = get_base_url(seeds['officialWebsite'])
            issue_href = base_url + href

            result.append({
                'year': year,
                'issue': issue,
                'raw_issue': p_text,
                'issue_href': issue_href,
                'nameOfTheJournal': seeds['nameOfTheJournal'],
                'officialWebsite': seeds['officialWebsite']
            })

        return result

    def _parse_v4(self, context: template.Context) -> List:
        result = []
        seeds = context.seeds
        response = context.response
        data = response.get('data')
        if data:
            for items  in data:
                year = items['year']
                issue = items['issue']
                publisherId = items['publisherId']
                raw_issue = ''
                base_url = get_base_url(seeds['officialWebsite'])
                issue_href = base_url + f"/{publisherId}/article/{year}/{issue}"
                result.append({
                    'year': year,
                    'issue': issue,
                    'raw_issue': raw_issue,
                    'issue_href': issue_href,
                    'nameOfTheJournal': seeds['nameOfTheJournal'],
                    'officialWebsite': seeds['officialWebsite']
                })

        else:
            return result



        return result

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

    spider.run(task_name='spider')
