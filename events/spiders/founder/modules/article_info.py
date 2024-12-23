#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/19 17:29
# @Author  : AllenWan
# @File    : article_info.py
# @Desc    ：
import base64
from http.client import responses
from typing import List, Dict

from bricks import const
from bricks.core import signals
from bricks.core.signals import Success, Failure
from bricks.db.redis_ import Redis
from bricks.lib.queues import RedisQueue
from bricks.plugins import scripts
from bricks.plugins.storage import to_mongo
from bricks.spider import template
from bricks.spider.template import Config, Context


from config.config_info import RedisConfig, MongoConfig
from db.mongo import MongoInfo
from events.data_conversion.clean_data import clean_text
from utils.dates import format_date
from utils.tool_box import generate_id


class ArticleInfo(template.Spider):
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
                ),

            ],
            download=[
                template.Download(
                    url="https://www.actasc.cn/rc-pub/front/journal-web/getData",
                    params={
                        "contentUrl": "{contentUrl}",
                        "timestamps": "1734579709469"
                    },
                    headers={
                        'accept': 'application/json, text/plain, */*',
                        'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                        'cookie': 'acw_tc=Hs2ZE3KNGRhxdoYPlkWY36cErWgSgPHkjDA0mHjkfp3ve_T4uChWCk9WVWzhN7mHbvKWpeTJOd4TDn9ccI_vBFxf9JVwjvxmoAAqXg**; _pk_testcookie..undefined=1; _pk_ses.1.ee79=1; language=zh; AntiLeech=2683300823; _pk_id.1.ee79=a09493479ba35d6a.1734751960.2.1734925629.1734925603.',
                        'language': 'zh',
                        'priority': 'u=1, i',
                        'referer': 'https://zgtl.crj.com.cn/thesisDetails',
                        'sec-ch-ua': '"Microsoft Edge";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"Windows"',
                        'sec-fetch-dest': 'empty',
                        'sec-fetch-mode': 'cors',
                        'sec-fetch-site': 'same-origin',
                        # 'siteid': '12',
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0'
                    },
                    ok={"response.status_code == 500": signals.Pass}
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
                        "path": "founder_article_info",
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
                ],
            }
        )

    def _parse(self, context: Context) -> List[dict]:
        author_info = []
        institution_info = []
        seeds = context.seeds
        response = context.response
        lang = response.get("defaultlang")

        contrib_group = response.get("contribgroup")
        author_list = contrib_group.get("author")
        for authors in author_list:
            author_name, en_author_name = '', ''
            names = authors.get("name")
            if not names:
                names = authors.get("stringName")
            for name_item in names:
                if name_item.get("lang") == "zh":
                    surname = name_item.get("surname")
                    given_name = name_item.get("givenname")
                    if given_name:
                        author_name = surname + given_name
                    else:
                        author_name = surname

                elif name_item.get("lang") == "en":
                    surname = name_item.get("surname")
                    given_name = name_item.get("givenname")
                    if given_name:
                        en_author_name = surname + ' ' + given_name
                    else:
                        en_author_name = surname

            aff_info = authors.get("aff")
            aff_ids = []
            for aff_item in aff_info:
                aff_ids.append(aff_item.get("text"))
            role = authors.get("role")
            first = authors.get("first")
            about_author = ''
            if first:
                about_author = first[0].get("text")
            corresp = authors.get("corresp")
            if corresp:
                about_author = corresp[0].get("text")
            note = authors.get("note")
            if note:
                about_author = note[0].get("text")
            email = authors.get("email", '')
            orc_id = authors.get("orcid", '')
            deceased = authors.get("deceased")
            author_info.append(
                {
                    "author_id": generate_id(author_name + email + orc_id),
                    "author_name": author_name,
                    "en_author_name": en_author_name,
                    "role": role,
                    "about_author": about_author,
                    "email": email,
                    "deceased": deceased,
                    "aff_ids": aff_ids,
                    "orc_id": orc_id,
                }
            )
        aff_list = contrib_group.get("aff")
        for aff_item in aff_list:
            aff_intro = aff_item.get("intro")
            aff_label, aff_name, en_aff_name = 1, '', ''
            address, location = '', ''
            for affs in aff_intro:

                if affs.get("lang") == "zh":
                    aff_label = affs.get("label")
                    aff_name = affs.get("text").strip()
                    if ',' in aff_name:
                        address, location = aff_name.split(",", maxsplit=1)
                    elif '，' in aff_name:
                        address, location = aff_name.split("，", maxsplit=1)
                    else:
                        address, location = aff_name, ''
                elif affs.get("lang") == "en":
                    en_aff_name = affs.get("text").strip()
            institution_info.append({
                "id": generate_id(aff_name),
                "institution_id": "",
                "aff_label": aff_label,
                "aff_name": address,
                "en_aff_name": en_aff_name,
                "location": location
            })
        executive_editor = contrib_group.get("executiveeditor")

        keywords = []
        en_keywords = []
        keywords_list = response.get("keyword")
        for keyword_item in keywords_list:
            if keyword_item.get("lang") == "zh":
                keyword_data = keyword_item.get("csv_data")
                for data in keyword_data:
                    keyword = data[0].get("csv_data")
                    keywords.append(keyword)
            elif keyword_item.get("lang") == "en":
                keyword_data = keyword_item.get("csv_data")
                for data in keyword_data:
                    en_keyword = data[0].get("csv_data")
                    en_keywords.append(en_keyword)

        citation_list = []
        ref_list = response.get("reflist")
        if ref_list:
            ref_data = ref_list.get("csv_data")
            for data in ref_data:
                citation_order = data.get("id")
                citation_label = data.get("label")
                citation = data.get("citation")
                citation_content = ''
                citation_lang = citation[0].get("lang")
                text_list = citation[0].get("text")
                for texts in text_list:
                    text = texts.get("csv_data")
                    name = texts.get("name")
                    if name == "text":
                        if isinstance(text, list):
                            for item in text:
                                inner_text = item['csv_data']
                                citation_content += inner_text
                        else:
                            citation_content += text
                origin_title = citation[0].get("title")
                citation_list.append({
                    "citation_order": citation_order,
                    "citation_label": citation_label,
                    "citation_content": citation_content,
                    "citation_lang": citation_lang,
                    "origin_title": origin_title,
                })

        articlemeta = response.get("articlemeta")
        publisher_id = articlemeta.get("publisherid")
        doi = articlemeta.get("doi")
        fund_info = []
        funding_group = articlemeta.get("fundinggroup")
        for funding_item in funding_group:
            if funding_item.get("lang") == "zh":
                text = funding_item.get("text")
                for inner_item in text:
                    fund_txt = inner_item.get("csv_data")
                    fund = fund_txt.split('；')
                    for fund_item in fund:
                        fund_info.append(
                            {
                                "fund_id": generate_id(fund_item),
                                "fund_name": fund_item,
                                "en_name": ''
                            }
                        )

        history = articlemeta.get("history")
        received_date = history.get("received")
        opub_date = history.get("opub")
        ppub_date = history.get("ppub")
        journal_title = response.get("journalTitle")
        issue = response.get("issue")
        volume = response.get("volume")
        journal_info = [
            {
                "journal_id": generate_id(seeds["sourcePublicationName"]),
                "journal_title": seeds["sourcePublicationName"],
                "en_name": seeds["enSourcePublicationName"],
                "type": "J"
            }
        ]

        article_title = clean_text(seeds["resName"])
        en_article_title = clean_text(seeds["enResName"])
        if not article_title and en_article_title:
            article_title = en_article_title
        achievement_info = [
            {
                "article_id": generate_id(article_title),
                "article_title": article_title,
                "en_article_title": en_article_title,
                "executive_editor": executive_editor,
                "source": seeds["sourcePublicationName"],
                "abstracts": seeds["summary"],
                "en_abstracts": seeds["enSummary"],
                "keywords": keywords,
                "en_keywords": en_keywords,
                "lang": lang,
                "status": '',
                "type": "J",
                "publisher_id": publisher_id,
                "doi": doi,
                "received_date": received_date,
                "opub_date": opub_date,
                "ppub_date": ppub_date,
                "journal_title": journal_title,
                "publish_date": format_date(seeds["publishDate"]) ,
                "revised_date": seeds["revisedDate"],
                "accept_date": seeds["acceptedDate"],
                "year": seeds["year"],
                "issue": issue,
                "volume": volume,
                "page_range": seeds['pageOrElocationId'],
                "contentUrl": seeds["contentUrl"],
            }
        ]

        # [{"id": "", "name": "", "org": "", "org_id": ""}, {"id": "", "name": "", "org": "", "org_id": ""}]
        authors = []
        publish = []
        work_at = []
        if author_info:
            for index, author_item in enumerate(author_info):
                aff_ids = author_item.get("aff_ids")
                publish.append({
                    "author_id": author_item.get("author_id"),
                    "achievement_id": achievement_info[0].get("article_id"),
                    "order_of_authors": index + 1,
                    "corresponding_author": '是' if "corresp" in author_item["role"] else '否'
                })
                if institution_info:
                    for institution_item in institution_info:
                        aff_label = institution_item.get("aff_label")
                        if aff_label in aff_ids:
                            authors.append({
                                "id": author_item.get("author_id"),
                                "name": author_item.get("author_name"),
                                "org": institution_item.get("aff_name"),
                                "org_id": institution_item.get("id"),

                            })
                            work_at.append(
                                {
                                    "author_id": author_item.get("author_id"),
                                    "institution_id": institution_item.get("id"),
                                }
                            )
        achievement_info = [{**item, "authors": authors}  for item in achievement_info]

        published_in  = []
        claimed_by = []
        supported_by = []
        for achievement_item in achievement_info:
            for journal_item in  journal_info:
                published_in.append({
                    "achievement_id": achievement_item.get("article_id"),
                    "journal_id": journal_item.get("journal_id"),
                    "year": achievement_item.get("year"),
                    "volume": achievement_item.get("volume"),
                    "period": achievement_item.get("issue"),
                    "page_range": achievement_item.get("page_range"),
                })
            if institution_info:
                for institution_item in institution_info:
                    claimed_by.append({
                        "achievement_id": achievement_item.get("article_id"),
                        "institution_id": institution_item.get("id"),
                    })
            if fund_info:
                for fund_item in fund_info:
                    supported_by.append({
                        "achievement_id": achievement_item.get("article_id"),
                        "fund_id": fund_item.get("fund_id"),
                    })

        source_info = [
            {
                "article_id": generate_id(article_title),
                "article_title": article_title,
                "journal_title": journal_title,
                "json": response.text

            }

        ]
        self.self_pipeline(source_info, "founder_source_json")
        self.self_pipeline(author_info, "founder_author_info")
        self.self_pipeline(institution_info, "founder_institution_info")
        self.self_pipeline(fund_info, "founder_fund_info")
        self.self_pipeline(journal_info, "founder_venue")
        self.self_pipeline(publish, "founder_publish")
        self.self_pipeline(published_in, "founder_published_in")
        self.self_pipeline(claimed_by, "founder_claimed_by")
        self.self_pipeline(supported_by, "founder_supported_by")
        self.self_pipeline(work_at, "founder_work_at")

        return achievement_info


    def self_pipeline(self, items: List[Dict], table_name, row_keys=None):
        # 写自己的存储逻辑
        to_mongo(
                items=items,
                path=table_name,
                conn=self.mongo,
                row_keys=row_keys,
        )


    def _init_seeds(self):

        iter_data = self.mongo.batch_data(
            collection="article_list_founder",
            query={ "contentUrl" : { "$ne" : None } },
            projection={
                "contentUrl": 1,
                "enSourcePublicationName": 1,
                "sourcePublicationName": 1,
                "summary": 1,
                "enSummary": 1,
                "publishDate": 1,
                "revisedDate": 1,
                "acceptedDate": 1,
                "pageOrElocationId": 1,
                "year": 1,
                "resName": 1,
                "enResName": 1,

            }
        )
        return iter_data

    def is_success(self, context: Context):
        response = context.response
        if response.status_code == 500:
            raise Success
        elif response.get("titlegroup") :
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

    spider = ArticleInfo(
        concurrency=50,
        # downloader=requests_.Downloader(),
        **{"init.queue.size": 5000000},
        task_queue=RedisQueue(
            host=RedisConfig.host,
            port=RedisConfig.port,
            password=base64.b64decode(RedisConfig.password).decode("utf-8"),
            database=RedisConfig.database,
        ),
        proxy=proxy
    )
    spider.run(task_name='spider')

