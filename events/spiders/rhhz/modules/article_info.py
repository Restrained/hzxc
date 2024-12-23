#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/18 16:01
# @Author  : AllenWan
# @File    : article_info.py
# from typing import List
import base64
import json
import re
from typing import List, Dict
from itertools import zip_longest
from bricks import const
from bricks.core import signals
from bricks.core.signals import Success, Failure
from bricks.db.mongo import Mongo
from bricks.db.redis_ import Redis
from bricks.downloader import requests_
from bricks.lib.queues import RedisQueue
from bricks.plugins.make_seeds import by_csv
from bricks.plugins.storage import to_mongo
from bricks.spider import template
from bricks.spider.template import Config
from bs4 import BeautifulSoup

from config.config_info import RedisConfig, MongoConfig
from db.mongo import MongoInfo
from utils.dates import timestamp_to_date


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
        return template.Config(
            init=[
                template.Init(
                    func=by_csv,
                    kwargs={
                        "path": r"C:\Users\PY-01\Documents\withingking\spider\article_list_incremental.csv_data",
                        "query": "select article_id, domain,  article_url from <TABLE> where article_url != '' ",
                        "batch_size": 5000
                    }
                )
            ],
            download=[
                template.Download(
                    url= "{article_url}",
                    headers={
                        'Accept': 'application/json, text/plain, */*',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                        'Content-Length': '0',
                        'Origin': '{domain}',
                        'Proxy-Connection': 'keep-alive',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0'
                    },
                    timeout=20,
                    max_retry=2,

                    ok={"response.status_code == 404": signals.Pass, "response.status_code == 403": signals.Pass, "response.status_code == 502": signals.Pass}
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
                        "path": "article_info",
                        "conn": self.mongo,
                    },
                    success=True,
                )
            ],
            events={
                const.BEFORE_REQUEST: [
                    template.Task(
                        func=self.clear_url
                    )
                ],
                const.AFTER_REQUEST: [
                    template.Task(
                        func=self.is_success,
                    )
                ]
            }
        )

    def clear_url(self, context: template.Context):
        seeds = context.seeds
        request = context.request
        url = seeds['article_url']
        if ' ' in url:
            request.url = url.replace(' ', '%20')


    def is_success(self, context: template.Context):
        seeds = context.seeds
        response = context.response



        if  response.status_code == 404:
            if seeds['article_url'].startswith('https://'):
                context.submit({**context.seeds, "article_url": seeds['article_url'].replace("https", "http")})
            raise Success
        elif response.status_code == 403 or not response.content:
            raise Success

        soup = BeautifulSoup(response.text, 'lxml')
        # 获取 <title> 标签的文本内容
        title = soup.title.string
        if title == "Error 404":
            raise Success

        if 'articleCn' in context.response.text:
            return True
        raise Failure


    def _parse(self, context: template.Context):
        def safe_find(meta_name, attr="content"):
            """Helper function to safely find meta tags."""
            tag = soup.find('meta', attrs={'name': meta_name})
            return tag.get(attr, "").strip() if tag else ""

        def safe_find_text(parent, tag_name, class_name=None, attr_name=None):
            """Helper function to safely extract text from a tag."""
            if class_name:
                tag = parent.find(tag_name, class_=class_name) if parent else None
            else:
                tag = parent.find(tag_name) if parent else None

            if attr_name:
                find_result = tag.get(attr_name) if tag else ""

            else:
                find_result = tag.text if tag else ""

            return find_result.strip() if find_result else find_result

        def safe_find_text_list(grandparent, parent_tag, tag_name, class_name=None, attr_name=None, parent_class_name=None):
            """从祖父级别获取其下类似li标签下中指定标签的文字"""
            if not parent_class_name:
                tag_list = grandparent.find_all(parent_tag) if grandparent else []
            else:
                tag_list = grandparent.find_all(parent_tag, class_=parent_class_name) if grandparent else []
            text_list = [safe_find_text(items, tag_name, class_name, attr_name) for items in tag_list] if tag_list else []
            return text_list

        def parse_dates(related_items):
            mapping_tables = {
                "收稿日期:": "received_date",
                "修回日期:":"revised_date",
                "网络出版日期:": "online_date",
                "刊出日期:": "online_date",
            }
            key_list = safe_find_text_list(related_items, "li", 'b')
            value_list = safe_find_text_list(related_items, "li", 'span')
            return {mapping_tables[key]: value for key, value in zip(key_list, value_list) if key in mapping_tables.keys()}

        def longer_list(list1, list2):
            # 比较两个列表的长度，返回较长的一个
            return list1 if len(list1) > len(list2) else list2

        # 请求上下文变量的声明
        seeds = context.seeds
        response = context.response


        # 构造解析对象soup
        soup = BeautifulSoup(response.text, 'html.parser')

        # 中英文作者、地址、基金等的标签块提前定位
        article_en = soup.find("div", class_="articleEn")
        article_cn = soup.find("div", class_="articleCn")

        # 获取页面收稿日期、收稿日期、网络出版日期
        article_related = soup.find("div", class_="article-related")
        dates = parse_dates(article_related)

        # 多次使用的主键提前解析出来
        title = safe_find("dc.title")
        keywords = safe_find("citation_keywords")
        publish_date = safe_find("dc.date")
        year = safe_find("citation_year")
        volume = safe_find("citation_volume")
        issue = safe_find("citation_issue")
        page_range = f"{safe_find('citation_firstpage')}-{safe_find('citation_lastpage')}"
        article_id = safe_find('citation_id')
        publisher = safe_find("dc.publisher")
        doi = safe_find("citation_doi")
        journal_title = safe_find('citation_journal_title')
        issn = safe_find("citation_issn")
        authors = safe_find("citation_authors")
        source = safe_find("dc.publisher")
        lang = safe_find("dc.language")
        url = safe_find("hw.identifier")
        journal_abbrev = safe_find('citation_journal_abbrev')
        if not lang:
            lang_search = soup.find("input", id="language")
            if lang_search:
                lang = lang_search.get("value")

        article_meta_pattern = re.compile(r"article_meta_data='(.*?)';")
        article_meta_search = re.search(article_meta_pattern, response.text)

        # 构造附件pdf下载链接
        pdf_link = f"{seeds['domain']}/article/exportPdf?id={article_id}"

        if article_meta_search:
            article_meta_data_base64 = article_meta_search.group(1)
            article_meta_decoded_bytes = base64.b64decode(article_meta_data_base64)  # 返回的是字节数据
            article_meta_decoded_string = article_meta_decoded_bytes.decode('utf-8')  # 将字节数据转换为字符串
            article_meta_data = json.loads(article_meta_decoded_string)
            item_dict, author_list, citation_list, reference_list = self._parse_json(article_meta_data, article_id, doi, journal_title, issn, authors, source, url, journal_abbrev, pdf_link)

            result = [item_dict]
        elif not article_cn:
            # 该部分引用信息都要走单独接口
            citation_cn_text = ''
            citation_en_text = ''

            article_top = soup.find("div", class_="article-top-left")
            article_top_info_en = article_top.find("div", class_="info-en")

            # 中英文摘要所在的标签定位，用于后续解析
            article_abstract = soup.find("div", class_="article-abstract abstract-cn")
            article_abstract_en = soup.find("div", class_="article-abstract abstract-en")

            article_box = soup.find('li', class_='article-box current')
            article_box_list = article_box.find_all('div', class_='panel panel-default')

            en_keywords = ''
            cn_funds = ''
            en_funds = ''

            author_list = []
            for div_box in article_box_list:
                box_title = div_box.find('a', class_='tablisttit')
                if box_title:
                    box_title_text = box_title.text.strip()
                    if box_title_text == '摘要':
                        info_en = div_box.find("div", class_="info-en")

                        # 提取英文关键词列表
                        article_keyword_tags = info_en.find("ul", class_="article-keyword") if info_en else None
                        en_keyword_list = safe_find_text_list(article_keyword_tags, 'li', 'a')
                        en_keywords = ','.join(en_keyword_list) if en_keyword_list else ""
                    elif box_title_text == '作者及机构信息':
                        info_cn = div_box.find("div", class_="info-cn")
                        info_en = div_box.find("div", class_="info-en")

                        # 基金中英文信息解析提取
                        cn_funds_tag = info_cn.find("li", class_="com-author-info article-fundPrjs")
                        en_funds_tag = info_en.find("li", class_="com-author-info article-fundPrjs")

                        if cn_funds_tag and cn_funds_tag.b:
                            cn_funds_tag.b.decompose()
                        if en_funds_tag and en_funds_tag.b:
                            en_funds_tag.b.decompose()

                        cn_funds = safe_find_text(cn_funds_tag, "div", "com-author-info")
                        en_funds = safe_find_text(en_funds_tag, "div", "com-author-info")

                        # 作者中英文信息，包括地址、邮箱、简介等信息，匹配关联
                        authors_cn = info_cn.find("ul", class_="article-author")
                        authors_en = info_en.find("ul", class_="article-author")

                        about_author_cn = info_cn.find("ul", class_="about-author")
                        about_author_en = info_en.find("ul", class_="about-author")

                        author_name_cn_list = safe_find_text_list(authors_cn, "li", 'a')
                        author_code_cn_list = safe_find_text_list(authors_cn, "li", 'sup')
                        author_mail_cn_list = safe_find_text_list(authors_cn, "li", 'a', attr_name='csv_data-relate')
                        author_name_en_list = safe_find_text_list(authors_en, "li", 'a')


                        clear_author_code_list = []
                        for code_item in author_code_cn_list:

                            if ',' in code_item:
                                code_item = code_item.split(',')
                            elif not code_item:
                                code_item = [0]
                            else:
                                code_item = [code_item]
                            clear_author_code_list.append(code_item)


                        address_cn_list = safe_find_text_list(about_author_cn, 'li', 'p', parent_class_name='article-author-address')
                        address_code_list = safe_find_text_list(about_author_cn, 'li', 'span', parent_class_name='article-author-address')

                        address_en_list = safe_find_text_list(about_author_en, 'li', 'p', parent_class_name='article-author-address')
                        clean_address_code_list = list(
                            map(lambda x: x.replace('.', ''),
                                address_code_list)) if address_code_list else address_code_list
                        if clean_address_code_list:
                            for cn_name, en_name, author_code_list, mail in zip_longest(
                                    author_name_cn_list, author_name_en_list, clear_author_code_list, author_mail_cn_list,
                                    ):
                                for cn_address, en_address, address_code in zip_longest(address_cn_list, address_en_list,
                                                                                        clean_address_code_list):
                                    if len(list(zip_longest(address_cn_list, address_en_list, clean_address_code_list))) == 1:
                                        author_list.append({
                                            "article_id": article_id,
                                            "doi": doi,
                                            "journal_title": journal_title,
                                            "issn": issn,
                                            "cn_name": cn_name,
                                            "en_name": en_name,
                                            "mail": mail,
                                            "cn_address": cn_address,
                                            "en_address": en_address,
                                            "cn_about_author": "",
                                            "en_about_author": "",

                                        })
                                    elif address_code in author_code_list:
                                        author_list.append({
                                            "article_id": article_id,
                                            "doi": doi,
                                            "journal_title": journal_title,
                                            "issn": issn,
                                            "cn_name": cn_name,
                                            "en_name": en_name,
                                            "mail": mail,
                                            "cn_address": cn_address,
                                            "en_address": en_address,
                                            "cn_about_author": "",
                                            "en_about_author": "",

                                        })
                        else :
                            for cn_name, en_name, author_code_list, mail in zip_longest(
                                    author_name_cn_list, author_name_en_list, clear_author_code_list,
                                    author_mail_cn_list,
                            ):
                                author_list.append({
                                    "article_id": article_id,
                                    "doi": doi,
                                    "journal_title": journal_title,
                                    "issn": issn,
                                    "cn_name": cn_name,
                                    "en_name": en_name,
                                    "mail": mail,
                                    "cn_address": '',
                                    "en_address": '',
                                    "cn_about_author": "",
                                    "en_about_author": "",

                                })

            # 基础信息输出
            item_dict = {
                "article_id": article_id,
                "title": title,
                "en_title": safe_find_text(article_top_info_en, "h2", class_name='article-tit'),
                "doi": doi,
                "authors": authors,
                "source": source,
                "abstract": article_abstract.text.strip() if article_abstract else "",
                "abstract_en": article_abstract_en.text.strip() if article_abstract_en else "",
                "keywords": keywords,
                "en_keywords": en_keywords,
                "lang": lang,
                "status": "",
                "article_type": "J",
                "publish_date": publish_date,
                "year": year,
                "volume": volume,
                "issue": issue,
                "page_range": page_range,
                "url": url,
                "issn": issn,
                "publisher": publisher,
                "journal_title": journal_title,
                "journal_abbrev": journal_abbrev,
                "cn_funds": cn_funds,
                "en_funds": en_funds,
                "pdf_link": pdf_link,
                "openAccess": 0,

            }

            # 基础信息与页面3个日期信息合并
            item_dict = item_dict | dates
            result = [item_dict]

            # 引用文献信息提取，并整理成list(dict)格式
            reference_tab = soup.find("table", class_="reference-tab")
            raw_reference_list = safe_find_text_list(reference_tab, "tr", 'td', class_name='td2')
            reference_list = []
            for index, item in enumerate(raw_reference_list):
                reference_no = index + 1
                reference_list.append({
                    "article_id": article_id,
                    "doi": doi,
                    "journal_title": journal_title,
                    "issn": issn,
                    "reference_no": reference_no,
                    "reference": item,

                })


            # 将引用信息独立出去，方便后续补爬
            citation_list = [{
                "article_id": article_id,
                "doi": doi,
                "journal_title": journal_title,
                "issn": issn,
                "citation_cn": citation_cn_text,
                "citation_en": citation_en_text,
            }]


        else:
            # 解析中英文引用信息
            citation_cn = soup.find("div", class_="citationCn")
            citation_cn_text = safe_find_text(citation_cn, "div", 'copyCitationInfo')
            citation_en = soup.find("div", class_="citationEn")
            citation_en_text = safe_find_text(citation_en, "div", 'copyCitationInfo')

            # 中英文摘要所在的标签定位，用于后续解析
            article_abstract = soup.find("div", class_="article-abstract")
            article_abstract_en = soup.find("div", class_="article-info-en")



            # 英文关键词所在标签提取定位，用于后续解析
            article_keyword_tags = soup.find("ul", class_="article-keyword article-info-en")
            en_keyword_list = safe_find_text_list(article_keyword_tags, 'li', 'a')


            # 基金中英文信息解析提取
            cn_funds_tag = article_cn.find("div", class_="article-fundPrjs") if article_cn else None
            en_funds_tag = article_en.find("div", class_="article-fundPrjs") if article_en else None

            if cn_funds_tag and cn_funds_tag.b:
                cn_funds_tag.b.decompose()
            if en_funds_tag and en_funds_tag.b:
                en_funds_tag.b.decompose()

            cn_funds = safe_find_text(cn_funds_tag, "div", "com-author-info")
            en_funds = safe_find_text(en_funds_tag, "div", "com-author-info")


            # 基础信息输出
            item_dict = {
                "article_id": article_id,
                "title": title,
                "en_title": safe_find_text(article_en, "h2"),
                "doi": doi,
                "authors": authors,
                "source": source,
                "abstract": article_abstract.text.split('摘要:')[-1].strip() if article_abstract else "",
                "abstract_en": article_abstract_en.text.split('Abstract:')[-1].strip() if article_abstract_en else "",
                "keywords": keywords,
                "en_keywords": ','.join(en_keyword_list) if en_keyword_list else "",
                "lang": lang,
                "status": "",
                "article_type": "J",
                "publish_date": publish_date,
                "year": year,
                "volume": volume,
                "issue": issue,
                "page_range": page_range ,
                "url": url,
                "issn": issn,
                "publisher": publisher,
                "journal_title": journal_title,
                "journal_abbrev": journal_abbrev ,
                "cn_funds": cn_funds,
                "en_funds": en_funds,
                "pdf_link": pdf_link,
                "openAccess": 0,

            }

            # 基础信息与页面3个日期信息合并
            item_dict = item_dict | dates
            result = [item_dict]

            # 引用文献信息提取，并整理成list(dict)格式
            reference_tab = soup.find("table", class_="reference-tab")
            raw_reference_list = safe_find_text_list(reference_tab, "tr", 'td', class_name='td2')
            reference_list = []
            for index, item in enumerate(raw_reference_list):
                reference_no = index + 1
                reference_list.append({
                    "article_id": article_id,
                    "doi": doi,
                    "journal_title": journal_title,
                    "issn": issn,
                    "reference_no": reference_no,
                    "reference": item,

                })

            # 作者中英文信息，包括地址、邮箱、简介等信息，匹配关联
            authors_en = article_en.find("ul", class_="article-author") if article_en else None
            authors_cn = article_cn.find("ul", class_="article-author") if article_cn else None

            about_author_cn = article_cn.find("ul", class_="about-author") if article_cn else None
            about_author_en = article_en.find("ul", class_="about-author")  if article_en else None

            author_name_cn_list = safe_find_text_list(authors_cn, "li", 'a')
            author_name_cn_list =  list(filter(None, author_name_cn_list))
            author_code_cn_list = safe_find_text_list(authors_cn, "li", 'span')
            author_code_cn_list = list(filter(None, author_code_cn_list))
            if not author_code_cn_list:
                author_code_cn_list = safe_find_text_list(authors_cn, "li", 'sup')
            author_mail_cn_list = safe_find_text_list(authors_cn, "li", 'a', attr_name='csv_data-relate')
            author_mail_cn_list = list(filter(None, author_mail_cn_list))
            author_name_en_list = safe_find_text_list(authors_en, "li", 'a')
            author_name_en_list = list(filter(None, author_name_en_list))
            author_code_en_list = safe_find_text_list(authors_en, "li", 'span')
            author_code_en_list = list(filter(None, author_code_en_list))
            if not author_code_en_list:
                author_code_en_list = safe_find_text_list(authors_en, "li", 'sup')
            about_author_cn_list = safe_find_text_list(about_author_cn, "h6", 'p', parent_class_name='com-introduction',class_name='intro-list')
            about_author_en_list = safe_find_text_list(about_author_en, "h6", 'p', parent_class_name='com-introduction', class_name='intro-list')
            author_name_en_list = list(filter(lambda x: x != '' ,author_name_en_list))
            clear_author_code_list = []
            author_code_list = author_code_cn_list if author_code_cn_list else author_code_en_list
            if author_code_cn_list and author_code_en_list:
                author_code_list = longer_list(author_code_cn_list, author_code_en_list)

            for code_item in author_code_list:

                if ',' in code_item:
                    code_item = code_item.split(',')
                elif not code_item:
                    code_item = [0]
                else:
                    code_item = [code_item]
                clear_author_code_list.append(code_item)

            address_cn = article_cn.find("ul", class_="addresswrap") if article_cn else None
            address_en = article_en.find("ul", class_="addresswrap") if article_en else None


            address_cn_list = safe_find_text_list(address_cn, 'li', 'p')
            address_en_list = safe_find_text_list(address_en, 'li', 'p')

            address_code_list = safe_find_text_list(address_cn, 'li', 'span')
            address_code_list = list(filter(None, address_code_list))
            if not address_code_list:
                address_code_list = safe_find_text_list(address_en, 'li', 'span', parent_class_name="article-author-address")
            clean_address_code_list = list(map(lambda x: x.replace('.', ''), address_code_list)) if address_code_list else address_code_list


            # 将引用信息独立出去，方便后续补爬
            citation_list = [{
                "article_id": article_id,
                "doi": doi,
                "journal_title": journal_title,
                "issn": issn,
                "citation_cn": citation_cn_text,
                "citation_en": citation_en_text,
            }]
            #author_name_cn_list, author_name_en_list, clear_author_code_list, author_mail_cn_list, about_author_cn_list, about_author_en_list
            author_list = []
            if clean_address_code_list:
                for cn_name, en_name, name_code, mail, cn_about_author, en_about_author in zip_longest(author_name_cn_list, author_name_en_list, clear_author_code_list, author_mail_cn_list, about_author_cn_list, about_author_en_list):
                    for cn_address, en_address, address_code in zip_longest(address_cn_list, address_en_list, clean_address_code_list):
                        if len(list(zip_longest(address_cn_list, address_en_list, clean_address_code_list))) == 1:
                            author_list.append({
                                "article_id": article_id,
                                "doi": doi,
                                "journal_title": journal_title,
                                "issn": issn,
                                "cn_name": cn_name,
                                "en_name": en_name,
                                "mail": mail,
                                "cn_address": cn_address,
                                "en_address": en_address,
                                "cn_about_author": cn_about_author,
                                "en_about_author": en_about_author,

                            })
                        elif address_code in name_code :
                            author_list.append({
                                "article_id": article_id,
                                "doi": doi,
                                "journal_title": journal_title,
                                "issn": issn,
                                "cn_name": cn_name,
                                "en_name": en_name,
                                "mail": mail,
                                "cn_address": cn_address,
                                "en_address": en_address,
                                "cn_about_author": cn_about_author,
                                "en_about_author": en_about_author,

                            })
                        elif len(list(zip_longest(address_cn_list, address_en_list, clean_address_code_list))) == 1:
                            author_list.append({
                                "article_id": article_id,
                                "doi": doi,
                                "journal_title": journal_title,
                                "issn": issn,
                                "cn_name": cn_name,
                                "en_name": en_name,
                                "mail": mail,
                                "cn_address": cn_address,
                                "en_address": en_address,
                                "cn_about_author": cn_about_author,
                                "en_about_author": en_about_author,

                            })
            else:
                for cn_name, en_name, name_code, mail, cn_about_author, en_about_author in zip_longest(
                    author_name_cn_list, author_name_en_list, clear_author_code_list, author_mail_cn_list,
                    about_author_cn_list, about_author_en_list):
                    author_list.append({
                        "article_id": article_id,
                        "doi": doi,
                        "journal_title": journal_title,
                        "issn": issn,
                        "cn_name": cn_name,
                        "en_name": en_name,
                        "mail": mail,
                        "cn_address": '',
                        "en_address": '',
                        "cn_about_author": cn_about_author,
                        "en_about_author": en_about_author,

                    })
        self.self_pipeline(reference_list, "reference_info")
        self.self_pipeline(citation_list, "citation_info")
        self.self_pipeline(author_list, "author_info")

        return result



    def _parse_json(self, json_content, article_id, doi, journal_title, issn ,authors_str, source, url, journal_abbrev, pdf_link):
        def get_email(about_info):
            email_pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
            search_result = re.search(email_pattern, about_info)
            if search_result:
                return search_result.group(0)
            else:
                return ''

        affiliations = json_content.get("affiliations")
        authors = json_content.get("authors")

        author_info_list = []
        for author_item in authors:
            author_tag_val = author_item.get("authorTagVal")

            if len(affiliations) == 1:
                author_tag_val_list = ['1']
            elif not author_tag_val:
                author_tag_val_list = ['0']
            elif ',' in author_tag_val:
                author_tag_val_list =  author_tag_val.split(',')

            else:
                author_tag_val_list = [author_tag_val]

            cn_name = author_item.get("authorNameCn")
            en_name = author_item.get("authorNameEn")
            bio_cn = author_item.get("bioCn")
            correspinfo_cn = author_item.get("correspinfoCn")
            bio_en = author_item.get("bioEn")
            correspinfo_en = author_item.get("correspinfoEn")
            mail = author_item.get("email")
            if not mail:
                if bio_cn:
                    mail = get_email(bio_cn)
                elif correspinfo_cn:
                    mail = get_email(correspinfo_cn)

            if affiliations:
                for affiliation_item in affiliations:
                    cn_address = affiliation_item.get("addressCn")
                    en_address = affiliation_item.get("addressEn")
                    sort_number =  str(affiliation_item.get("sortNumber")) if affiliation_item.get("sortNumber") else None
                    if sort_number in author_tag_val_list :
                        author_info_list.append(
                            {
                                "article_id": article_id,
                                "doi": doi,
                                "journal_title": journal_title,
                                "issn": issn,
                                "cn_name": cn_name,
                                "en_name": en_name,
                                "mail": mail,
                                "cn_address": cn_address,
                                "en_address": en_address,
                                "cn_about_author": bio_cn if bio_cn else correspinfo_cn if correspinfo_cn else '' ,
                                "en_about_author":  bio_en if bio_en else correspinfo_en if correspinfo_en else '' ,

                            }
                        )
                    if not sort_number and len(affiliations) == 1:
                        author_info_list.append(
                            {
                                "article_id": article_id,
                                "doi": doi,
                                "journal_title": journal_title,
                                "issn": issn,
                                "cn_name": cn_name,
                                "en_name": en_name,
                                "mail": mail,
                                "cn_address": cn_address,
                                "en_address": en_address,
                                "cn_about_author": bio_cn if bio_cn else correspinfo_cn if correspinfo_cn else '',
                                "en_about_author": bio_en if bio_en else correspinfo_en if correspinfo_en else '',

                            }
                        )
            else:
                author_info_list.append(
                    {
                        "article_id": article_id,
                        "doi": doi,
                        "journal_title": journal_title,
                        "issn": issn,
                        "cn_name": cn_name,
                        "en_name": en_name,
                        "mail": mail,
                        "cn_address": '',
                        "en_address": '',
                        "cn_about_author": bio_cn if bio_cn else correspinfo_cn if correspinfo_cn else '',
                        "en_about_author": bio_en if bio_en else correspinfo_en if correspinfo_en else '',

                    }
                )
            if not author_info_list:
                author_info_list.append(
                    {
                        "article_id": article_id,
                        "doi": doi,
                        "journal_title": journal_title,
                        "issn": issn,
                        "cn_name": cn_name,
                        "en_name": en_name,
                        "mail": mail,
                        "cn_address": '',
                        "en_address": '',
                        "cn_about_author": bio_cn if bio_cn else correspinfo_cn if correspinfo_cn else '',
                        "en_about_author": bio_en if bio_en else correspinfo_en if correspinfo_en else '',

                    }
                )


        citation_cn_text = json_content.get("citationCn")
        citation_en_text = json_content.get("citationEn")
        citation_list = [{
            "article_id": article_id,
            "doi": doi,
            "journal_title": journal_title,
            "issn": issn,
            "citation_cn": citation_cn_text,
            "citation_en": citation_en_text,
        }]

        keywords = json_content.get("keywords")
        if keywords:
            cn_keywords_list = []
            en_keywords_list = []
            for item in keywords:
                if item.get('keywordEn'):
                    en_keywords_list.append(item['keywordEn'])
                if item.get('keywordCn'):
                    cn_keywords_list.append(item['keywordCn'])

            cn_keywords = ','.join(cn_keywords_list) if cn_keywords_list else ''
            en_keywords = ','.join(en_keywords_list) if en_keywords_list else ''
        else:
            cn_keywords = ''
            en_keywords =  ''

        fund_prjs = json_content.get("fundPrjs")
        fund_prj_info_cn = json_content.get("fundPrjInfoCN")
        fund_prj_info_en = json_content.get("fundPrjInfoEN")
        cn_funds = ''
        en_funds = ''
        if fund_prjs:
            cn_fund_list = []
            en_fund_list = []
            for item in fund_prjs:
                cn_fund = item.get('fundsInfoCn')
                en_fund = item.get('fundsInfoCn')
                if not cn_fund:
                    cn_fund = item.get("sourceCn")
                if not en_fund:
                    en_fund = item.get("sourceEn")
                if cn_fund:
                    cn_fund_list.append(cn_fund)
                if en_fund:
                    en_fund_list.append(en_fund)
            if cn_fund_list:
                cn_funds = '；'.join(cn_fund_list)
            if en_fund_list:
                en_funds = ','.join(en_fund_list)
        elif fund_prj_info_cn or fund_prj_info_en:
            cn_funds = fund_prj_info_cn
            en_funds = fund_prj_info_en

        en_title = json_content.get("titleEn")
        pub_date = json_content.get("pubDate")
        rev_recd_date = json_content.get("revRecdDate")
        received_date = json_content.get("receivedDate")
        preprint_date = json_content.get("preprintDate")
        page = json_content.get("page")
        article_id = json_content.get("id")
        lang = json_content.get("language")
        year = json_content.get("year")
        volume = json_content.get("volume")
        openAccess = json_content.get("openAccess")
        issue=  json_content.get("issue")
        publish_date = timestamp_to_date(pub_date) if pub_date else None
        received_date = timestamp_to_date(received_date) if received_date else None
        revised_date = timestamp_to_date(rev_recd_date) if rev_recd_date else None
        online_date = timestamp_to_date(preprint_date) if preprint_date else None
        item_dict = {


            "article_id": article_id,
            "title": json_content.get("titleCn"),
            "en_title": en_title,
            "doi": doi,
            "authors": authors_str,
            "source": source,
            "abstract": re.sub(r'^<p>|</p>$', '', json_content.get("abstractinfoCn", '')),
            "abstract_en": re.sub(r'^<p>|</p>$', '', json_content.get("abstractinfoEn", '')),
            "keywords": cn_keywords,
            "en_keywords": en_keywords,
            "lang": lang,
            "status": "",
            "article_type": "J",
            "publish_date": publish_date,
            "year": year,
            "volume": volume,
            "issue": issue,
            "page_range": page,
            "url": url,
            "issn": issn,
            "publisher": source,
            "journal_title": journal_title,
            "journal_abbrev": journal_abbrev,
            "cn_funds": cn_funds,
            "en_funds": en_funds,
            "pdf_link": pdf_link,
            "received_date": received_date,
            "revised_date": revised_date,
            "online_date": online_date,
            "openAccess": openAccess,
        }

        refers = json_content.get("refers")
        reference_list = []
        if refers:
            for item in refers:
                reference_no = item.get('sortnum')
                reference = item.get('allinfo')
                reference_list.append({
                    "article_id": article_id,
                    "doi": doi,
                    "journal_title": journal_title,
                    "issn": issn,
                    "reference_no": reference_no,
                    "reference": reference,

                })

        return item_dict, author_info_list, citation_list, reference_list

    def self_pipeline(self, items: List[Dict], table_name, row_keys=None):
        # 写自己的存储逻辑
        to_mongo(
                items=items,
                path=table_name,
                conn=self.mongo,
                row_keys=row_keys,
        )

        # 确认种子爬取完毕后删除, 不删除的话后面又会爬取
        # context.success()

if __name__ == '__main__':
    spider = ArticleInfo(
        concurrency=100,
        **{"init.queue.size": 1000000},
        download=requests_.Downloader,
        task_queue=RedisQueue(
            host=RedisConfig.host,
            port=RedisConfig.port,
            password=base64.b64decode(RedisConfig.password).decode("utf-8"),
            database=RedisConfig.database,
        ),
    )
    # spider.run(task_name='init')
    print(ArticleInfo.__module__)
    # spider.survey({'article_id': '20030418', 'article_url': 'http://www.chinjmap.com/cn/article/id/20030418', 'domain': 'http://www.chinjmap.com', '$config': 0})
    # 'https://zlxb.zafu.edu.cn/article/id/1456'
    # 'https://plantscience.cn/cn/article/doi/10.3724/SP.J.1142.2010.30365'