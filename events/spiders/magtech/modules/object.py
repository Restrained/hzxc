#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/7 11:52
# @Author  : AllenWan
# @File    : object.py
# @Desc    ：
import hashlib
import json
import re
import time
from abc import ABC
from itertools import zip_longest
from typing import Dict, List, TypeVar, Union, Optional, Any

from bricks import Response
from bricks.lib.queues import Item
from bs4 import BeautifulSoup, Tag

from events.data_conversion.clean_data import clean_text
from utils.parse import contains_no_chinese
from utils.tool_box import generate_id


class Entity(ABC):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

class Relationship(ABC):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

# 定义泛型类型变量 T，表示 Table 或其子类
T = TypeVar('T', bound=Entity)

class Achievement(Entity):
    """
    此表对应数据库中Achievement表
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._id = None
        self._title = None
        self._en_title = None
        self._doi = None
        self._authors = None
        self._source = None
        self._abstracts = None
        self._en_abstracts = None
        self._keywords = None
        self._en_keywords = None
        self._lang = None
        self._status = None
        self._type = None
        self._publish_date = None
        self._year = None
        self._volume = None
        self._period = None
        self._page_range = None
        self._received_date = None
        self._revised_date = None
        self._accept_date = None
        self._online_date = None
        self._url = None
        self._issn = None
        self._publishers = None
        self._origin_journal_title = None
        self._pdf_link = None

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value

    @property
    def title(self):
        return self._title

    @title.setter
    def title(self, title: str):
        self._title = title

    @property
    def en_title(self):
        return self._en_title

    @en_title.setter
    def en_title(self, en_title: str):
        self._en_title = en_title

    @property
    def doi(self):
        return self._doi

    @doi.setter
    def doi(self, doi: str):
        self._doi = doi

    @property
    def authors(self):
        return self._authors

    @authors.setter
    def authors(self, authors: Dict[str, str]):
        """
        格式： [{"id": "", "name": "", "org": "", "org_id": ""}, {"id": "", "name": "", "org": "", "org_id": ""}]
        :param authors:
        :return:
        """
        self._authors = authors

    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, source: str):
        self._source = source

    @property
    def abstracts(self):
        return self._abstracts

    @abstracts.setter
    def abstracts(self, abstracts: Dict[str, str]):
        self._abstracts = abstracts

    @property
    def en_abstracts(self):
        return self._en_abstracts

    @en_abstracts.setter
    def en_abstracts(self, en_abstracts: Dict[str, str]):
        self._en_abstracts = en_abstracts

    @property
    def keywords(self):
        return self._keywords

    @keywords.setter
    def keywords(self, keywords: List[str]):
        self._keywords = keywords

    @property
    def en_keywords(self):
        return self._en_keywords

    @en_keywords.setter
    def en_keywords(self, en_keywords: List[str]):
        self._en_keywords = en_keywords

    @property
    def lang(self):
        return self._lang

    @lang.setter
    def lang(self, lang: str):
        self._lang = lang

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, status: str):
        self._status = status

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, type: str):
        self._type = type

    @property
    def publish_date(self):
        return self._publish_date

    @publish_date.setter
    def publish_date(self, publish_date: str):
        self._publish_date = publish_date

    @property
    def year(self):
        return self._year

    @year.setter
    def year(self, year: str):
        self._year = year

    @property
    def volume(self):
        return self._volume

    @volume.setter
    def volume(self, volume: str):
        self._volume = volume

    @property
    def period(self):
        return self._period

    @period.setter
    def period(self, period: str):
        self._period = period

    @property
    def page_range(self):
        return self._page_range

    @page_range.setter
    def page_range(self, page_range: str):
        self._page_range = page_range

    @property
    def received_date(self):
        return self._received_date

    @received_date.setter
    def received_date(self, value):
        self._received_date = value

    @property
    def revised_date(self):
        return self._revised_date

    @revised_date.setter
    def revised_date(self, revised_date: str):
        self._revised_date = revised_date

    @property
    def accept_date(self):
        return self._accept_date

    @accept_date.setter
    def accept_date(self, accept_date: str):
        self._accept_date = accept_date

    @property
    def online_date(self):
        return self._online_date

    @online_date.setter
    def online_date(self, online_date: str):
        self._online_date = online_date

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, url: str):
        self._url = url

    @property
    def issn(self):
        return self._issn

    @issn.setter
    def issn(self, issn: str):
        self._issn = issn

    @property
    def publishers(self):
        return self._publishers

    @publishers.setter
    def publishers(self, publishers: List[str]):
        self._publishers = publishers

class Institution(Entity):
    """
        此表对应数据库中Institution表
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._id = None
        # self._achievement_id = None
        # self._author_id = None
        self.institution_id = None
        self._name = None
        self._en_name = None
        self._location = None

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value


    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def en_name(self):
        return self._en_name

    @en_name.setter
    def en_name(self, value):
        self._en_name = value

    @property
    def location(self):
        return self._location

    @location.setter
    def location(self, value):
        self._location = value

class Venue:
    def __init__(self):
        self._id = None
        self._name = None
        self._en_name = None
        self._type = None
        self._achievement_id = None

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def en_name(self):
        return self._en_name

    @en_name.setter
    def en_name(self, value):
        self._en_name = value

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, value):
        self._type = value

    @property
    def achievement_id(self):
        return self._achievement_id

    @achievement_id.setter
    def achievement_id(self, value):
        self._achievement_id = value

class Author(Entity):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._id = None
        self._name = None
        self._en_name = None
        self._type = None
        self._email = None
        self._profile = None
        self._orc_id = None
        self._institution_id = None
        self._achievement_id = None

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def en_name(self):
        return self._en_name

    @en_name.setter
    def en_name(self, value):
        self._en_name = value

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, value):
        self._type = value

    @property
    def email(self):
        return self._email

    @email.setter
    def email(self, value):
        self._email = value

    @property
    def profile(self):
        return self._profile

    @profile.setter
    def profile(self, value):
        self._profile = value

    @property
    def orc_id(self):
        return self._orc_id

    @orc_id.setter
    def orc_id(self, value):
        self._orc_id = value

    @property
    def institution_id(self):
        return self._institution_id

    @institution_id.setter
    def institution_id(self, value):
        self._institution_id = value

    @property
    def achievement_id(self):
        return self._achievement_id

    @achievement_id.setter
    def achievement_id(self, value):
        self._achievement_id = value

class Fund:
    def __init__(self):
        self._id = None
        self._name = None
        self._en_name = None
        self._achievement_id = None

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def en_name(self):
        return self._en_name

    @en_name.setter
    def en_name(self, value):
        self._en_name = value

    @property
    def achievement_id(self):
        return self._achievement_id

    @achievement_id.setter
    def achievement_id(self, value):
        self._achievement_id = value

class WorkAt(Relationship):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._author_id = None
        self._institution_id = None

    @property
    def author_id(self):
        return self._author_id

    @author_id.setter
    def author_id(self, value):
        self._author_id = value

    @property
    def institution_id(self):
        return self._institution_id

    @institution_id.setter
    def institution_id(self, value):
        self._institution_id = value


class Publish(Relationship):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._achievement_id = None
        self._author_id = None
        self._order_of_authors = None
        self._corresponding_author = None

    @property
    def achievement_id(self):
        return self._achievement_id

    @achievement_id.setter
    def achievement_id(self, value):
        self._achievement_id = value

    @property
    def author_id(self):
        return self._author_id

    @author_id.setter
    def author_id(self, value):
        self._author_id = value

    @property
    def order_of_authors(self):
        return self._order_of_authors

    @order_of_authors.setter
    def order_of_authors(self, value):
        self._order_of_authors = value
        self._corresponding_author = value

class ClaimedBy(Relationship):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._achievement_id = None
        self._institution_id = None

    @property
    def achievement_id(self):
        return self._achievement_id

    @achievement_id.setter
    def achievement_id(self, value):
        self._achievement_id = value

    @property
    def institution_id(self):
        return self._institution_id

    @institution_id.setter
    def institution_id(self, value):
        self._institution_id = value



class JournalParseRole:
    def __init__(self, achievement: T,  seeds: Union[Item, List[Item]], response: Optional[Response]):
        self.achievement = achievement
        self.seeds = seeds
        self.response = response

        self.json_data = None
        self.meta_data = None

    @staticmethod
    def common_meta_find(tag:Tag, **kwargs) -> str:
        find_result = tag.find('meta', attrs=kwargs)
        if find_result:
            return find_result.get('content')
        # raise ValueError("tag meta extract have an error, please check!")

    @staticmethod
    def common_meta_find_all(tag:Tag, **kwargs) -> List:
        find_result = tag.find_all('meta', attrs=kwargs)
        if find_result:
            result = [item.get('content') for item in find_result]
            return result
        # raise ValueError("tag meta extract have an error, please check!")



    def get_article_title(self, soup: BeautifulSoup) -> str:
        title = None
        if self.json_data:
            article =  self.json_data.get('article')
            title = article.get('title_cn')
        if not title:
            features = {"name": ["apple-mobile-web-app-title", "DC.Title", "citation_title"]}
            title = self.common_meta_find(soup, **features)
        if title:
            return title
        raise ValueError("article title extract have an error, please check!")

    def get_lang(self, soup: BeautifulSoup) -> str:
        lang = None

        if not lang:
            features = {"name": ["citation_language", "DC.Language"]}
            lang = self.common_meta_find(soup, **features)
        if lang:
            return lang
        raise ValueError("article title extract have an error, please check!")

    def get_publish_date(self, soup: BeautifulSoup) -> str:
        publish_date = None
        if self.json_data:
            article = self.json_data.get('article')
            publish_date = article.get('shouCiFaBuRiQi')

        if not publish_date:
            tag_div = soup.find('div', id="divPanel")
            pattern = re.compile("出版日期:({{\d}4--21)")
            if tag_div:
                tag_span = tag_div.find_all("span")
                for item in tag_span:
                    if item.text.__contains__("出版日期"):
                        match = re.search(pattern, re.sub('\s', '', item.text))
                        if match:
                            publish_date = match.group(1)
            else:
                tag_table = soup.find('table', class_="new_full_Article_history")
                if tag_table:
                    thead = tag_table.find('thead')
                    date_key_list = [item.text for item in thead.find_all("th")]
                    tbody = tag_table.find('tbody')
                    date_value_list = [item.text for item in tbody.find_all("td")]
                    for index, key in enumerate(date_key_list):
                        if "出版日期" in key:
                            publish_date = date_value_list[index]
        if not publish_date:
            features = {"name": ["citation_publication_date"]}
            publish_date = self.common_meta_find(soup, **features)
        if publish_date:
            return publish_date
        # raise ValueError("publish date extract have an error, please check!")


    def get_article_en_title(self, soup: BeautifulSoup) -> str:
        en_title = None
        if self.json_data:
            article = self.json_data.get('article')
            en_title = article.get('title_en')
        if not en_title:
            features = {"name": "citation_title", "xml:lang": "en"}
            en_title = self.common_meta_find(soup, **features)
        if not en_title:
            title_list = soup.find_all("h3", class_="abs-tit")
            en_title = title_list[-1].text if len(title_list) > 1 else None
        if en_title:
            return en_title
        # raise ValueError("article en title extract have an error, please check!")

    def get_doi(self, soup: BeautifulSoup) -> str:
        doi = None
        if self.json_data:
            article = self.json_data.get('article')
            doi = article.get('doi')

        if not doi:
            features = {"name": ["citation_doi", "DC.Identifier"]}
            doi = self.common_meta_find(soup, **features)
        if doi:
            return doi
        # raise ValueError("doi extract have an error, please check!")

    def get_source(self, soup: BeautifulSoup) -> str:
        features = {"name": "citation_journal_title"}
        source = self.common_meta_find(soup, **features)
        if source:
            return source
        raise ValueError("doi extract have an error, please check!")

    def get_abstracts(self, soup: BeautifulSoup) -> str:
        abstracts = None
        if self.json_data:
            article = self.json_data.get('article')
            if article:
                key_points_cn = article.get('keyPoints_cn')
                zhaiyao_cn = article.get('zhaiyao_cn')
                if key_points_cn is not None:
                    abstracts = clean_text(key_points_cn)
                elif zhaiyao_cn is not None:
                    abstracts = clean_text(zhaiyao_cn)


        if abstracts is None:
            features = {"name": ["dc.description", "citation_abstract", "description", "og:description"]}
            abstracts = self.common_meta_find(soup, **features)
            if not abstracts:
                div_tag = soup.find('div', attrs={"name": "#abstract"})
                # 查找所有 p 标签
                if div_tag:
                    p_tags = div_tag.find_all('p')

                    # 使用正则表达式匹配部分文本
                    for p_tag in p_tags:
                        if re.search('摘要：', p_tag.text):
                            abstracts = p_tag.text.strip().replace('摘要：', '')
                else:
                    span_tag = soup.find("span", class_="J_zhaiyao")
                    if span_tag:
                        abstracts = clean_text(span_tag.text)
        if abstracts is not None:
            return abstracts
        # raise ValueError("abstracts extract have an error, please check!")


    def get_en_abstracts(self, soup: BeautifulSoup) -> str:
        en_abstracts = None
        if self.json_data:
            article = self.json_data.get('article')
            key_points_en = article.get('keyPoints_en')
            zhaiyao_en = article.get('zhaiyao_en')
            if key_points_en is not None:
                en_abstracts = clean_text(key_points_en)
            elif zhaiyao_en is not None:
                en_abstracts = clean_text(zhaiyao_en)

        if en_abstracts is None:

            div_tag = soup.find('div', attrs={"name": "#abstract"})
            if div_tag:
                p_tags = div_tag.find_all('p')
                # 使用正则表达式匹配部分文本
                for p_tag in p_tags:
                    if re.search('Abstract:', p_tag.text):
                        en_abstracts = p_tag.text.strip().replace('Abstract:', '')
            else:
                span_tag = soup.find("span", class_="J_zhaiyao_en")
                if span_tag:
                    en_abstracts = clean_text(span_tag.text)
        if en_abstracts is not None:
            return en_abstracts
        # raise ValueError("en abstracts extract have an error, please check!")


    def get_keywords(self, soup: BeautifulSoup) -> List[str]:
        keywords = None
        if self.json_data:
            article = self.json_data.get('article')
            keywords = article.get('keywordList_cn')
        if not keywords:
            features = {"name": "citation_keywords", "xml:lang": "zh"}
            keywords = self.common_meta_find_all(soup, **features)
            if not keywords:
                div_tag = soup.find('div', attrs={"name": "#abstract"})
                if div_tag:
                    p_tags = div_tag.find_all('p')
                    # 使用正则表达式匹配部分文本
                    for p_tag in p_tags:
                        if re.search('关键词:', p_tag.text):
                            keywords = [tag.text for tag in p_tag.find_all('a')]
                else:
                    keywords = []
        if keywords:
            return keywords
        # raise ValueError("keywords extract have an error, please check!")

    def get_en_keywords(self, soup: BeautifulSoup) -> List[str]:
        en_keywords = None
        if self.json_data:
            article = self.json_data.get('article')
            en_keywords = article.get('keywordList_en')

        if not en_keywords:
            features = {"name": "citation_keywords", "xml:lang": "en"}
            en_keywords = self.common_meta_find_all(soup, **features)
            if not en_keywords:
                div_tag = soup.find('div', attrs={"name": "#abstract"})
                if div_tag:
                    p_tags = div_tag.find_all('p')
                    # 使用正则表达式匹配部分文本
                    for p_tag in p_tags:
                        if re.search('Key words: ', p_tag.text):
                            en_keywords = [tag.text for tag in p_tag.find_all('a')]
                else:
                    en_keywords = []
        if en_keywords:
            return en_keywords
        # raise ValueError("keywords extract have an error, please check!")

    def get_page_range(self, soup: BeautifulSoup) -> str:
        page_range = None
        if self.json_data:
            article = self.json_data.get('article')
            start_page = article.get('qiShiYe')
            end_page = article.get('jieShuYe')
            page_range = f"{start_page}-{end_page}"

        if not page_range:
            start_feature = {"name": ["citation_firstpage", "dc.citation.spage", "prism.startingPage"]}
            end_feature = {"name": ["citation_lastpage", "dc.citation.epage", "prism.endingPage"]}
            start_page = self.common_meta_find(soup, **start_feature)
            end_page = self.common_meta_find(soup, **end_feature)
            if start_page and end_page:
                page_range = f"{start_page}-{end_page}"
            else:
                tag_div = soup.find('div', class_="abs-con")
                if tag_div:
                    text = re.sub("\s", '', tag_div.text)
                    pattern = r"Issue\(\d+\):(\d+-\d+)"
                    match = re.search(pattern, text)
                    if match:
                        page_range = match.group(1)
        if page_range:
            return page_range
        # raise NotImplementedError("page range extract have an error, please check!")

    def get_received_date(self, soup: BeautifulSoup) -> str:
        received_date = None
        if self.json_data:
            article = self.json_data.get('article')
            received_date = article.get('received')

        if not received_date:
            tag_div = soup.find('div', id="divPanel")
            pattern = re.compile("收稿日期:({{\d}4--21)")
            if tag_div:
                tag_span = tag_div.find_all("span")
                for item in tag_span:
                    if item.text.__contains__("收稿日期"):
                        match = re.search(pattern, re.sub('\s', '',item.text))
                        if match:
                            received_date = match.group(1)

            else:
                tag_table = soup.find('table', class_="new_full_Article_history")
                if tag_table:
                    thead = tag_table.find('thead')
                    date_key_list = [item.text for item in thead.find_all("th")]
                    tbody = tag_table.find('tbody')
                    date_value_list = [item.text for item in tbody.find_all("td")]
                    for index,key in enumerate(date_key_list):
                        if "收稿日期" in key :
                            received_date = date_value_list[index]
        if received_date:
            return received_date
        # raise ValueError("received date extract have an error, please check!")

    def get_revised_date(self, soup: BeautifulSoup) -> str:
        revised_date = None
        if self.json_data:
            article = self.json_data.get('article')
            revised_date = article.get('revised')
        if not revised_date:
            tag_div = soup.find('div', id="divPanel")
            pattern = re.compile("修回日期:({{\d}4--21)")
            if tag_div:
                tag_span = tag_div.find_all("span")
                for item in tag_span:
                    if item.text.__contains__("修回日期"):
                        match = re.search(pattern, re.sub('\s', '', item.text))
                        if match:
                            revised_date = match.group(1)

            else:
                tag_table = soup.find('table', class_="new_full_Article_history")
                if tag_table:
                    thead = tag_table.find('thead')
                    date_key_list = [item.text for item in thead.find_all("th")]
                    tbody = tag_table.find('tbody')
                    date_value_list = [item.text for item in tbody.find_all("td")]
                    for index, key in enumerate(date_key_list):
                        if "修回日期" in key:
                            revised_date = date_value_list[index]
        if revised_date:
            return revised_date
        # raise ValueError("revised date extract have an error, please check!")

    def get_accept_date(self, soup: BeautifulSoup) -> str:
        accept_date = None
        if self.json_data:
            article = self.json_data.get('article')
            accept_date = article.get('accept')
        if not accept_date:
            tag_div = soup.find('div', id="divPanel")
            pattern = re.compile("录用日期:({{\d}4--21)")
            if tag_div:
                tag_span = tag_div.find_all("span")
                for item in tag_span:
                    if item.text.__contains__("录用日期"):
                        match = re.search(pattern, re.sub('\s', '', item.text))
                        if match:
                            accept_date = match.group(1)

            else:
                tag_table = soup.find('table', class_="new_full_Article_history")
                if tag_table:
                    thead = tag_table.find('thead')
                    date_key_list = [item.text for item in thead.find_all("th")]
                    tbody = tag_table.find('tbody')
                    date_value_list = [item.text for item in tbody.find_all("td")]
                    for index, key in enumerate(date_key_list):
                        if "录用日期" in key:
                            accept_date = date_value_list[index]
        if accept_date:
            return accept_date
        # raise ValueError("accept date extract have an error, please check!")

    def get_online_date(self, soup: BeautifulSoup) -> str:
        features = {"name": ["dc.date", "prism.publicationDate"]}
        online_date = self.common_meta_find(soup, **features)
        if not online_date:
            tag_div = soup.find('div', id="divPanel")
            pattern = re.compile("出版日期:({{\d}4--21)")
            if tag_div:
                tag_span = tag_div.find_all("span")
                for item in tag_span:
                    if item.text.__contains__("出版日期"):
                        match = re.search(pattern, re.sub('\s', '', item.text))
                        if match:
                            online_date = match.group(1)

            else:
                tag_table = soup.find('table', class_="new_full_Article_history")
                if tag_table:
                    thead = tag_table.find('thead')
                    date_key_list = [item.text for item in thead.find_all("th")]
                    tbody = tag_table.find('tbody')
                    date_value_list = [item.text for item in tbody.find_all("td")]
                    for index, key in enumerate(date_key_list):
                        if "出版日期" in key:
                            online_date = date_value_list[index]
        if online_date:
            return online_date
        # raise ValueError("online date extract have an error, please check!")

    def get_meta_data(self):
        html = self.response.text
        pattern = re.compile("window.metaData\s*=\s*(\{.*?\});")
        match = re.search(pattern, html)
        if match:
            meta_data = match.group(1)
            json_data = json.loads(meta_data)
            self.json_data = json_data

    def get_url(self, soup: BeautifulSoup) -> str:
        url = None
        if self.json_data:
            article = self.json_data.get('article')
            url = article.get('abstractUrl_cn')
        if not url:
            features = {"name": ["prism.url", "citation_fulltext_html_url", "citation_abstract_html_url", "citation_fulltext_html_url"]}
            url = self.common_meta_find(soup, **features)
        if not url:
            url = self.seeds["article_url"]

        return url

    def get_issn(self, soup: BeautifulSoup) -> str:
        issn = None
        if self.json_data:
            journal = self.json_data.get('journal')
            issn = journal.get('issn')
        if not issn:
            features = {"name": ["citation_issn", "prism.issn"]}
            issn = self.common_meta_find(soup, **features)
        if issn:
            return issn
        raise ValueError("issn extract have an error, please check!")

    def get_journal_title(self, soup: BeautifulSoup) -> str:
        journal_title = None
        if self.json_data:
            journal = self.json_data.get('journal')
            journal_title = journal.get('qiKanMingCheng_CN')
        if not journal_title:
            features = {"name": ["citation_journal_title", "dc.source", "og:site_name", "dc.relation.is"]}
            journal_title = self.common_meta_find(soup, **features)
        if journal_title:
            return journal_title
        raise ValueError("title extract have an error, please check!")

    def get_journal_en_title(self, soup: BeautifulSoup) -> str:
        en_title = None
        if self.json_data:
            journal = self.json_data.get('journal')
            en_title = journal.get('qiKanMingCheng_EN')
        if not en_title:
            features = {"name": "citation_title", "xml:lang": "en"}
            en_title = self.common_meta_find(soup, **features)
        if not en_title:
            title_list = soup.find_all('h3', class_="abs-tit")
            if len(title_list) == 2:
                en_title = title_list[1].text.strip()

        if en_title:
            return en_title
        # raise ValueError("en_title extract have an error, please check!")

    def get_pdf_link(self, soup: BeautifulSoup) -> str:
        features = {"name": ["citation_pdf_url"]}
        pdf_link = self.common_meta_find(soup, **features)
        return pdf_link

    def extract_citation(self, reference_html:str) -> list:
        # 匹配每一段引用正则表达式
        pattern = r"\[\d+\].*?(?=(?:\[\d+\])|$)"

        # 使用正则提取每段引用
        references = re.findall(pattern, reference_html, flags=re.DOTALL)

        # 去除每段中的 \xa0
        cleaned_references = [ref.replace('\xa0', ' ').strip() for ref in references]

        return cleaned_references

    def get_reference_list(self, soup: BeautifulSoup) -> list:
        reference_result_list = []
        if self.json_data:
            reference_list = self.json_data.get('referenceList')
            if reference_list:
                for refer_item in reference_list:
                    source_en = refer_item.get('sourceEn')
                    publication_type = refer_item.get('publicationType')
                    label = refer_item.get('label')
                    year = refer_item.get('nian')
                    cited_count = refer_item.get('citedCount')
                    citation_list = refer_item.get('citationList')
                    person_list = citation_list[0].get('personList')
                    content = citation_list[0].get('content')
                    reference_result_list.append({
                        "achievement_id": self.achievement.id,
                        "source_en": source_en,
                        "publication_type": publication_type,
                        "label": label,
                        "year": year,
                        "cited_count": cited_count,
                        "person_list": person_list,
                        "content": clean_text(content),


                    })
            else:
                article = self.json_data.get('article')
                reference_html = article.get('reference')
                clear_reference_html = clean_text(reference_html)
                content_list = self.extract_citation(clear_reference_html)
                if content_list:
                    for index, ref in enumerate(content_list):
                        reference_result_list.append({
                            "achievement_id": self.achievement.id,
                            "source_en": '',
                            "publication_type": '',
                            "label": index+1,
                            "year": '',
                            "cited_count": '',
                            "person_list": '',
                            "content": ref,

                        })
        else:
            div_tag = soup.find('div', id=["collapseThree", "reference_tab_content"])
            if div_tag:
                td_tag = div_tag.find('td', class_=["J_author", "J_zhaiyao"])
                if td_tag:
                    reference_html = td_tag.text
                    clear_reference_html = clean_text(reference_html)
                    content_list = self.extract_citation(clear_reference_html)
                    if content_list:
                        for index, ref in enumerate(content_list):
                            reference_result_list.append({
                                "achievement_id": self.achievement.id,
                                "source_en": '',
                                "publication_type": '',
                                "label": index + 1,
                                "year": '',
                                "cited_count": '',
                                "person_list": '',
                                "content": ref,

                            })



        return reference_result_list

    def get_fund_name(self, soup: BeautifulSoup) -> List:
        fund_list = []
        result = []
        if self.json_data:
            fund_list = self.json_data.get('fundList_cn')
        if not fund_list:
            tag_div = soup.find('div', id="divPanel")
            if tag_div:
                tag_span = tag_div.find_all("span")
                for item in tag_span:
                    if item.text.__contains__("基金资助"):
                        raw_fund_info = item.text.replace("基金资助:", '')
                        fund_info = clean_text(raw_fund_info)
                        if ',' in fund_info:
                            fund_list = fund_info.split(',')
                        else:
                            fund_list = fund_info.split('，')
            else:
                tag_td = soup.find_all("td", class_="J_zhaiyao")
                for item in tag_td:
                    if item.text.__contains__("基金资助"):
                        raw_fund_info = item.text.replace("基金资助:", '')
                        fund_info = clean_text(raw_fund_info)
                        if ',' in fund_info:
                            fund_list = fund_info.split(',')
                        else:
                            fund_list = fund_info.split('，')

        if fund_list:
            for fund in fund_list:
                if not fund.endswith(")") and ')' in fund:
                    fund = fund.split(")")[0] + ')'
                result.append(fund)
        return result

    def get_en_fund_name(self, soup: BeautifulSoup) -> List:
        en_fund_list = []
        result = []
        if self.json_data:
            en_fund_list = self.json_data.get('fundList_en')
        if not en_fund_list:
            tag_div = soup.find('div', id="divPanelEn")
            if tag_div:
                tag_span = tag_div.find_all("span")
                for item in tag_span:
                    if item.text.__contains__("Supported by"):
                        raw_fund_info = item.text.replace("Supported by:", '')
                        en_fund_info = clean_text(raw_fund_info)
                        en_fund_list = en_fund_info.split(',')

        if en_fund_list:
            for item in en_fund_list:
                if item.endswith(".") and ')' in item:
                    item = item[:-1]
                if item.__contains__("and"):
                    result.extend(item.split(' and '))
                else:
                    result.append(item)
        return result

    def get_volume_info(self, soup: BeautifulSoup) -> str:
        if self.json_data:
            article = self.json_data.get('article')
            volume = article.get('juan')
        else:
            features = {"name": "citation_volume" }
            volume = self.common_meta_find(soup, **features)
        if volume:
            return volume

    def get_parse_info(self, soup:BeautifulSoup):
        self.get_meta_data()
        self.achievement.title =  self.get_article_title(soup)
        self.achievement.id = generate_id(self.achievement.title)
        self.achievement.en_title =  self.get_article_en_title(soup)
        self.achievement.doi =  self.get_doi(soup)
        self.achievement.source =  self.get_source(soup)
        self.achievement.abstracts = self.get_abstracts(soup)
        self.achievement.en_abstracts = self.get_en_abstracts(soup)
        self.achievement.keywords = self.get_keywords(soup)
        self.achievement.en_keywords = self.get_en_keywords(soup)
        self.achievement.lang = self.get_lang(soup)
        self.achievement.status = ''
        self.achievement.type = 'J'
        self.achievement.publish_date = self.get_publish_date(soup)
        self.achievement.year = self.seeds['year']
        self.achievement.volume = self.get_volume_info(soup)
        self.achievement.period = self.seeds['issue']
        self.achievement.page_range = self.get_page_range(soup)
        self.achievement.received_date = self.get_received_date(soup)
        self.achievement.revised_date = self.get_revised_date(soup)
        self.achievement.accept_date = self.get_accept_date(soup)
        self.achievement.online_date = self.get_online_date(soup)
        self.achievement.url = self.get_url(soup)
        self.achievement.issn = self.get_issn(soup)
        self.achievement.publishers = self.get_journal_title(soup)
        self.achievement.pdf_link = self.get_pdf_link(soup)
        self.achievement.open_access = 0
        self.achievement.origin_journal_title = self.seeds["journal_title"]

        author_list, institution_list, publish_list, claimed_by_list, work_at_list = self.get_author_institution_info(
            soup)

        achievement_list = [
            {
                "achievement_id": self.achievement.id,
                "title": self.achievement.title,
                "en_title": self.achievement.en_title,
                "doi": self.achievement.doi,
                "authors": self.achievement.authors,
                "source": self.achievement.source,
                "abstracts": self.achievement.abstracts,
                "en_abstracts": self.achievement.en_abstracts,
                "keywords": self.achievement.keywords,
                "lang": self.achievement.lang,
                "status": self.achievement.status,
                "type": self.achievement.type,
                "publish_date": self.achievement.publish_date,
                "year": self.achievement.year,
                "volume": self.achievement.volume,
                "period": self.achievement.period,
                "page_range": self.achievement.page_range,
                "received_date": self.achievement.received_date,
                "revised_date": self.achievement.revised_date,
                "accept_date": self.achievement.accept_date,
                "online_date": self.achievement.online_date,
                "url": self.achievement.url,
                "issn": self.achievement.issn,
                "publishers": self.achievement.publishers,
                "pdf_link": self.achievement.pdf_link,
                "open_access": self.achievement.open_access,
                "origin_journal_title": self.achievement.origin_journal_title,
                "batch_id": self.seeds["batch_id"],
                "time_stamp": int(time.time() * 1000),
            }
        ]



        venue_list, published_list = self.get_venue_published_in_list(soup)

        fund_list, supported_by_list = self.get_fund_supported_by_list(soup)

        reference_list = self.get_reference_list(soup)

        return achievement_list, author_list, institution_list, publish_list, claimed_by_list, work_at_list, venue_list, fund_list, supported_by_list, reference_list

    @staticmethod
    def remove_tag_sup(raw_str: str) -> str:
        # 使用正则表达式移除所有 <sup> 标签
        cleaned_text = re.sub(r"<sup>.*?</sup>", "", raw_str)
        return cleaned_text

    def extract_org_id(self, xref: str) -> List[str]:
        match = re.findall(r'<sup>(\d+)</sup>', xref)
        if match:
            return match
        raise ValueError("extract_org_id have an error, please check!")

    def extract_institution_info(self, xref: Union[str,List], en_xref: Union[str,List]) -> List[dict]:
        def extract_info(raw_str: str) -> tuple[Union[str, Any], Union[str, Any], Union[str, Any]]:
            pattern_list = [re.compile(r"(\d+)\.\s*(.*?)(?:[,， ]([^，, ]+))?$"),
                            re.compile(r"\((.+?)[\s,]+(.+?)\)<sup>(\d+)</sup>$")
                            ]
            raw_str = clean_text(raw_str)
            for pattern in pattern_list:
                match = re.search(pattern, raw_str)
                if match:
                    num = match.group(1)  # 编号
                    ins = match.group(2)  # 机构信息
                    loc = match.group(3)  # 城市和邮编
                    return num, ins, loc

            extra_list = raw_str.split(' ')
            if len(extra_list) >= 3:
                if isinstance(extra_list[0], int):
                    num = extra_list[0]
                    ins = extra_list[1]
                    loc = ' '.join(extra_list[2:])
                else:
                    num = "1"
                    ins = extra_list[0]
                    loc = ' '.join(extra_list[1:])
            else:
                num = '1'
                ins = extra_list[0]
                loc = ''
            return num, ins, loc

        def extract_en_info(raw_str: str) -> tuple[Union[str, Any], Union[str, Any], Union[str, Any]]:
            en_pattern_list = [re.compile(r"(\d+)\.\s*(.*),([^,]+)$"),
                               re.compile(r"\((.+?)[\s,]+(.+?)\)<sup>(\d+)</sup>$")
                               ]
            raw_str = clean_text(raw_str)
            for pattern in en_pattern_list:
                match = re.match(pattern, raw_str)
                if match:
                    num = match.group(1)  # 编号
                    ins = match.group(2)  # 机构信息
                    loc = match.group(3)  # 城市和邮编
                    if loc == "China":
                        ins_list = ins.split(',')
                        ins = ','.join(ins_list[:-1])
                        loc = ins_list[-1] + loc
                    return num, ins, loc
            extra_list = raw_str.split(' ', maxsplit=1)
            if len(extra_list) >= 2:
                num = extra_list[0]
                other_list = extra_list[1].split(',')
                if len(other_list) >= 4:
                    ins = ','.join(other_list[0:2])
                    loc = ','.join(other_list[2:])
                elif len(other_list) >= 2:
                    ins = other_list[0]
                    loc = ','.join(other_list[1:])
                else:
                    ins = other_list
                    loc = ''
            else:
                num = '1'
                ins = extra_list[0]
                loc = ''
            return num, ins, loc


        result = []
        aff_list = []
        en_aff_list = []

        if isinstance(xref, str):
            xref = xref.replace("<p>", "").replace("</p>", "")
            aff_list = xref.split("<br/>")
            en_aff_list = en_xref.split("<br/>")
        elif isinstance(xref, list):
            if len(xref) == 1:
                xref[0] = xref[0].replace("<p>", "").replace("</p>", "")
                if xref[0].__contains__("\r\n"):
                    aff_list = xref[0].split("\r\n")
                    en_aff_list = en_xref[0].split("\r\n") if en_xref  else []
                elif xref[0].__contains__("<br"):
                    aff_list = xref[0].split("<br")
                    en_aff_list = en_xref[0].split("<br") if en_xref  else []
                else:
                    aff_list = xref
                    en_aff_list = en_xref
            else:
                aff_list = xref
                en_aff_list = en_xref
        # else:
        #     raise TypeError("extract_institution_info have an error, please check!")
        if aff_list:
            aff_list = [item for item in aff_list if item.strip()]
            # 正则表达式模式

            if en_aff_list and len(en_aff_list) == len(aff_list):
                for index, item in enumerate(aff_list):
                    if contains_no_chinese(item):
                        continue
                    number, institute, location = extract_info(item)
                    if not institute or not location:
                        continue
                    en_number, en_institute, en_location = extract_en_info(en_aff_list[index]) if en_aff_list else ('', '', '')
                    institution_id = generate_id(institute)
                    result.append({
                        "institution_id": institution_id,
                        "institute": institute,
                        "en_institute": en_institute,
                        "location": location,
                        "org_id": number,
                    })
            else:
                for index, item in enumerate(aff_list):
                    if contains_no_chinese(item):
                        continue
                    number, institute, location = extract_info(item)
                    if not institute:
                        continue
                    institution_id = generate_id(institute)
                    result.append({
                        "institution_id": institution_id,
                        "institute": institute,
                        "en_institute": '',
                        "location": location,
                        "org_id": number,
                    })
        if result:
            return result
        # raise ValueError("extract_institution_info have an error, please check!")

    @staticmethod
    def extract_author_name_from_briefly(briefly: str) -> str:
        pattern = re.compile(r"([\u4e00-\u9fa5]+)（\d{4}")
        # 首先去除掉所有空白字符
        briefly = re.sub("\s+", "", briefly)
        match = re.search(pattern, briefly)
        if match:
            author_name = match.group(1)
            return author_name

    @staticmethod
    def extract_trailing_number(s) ->tuple[str,List[str]]:
        # 匹配以数字结尾的情况
        # 匹配末尾以逗号分隔的数字序列
        s = s.strip()
        if "*" in s:
            s = s.replace("*", "")
        match = re.search(r"(\d+(?:\s*,\s*\d+)*)$", s)
        if match:
            # 提取匹配的数字部分并分割为列表
            numbers = match.group(1).split(",")
            # 去除数字部分
            new_string = s[:match.start()].strip()
            return new_string, numbers
        else:
            return s.strip(), ["1"]


    def extract_author_name_from_str(self, author_list_str: str, en_author_list_str: str) -> List[dict]:
        result  = []
        raw_author_list = author_list_str.split(', ')
        raw_en_author_list = en_author_list_str.split(', ')
        if len(raw_author_list) == 1:
            raw_author_list = author_list_str.split('，')
        if len(raw_en_author_list) == 1:
            raw_en_author_list = en_author_list_str.split('，')
        if len(raw_author_list) == 1:
            raw_author_list = raw_author_list[0].split(',')
        if len(raw_en_author_list) == 1:
            raw_en_author_list = raw_en_author_list[0].split(',')
        if len(raw_author_list) == 1:
            raw_author_list = raw_author_list[0].split(' ')
        if len(raw_en_author_list) == 1:
            raw_en_author_list = raw_en_author_list[0].split(' ')
        if len(raw_en_author_list) == len(raw_author_list):
            for index, item in enumerate(raw_author_list):
                clear_name, org_id = self.extract_trailing_number(item)
                if not clear_name:
                    continue
                clear_en_name, org_id = self.extract_trailing_number(raw_en_author_list[index])

                author_id = generate_id(clear_name)

                result.append({
                    "author_id": author_id,
                    "author_name": clear_name,
                    "author_en_name": clear_en_name,
                    "email": "",
                    "org_id": org_id,
                    "type": 0,
                    "profile": ''
                })
        else:
            for index, item in enumerate(raw_author_list):
                clear_name, org_id = self.extract_trailing_number(item)
                if not clear_name:
                    continue
                author_id = generate_id(clear_name)
                result.append({
                    "author_id": author_id,
                    "author_name": clear_name,
                    "author_en_name": '',
                    "email": "",
                    "org_id": org_id,
                    "type": 0,
                    "profile": ''
                })
        return result

    def get_briefly_list_from_html(self, soup:BeautifulSoup):
        briefly_list = []
        result = []
        tag_div = soup.find('div', id="divPanel")
        if tag_div:
            tag_span = tag_div.find_all("span")
            for item in tag_span:
                if item.text.__contains__("作者简介"):
                    raw_briefly_info = item.text.replace("作者简介:", '')
                    briefly_info = clean_text(raw_briefly_info)
                    briefly_list = briefly_info.split(';')
        else:
            tag_span = soup.find_all("span", "J_zhaiyao")
            for item in tag_span:
                if item.text.__contains__("作者简介"):
                    raw_briefly_info = item.text.replace("作者简介:", '')
                    briefly_info = clean_text(raw_briefly_info)
                    briefly_list = briefly_info.split(';')
        if briefly_list:
            for content in briefly_list:
                clear_content = clean_text(content)
                author_name = self.extract_author_name_from_briefly(clear_content)
                result.append({
                    "author_name": author_name,
                    "briefly": clear_content
                })
        if result:
            return result

    def get_author_institution_info(self, soup:BeautifulSoup):
        publish_list = []
        claimed_by_list = []
        author_list = []
        institution_list = []
        authors = []
        briefly_list = []
        work_at_list = []
        self.get_meta_data()
        if self.json_data:
            bio_list_cn = self.json_data.get('bioList_cn')
            if bio_list_cn:
                for bio in bio_list_cn:
                    content = bio.get('content')
                    clear_content = clean_text(content)
                    author_name = self.extract_author_name_from_briefly(clear_content)
                    briefly_list.append({
                        "author_name": author_name,
                        "briefly": clear_content
                    })

            author_list_cn = self.json_data.get('authorList')
            if author_list_cn:
                for item in author_list_cn:
                    author_name = item.get('name_cn')
                    author_en_name = item.get('name_en')
                    email = item.get('email')
                    xref = item.get('xref')
                    org_id = self.extract_org_id(xref)
                    encrypt_str = f"{author_name} {author_en_name}{email}"
                    author_id = generate_id(encrypt_str)
                    author_list.append({
                        "author_id": author_id,
                        "author_name": author_name,
                        "author_en_name": author_en_name,
                        "email": email,
                        "org_id": org_id,
                        "type": 0,
                        "profile": ''
                    })
            else:
                article = self.json_data.get('article')
                if article:
                    author_list_str = article.get('zuoZhe_CN')
                    en_author_list_str = article.get('zuoZhe_EN')
                    if author_list_str:
                        if not en_author_list_str:
                            author_list =  self.extract_author_name_from_str(clean_text(author_list_str), "")
                        else:
                            author_list =  self.extract_author_name_from_str(clean_text(author_list_str), clean_text(en_author_list_str))

            aff_list_cn = self.json_data.get('affList_cn')
            aff_list_en = self.json_data.get('affList_en')
            institution_list = self.extract_institution_info(aff_list_cn, aff_list_en)

        else:
            div_box = soup.find('div', class_="abs-con")
            if div_box:
                title_element = div_box.find("p", attrs={"csv_data-target": "#divPanel"})
                en_title_element = div_box.find("p", attrs={"csv_data-target": "#divPanelEn"})
                if title_element:
                    author_info_text = title_element.text
                    en_author_info_text = en_title_element.text
                    author_list = self.extract_author_name_from_str(clean_text(author_info_text),
                                                                    clean_text(en_author_info_text))
            else:
                tag_td = soup.find("td", class_="J_author_cn")
                en_tag_td = soup.find("td", class_="J_author_en")
                if tag_td:
                    author_info_text = tag_td.text
                    if en_tag_td:
                        en_author_info_text = en_tag_td.text
                    else:
                        en_author_info_text = ''
                    author_list = self.extract_author_name_from_str(clean_text(author_info_text),
                                                                    clean_text(en_author_info_text))
            institution_div = soup.find("div", id="divPanel")
            if institution_div:
                en_institution_div = soup.find("div", id="divPanelEn")
                tag_address = institution_div.find("address", class_="address")
                tag_en_address = en_institution_div.find("address", class_="address")
                # 使用 decode_contents() 获取包含子标签的 HTML 内容
                institution_content = tag_address.decode_contents()
                en_institution_content = tag_en_address.decode_contents()
                institution_list = self.extract_institution_info(institution_content, en_institution_content)
            else:
                institution_tag = soup.find_all("span", class_="J_author_dizhi")
                if institution_tag:
                    institution_content = institution_tag[0].decode_contents()
                    if len(institution_tag) > 1:
                        en_institution_content = institution_tag[1].decode_contents()
                    else:
                        en_institution_content = ""
                    institution_list = self.extract_institution_info(institution_content, en_institution_content)

            briefly_list = self.get_briefly_list_from_html(soup)


        if author_list:

            for index, author_item in enumerate(author_list):
                publish_list.append({
                    "author_id": author_item["author_id"],
                    "achievement_id": self.achievement.id,
                    "order_of_authors": index+1,
                    "corresponding_author": ''
                })
                if institution_list:
                    # work_at关系的建立
                    for institute_item in institution_list:
                        if institute_item.get('org_id') in author_item.get('org_id'):

                            authors.append(
                                {
                                    "id":  author_item.get('author_id'),
                                    "name":  author_item.get('author_name'),
                                    "org":  institute_item.get('institute'),
                                    "org_id":  institute_item.get('institution_id'),
                                }
                            )
                            work_at_list.append(
                                {
                                    "author_id": author_item.get('author_id'),
                                    "institution_id": institute_item.get('institution_id')
                                }
                            )
                else:

                    authors.append(
                        {
                            "id":  author_item.get('author_id'),
                            "name":  author_item.get('author_name'),
                            "org":  "",
                            "org_id":  "",
                        }
                    )

                if briefly_list:
                    # 补充对应的作者简介
                    for briefly_item in briefly_list:
                        if briefly_item.get('author_name') == author_item.get('author_name'):
                            author_item['profile'] = briefly_item['briefly']

        if institution_list:
            # work_at关系的建立
            for institute_item in institution_list:
                claimed_by_list.append({
                    "achievement_id": self.achievement.id,
                    "institute_id": institute_item.get('institution_id'),
                })
        if authors:
            self.achievement.authors = authors
        return author_list, institution_list, publish_list, claimed_by_list, work_at_list

    def get_venue_published_in_list(self, soup:BeautifulSoup):
        self.get_meta_data()
        journal_name = self.get_journal_title(soup)
        journal_en_name = self.get_journal_en_title(soup)
        year = self.achievement.year
        volume = self.achievement.volume
        period = self.achievement.period
        page_range = self.achievement.page_range
        journal_type = "J"
        journal_id = generate_id(journal_name)
        venue_list = [{
            "journal_id": journal_id,
            "name": journal_name,
            "en_name": journal_en_name,
            "type": journal_type,
        }]

        published_in_list = [
            {
                "achievement_id": self.achievement.id,
                "journal_id": journal_id,
                "year": year,
                "volume": volume,
                "period": period,
                "page_range": page_range,
            }
        ]

        return venue_list, published_in_list

    def get_fund_supported_by_list(self, soup:BeautifulSoup):
        fund_list = []
        supported_by_list = []
        cn_fund_list = self.get_fund_name(soup)
        en_fund_list = self.get_en_fund_name(soup)
        if cn_fund_list:
            for fund_name, en_fund_name in zip_longest(cn_fund_list, en_fund_list):
                fund_id = generate_id(fund_name)
                fund_list.append({
                    "fund_id": fund_id,
                    "name": fund_name,
                    "en_name": en_fund_name,
                })
                supported_by_list.append({
                    "achievement_id": self.achievement.id,
                    "fund_id": fund_id,
                })
        return fund_list, supported_by_list

if __name__ == '__main__':
    text = r"1.&nbsp;Key Laboratory of Ministry of Communications for Bridge Detection ＆ Reinforcement Technology,&nbsp;School of Highway, Chang'an University, Xi'an Shanxi 710064, China"
    print(clean_text(text))
    # pattern = re.compile(r"(\d+)\.\s*(.*?)[,，]([^，,]+)$")
    # print(type(pattern))