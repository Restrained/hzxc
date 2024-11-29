#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/25 16:23
# @Author  : AllenWan
# @File    : get_achievement_info.py
import pandas as pd

from events.data_conversion.clean_data import clean_text


def get_achievement_info(input_file1, input_file2, output_file):
    """
    处理表1，将指定列与表2进行关联后生成最终数据，并修正空值和引号问题。

    :param input_file1: str, 表1的文件路径。
    :param input_file2: str, 表2的文件路径 (包含作者信息)。
    :param output_file: str, 处理后的文件路径。
    """
    # 读取表1
    table1 = pd.read_csv(input_file1)
    table2 = pd.read_csv(input_file2)

    selected_columns = ['article_id', 'journal_title', 'citation_cn', 'citation_en']

    table2 = table2[selected_columns]
    # 选择表1需要的列并重命名，保留 article_id, journal_title, issn 以便后续关联
    columns_mapping = {
        "_id": "id",
        "abstract": "abstracts",
        "abstract_en": "en_abstracts",
        "article_type": "type"
    }
    selected_columns = [
        "article_id", "journal_title", "issn",  # 用于关联
        "_id", "title", "en_title", "doi", "source",
        "abstract", "abstract_en", "keywords", "en_keywords",
        "lang", "status", "publish_date", "article_type"
    ]
    table1 = table1[selected_columns].rename(columns=columns_mapping)

    table1['publish_date'] = table1['publish_date'].replace('时间戳输入错误，请检查后重试！', '1900-01-01')

    # 先填充 NaN 值（如果有的话），可以填充为空字符串
    table1["abstracts"] = table1["abstracts"].fillna('')
    table1["en_abstracts"] = table1["en_abstracts"].fillna('')

    table1["status"] = "0"

    table1["abstracts"] = table1["abstracts"].apply(clean_text)
    table1["en_abstracts"] = table1["en_abstracts"].apply(clean_text)

    table1["title"] = table1["title"].apply(clean_text)
    table1["en_title"] = table1["en_title"].apply(clean_text)

    table1["keywords"] = table1["keywords"].apply(clean_text).apply(lambda x: str([item.strip() for item in x.split(',')]) if x else '' )
    table1["en_keywords"] = table1["en_keywords"].apply(clean_text).apply(lambda x: str([item.strip() for item in x.split(',')]) if x else '')


    df_merge = pd.merge(table1, table2, left_on=["article_id", "journal_title"],right_on=["article_id", "journal_title"], how="left")

    df_merge.to_csv(output_file, index=False, encoding="utf-8-sig")


if __name__ == '__main__':

    get_achievement_info(
        input_file1=r"C:\Users\PY-01\Documents\local\renHeHuiZhi\article_info.csv",
        input_file2=r"D:\output\csv\citation_info.csv",
        output_file=r"D:\output\csv\article_info.csv"
    )