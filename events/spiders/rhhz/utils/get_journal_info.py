#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/26 16:32
# @Author  : AllenWan
# @File    : get_journal_info.py
from urllib.parse import urlparse

import pandas as pd


def get_journal_info(input_file, output_file, category:str):

    df = pd.read_csv(input_file)
    selected_columns = ["issn", "journal_title", "journal_abbrev", "url"]
    df_selected = df[selected_columns].copy()
    df_unique = df_selected.drop_duplicates(keep='first',subset='journal_title')
    # 提取域名
    df_unique['domain'] = df_unique['url'].apply(lambda x: f"{urlparse(x).scheme}://{urlparse(x).netloc}")
    df_unique["category"] = category
    df_unique = df_unique.drop(columns=['url'])
    df_unique.to_csv(output_file, index=False)

def add_category_info(input_file, output_file):
    df = pd.read_csv(input_file)
    df["category"] = '仁和汇智'
    df.to_csv(output_file, index=False)


if __name__ == '__main__':
    get_journal_info(r"D:\output\csv\article_info.csv", r"D:\output\csv\journal_info.csv", "仁和汇智")
    # add_category_info(
    #     input_file=r"D:\output\csv_data\journal_info.csv_data",
    #     output_file=r"D:\output\csv_data\journal_info_v2.csv_data"
    # )