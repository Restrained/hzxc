#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/25 11:44
# @Author  : AllenWan
# @File    : AUTHOR.py
import csv

import pandas as pd


def merge_author_columns(input_file, output_file):
    df = pd.read_csv(input_file)
    columns_mapping = {
        "authorId": "id",
        "cn_name": "name",
        "mail": "email",
        "cn_about_author": "profile",

    }
    df_renamed = df.rename(columns=columns_mapping)
    df_renamed = df_renamed.drop(columns="en_about_author")
    df_renamed['type'] = 0
    df_renamed['orc_id'] = ''

    # 去除每列的前后双引号
    for col in df_renamed.columns:
        df_renamed[col] = df_renamed[col].fillna("").astype(str).str.strip('"')


    column_rows = "⌘".join(df_renamed.columns)
    data_rows = df_renamed.fillna("").astype(str).apply("⌘".join, axis=1)

    result = pd.DataFrame([column_rows] + data_rows.tolist(), columns=["data"])

    # 直接将处理好的数据写入文本文件
    with open(output_file, 'w', encoding='utf-8') as f:
        for row in result.itertuples(index=False, name=None):
            # 将每一行的列数据按需要格式化并写入
            # 假设数据是 JSON 格式化的字符串，先去掉可能存在的双引号转义
            row_data = '⌘'.join(map(str, row))  # 将每一行的数据用 "⌘" 分隔
            f.write(row_data + '\n')  # 逐行写入

    # result.to_csv(output_file, index=False, quoting=csv.QUOTE_NONE, escapechar='\\')


if __name__ == '__main__':
    input_file = r"D:\output\csv\author_info_clean.csv"
    output_file = r'D:\output\csv\Author.csv'
    merge_author_columns(input_file, output_file)