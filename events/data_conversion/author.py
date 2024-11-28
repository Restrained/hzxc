#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/25 11:44
# @Author  : AllenWan
# @File    : author.py
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
    df_renamed['type'] = 0
    df_renamed['orc_id'] = ''

    column_rows = "§".join(df_renamed.columns)
    data_rows = df_renamed.fillna("").astype(str).apply("§".join, axis=1)

    result = pd.DataFrame(data_rows, columns=[column_rows])

    result.to_csv(output_file, index=False)


if __name__ == '__main__':
    input_file = r"D:\output\csv\author_info.csv"
    output_file = r'D:\output\csv\Author.csv'
    merge_author_columns(input_file, output_file)