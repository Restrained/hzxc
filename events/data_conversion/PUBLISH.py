#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/25 17:42
# @Author  : AllenWan
# @File    : PUBLISH.py
import pandas as pd

def get_publish_info(input_file, output_file):
    df = pd.read_csv(input_file)
    selected_list = ["id", "authorId"]
    df_selected = df[selected_list].copy()
    # ["id", "authorId", "order_of_authors", "corresponding_author"]
    df_selected["order_of_authors"] = df.groupby('id').cumcount() + 1
    df_selected["corresponding_author"] = '否'

    # 选择需要的列
    columns_to_concat = ["id", "authorId", "order_of_authors", "corresponding_author"]

    # 拼接列并用 `^` 分隔
    df_selected['concatenated'] = df_selected[columns_to_concat].apply(lambda row: '§'.join(row.fillna("").astype(str)), axis=1)

    result = df_selected[['concatenated']]

    result.to_csv(output_file, header=False, index=False, encoding='utf-8-sig')

    return result

if __name__ == '__main__':
    get_publish_info(
        input_file=r"D:\output\csv\Achievement.csv",
        output_file=r"D:\output\csv\PUBLISH.csv"
    )