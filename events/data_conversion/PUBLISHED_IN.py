#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/26 09:15
# @Author  : AllenWan
# @File    : PUBLISHED_IN.py
import pandas as pd


def get_published_info(input_file1, input_file2, output_file):
    df1 = pd.read_csv(input_file1)
    df2 = pd.read_csv(input_file2)
    selected_list = ["_id", "year", "volume", "issue", "page_range", "journal_title"]
    df1_selected = df1[selected_list]
    df1_selected['volume'] = df1_selected['volume'].apply(lambda x: int(x) if pd.notna(x) else '')
    columns_mapping = {
        "_id": "article_id",
        "issue": "period",
    }
    df2_selected = df2[["id", "name"]]
    df1_selected = df1_selected.rename(columns=columns_mapping)
    merged = df1_selected.merge(df2_selected, left_on="journal_title", right_on="name", how="left")
    final_column_lisst = ["article_id", "id", "year", "volume", "period", "page_range"]
    merged['concatenated'] = merged[final_column_lisst].fillna("").astype(str).apply("§".join, axis=1)

    result = merged[['concatenated']]

    result.to_csv(output_file, index=False, header=False, encoding="utf-8-sig")

    return result

if __name__ == '__main__':
    get_published_info(
        input_file1=r"D:\output\csv\article_info.csv", input_file2=r"D:\output\csv\venue_info.csv", output_file=r"D:\output\csv\PUBLISH_IN.csv"
    )
