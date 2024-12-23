#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/26 09:45
# @Author  : AllenWan
# @File    : CLAIMED_BY.py
import pandas as pd
from redis.commands.search.result import Result


def get_claimed_info(input_file1, input_file2, output_file):
    df1 = pd.read_csv(input_file1)
    df1_selected = df1[["_id", "article_id", "journal_title"]]
    df1_renamed = df1_selected.rename(columns={"_id": "id"})
    df2 = pd.read_csv(input_file2)
    df2_selected = df2[["article_id", "journal_title", "InstitutionId"]]
    merged = df1_renamed.merge(df2_selected, left_on=["article_id", "journal_title"], right_on=["article_id", "journal_title"])
    result_column_list = ["id", "InstitutionId"]
    merged["concate"] = merged[result_column_list].fillna("").astype(str).apply("⌘".join, axis=1)

    result = merged[["concate"]]
    result = result.drop_duplicates(subset="concate", keep="first")
    result.to_csv(output_file, index=False, header=False, encoding="utf-8-sig")

    return result

if __name__ == "__main__":
    get_claimed_info(
        input_file1=r"D:\output\csv_data\article_info.csv_data",
        input_file2=r"D:\output\csv_data\author_info_id.csv_data",
        output_file=r"D:\output\csv\CLAIMED_BY.csv"
    )