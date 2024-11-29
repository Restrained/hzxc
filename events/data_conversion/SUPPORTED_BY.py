#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/26 10:46
# @Author  : AllenWan
# @File    : SUPPORTED_BY.py
import pandas as pd

def get_supported_info(input_file1, input_file2, output_file):
    df1 = pd.read_csv(input_file1)
    df2 = pd.read_csv(input_file2)
    df_filter = df1[["_id", "cn_funds"]].drop_duplicates(subset=["_id", "cn_funds"],keep="first")
    merged = pd.merge(df_filter, df2, left_on='cn_funds', right_on='cn_funds')

    result = merged[["_id", "id"]].fillna("").astype(str).apply("⌘".join, axis=1)
    result.to_csv(output_file, index=False, header=False, encoding="utf-8-sig")
    return result

if __name__ == '__main__':
    get_supported_info(
        input_file1=r"C:\Users\PY-01\Documents\local\renHeHuiZhi\article_info.csv",
        input_file2=r"D:\output\csv\funds_info.csv",
        output_file=r"D:\output\csv\SUPPORTED_BY.csv"
    )