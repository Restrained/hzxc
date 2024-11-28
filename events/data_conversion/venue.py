#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/25 11:36
# @Author  : AllenWan
# @File    : venue.py

import pandas as pd

def merge_venue_columns(input_file, output_file):
    df = pd.read_csv(input_file)

    # 将列名和数据分别用 § 连接
    column_names = "§".join(df.columns)
    data_rows = df.fillna("").astype(str).apply("§".join, axis=1)

    # 合并列名和数据
    result = pd.DataFrame(data_rows.tolist(), columns=[column_names])

    # 保存到新的 CSV 文件
    result.to_csv(output_file, index=False,encoding="utf-8-sig")

if __name__ == '__main__':
    input_file = r"D:\output\csv\venue_info.csv"
    output_file = r"D:\output\csv\Venue.csv"
    merge_venue_columns(input_file, output_file)
