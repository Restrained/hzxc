#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/6 17:15
# @Author  : AllenWan
# @File    : title_repair.py
# @Desc    ：
import pandas as pd

# 读取两个CSV文件
df1 = pd.read_csv(r"C:\Users\PY-01\Documents\local\renHeHuiZhi\article_list\origin_name.csv")
df2 = pd.read_csv(r"C:\Users\PY-01\Documents\local\renHeHuiZhi\article_list\aggregation_query.csv")

# 确保两个CSV文件行数相同（可以加上检查）
if len(df1) != len(df2):
    raise ValueError("两个 CSV 文件的行数不相同！")

# 遍历每一行进行对比
for index, (row1, row2) in enumerate(zip(df1.iterrows(), df2.iterrows())):
    # row1[1] 和 row2[1] 是行数据，逐列比较
    diff_columns = []
    for col1, col2 in zip(row1[1], row2[1]):
        if col1 != col2:  # 数据不同
            diff_columns.append((col1, col2))

    if diff_columns:
        print(f"第 {index + 1} 行有不同的数据：")
        for col1, col2 in diff_columns:
            print(f"  数据不同: {col1} -> {col2}")
        print()  # 空行分隔不同的行

