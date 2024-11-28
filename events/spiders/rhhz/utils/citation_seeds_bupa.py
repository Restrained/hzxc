#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/25 15:30
# @Author  : AllenWan
# @File    : citation_seeds_bupa.py

import pandas as pd


def find_unmatched_rows(csv1_path, csv2_path, csv1_key, csv2_key, output_path=None):
    """
    从两个 CSV 文件中关联数据，并取出表 1 中不在表 2 的所有行。

    Args:
        csv1_path (str): 表 1 的 CSV 文件路径。
        csv2_path (str): 表 2 的 CSV 文件路径。
        csv1_key (str): 表 1 用于关联的列名。
        csv2_key (str): 表 2 用于关联的列名。
        output_path (str, optional): 如果指定，将结果保存到此路径的 CSV 文件。

    Returns:
        pd.DataFrame: 表 1 中不在表 2 中的所有数据。
    """
    # 读取两个 CSV 文件
    df1 = pd.read_csv(csv1_path)
    df2 = pd.read_csv(csv2_path)

    # 使用 left join 找出表 1 中不在表 2 的数据
    merged = pd.merge(df1, df2, left_on=csv1_key, right_on=csv2_key, how='left', indicator=True)

    # 筛选出表 1 中没有匹配到表 2 的数据
    unmatched = merged[merged['_merge'] == 'left_only']

    # 删除 merge 指示列
    unmatched = unmatched.drop(columns=['_merge'])

    # 保存结果到文件（如果指定了 output_path）
    if output_path:
        unmatched.to_csv(output_path, index=False, encoding='utf-8-sig')

    return unmatched


if __name__ == '__main__':
    find_unmatched_rows(
        r"/events/modules/rhhz/seeds/citation_seeds.csv",
        r"C:\Users\PY-01\Documents\local\renHeHuiZhi\citation_info_v2.csv",
        ['article_id', 'journal_title'],
        ['article_id', 'journal_title'],
        output_path=r'/events/modules\chineseoptics\seeds\citation_seeds_bupa.csv'

    )