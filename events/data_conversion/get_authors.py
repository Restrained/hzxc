#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/23 15:26
# @Author  : AllenWan
# @File    : get_authors.py
import pandas as pd
import json

import pandas as pd

def merge_table2_with_others(table2_file, authors_file, institutions_file, output_file):
    """
    将表2分别与表3和表4关联，生成结果并导出为 CSV。

    :param table2_file: str, 表2的文件路径。
    :param authors_file: str, 表3的文件路径 (包含 authorId 信息)。
    :param institutions_file: str, 表4的文件路径 (包含 InstitutionId 信息)。
    :param output_file: str, 关联结果的输出文件路径。
    """
    # 读取表2
    table2_df = pd.read_csv(table2_file)

    # 读取表3 (包含 authorId)
    authors_df = pd.read_csv(authors_file)

    # 读取表4 (包含 InstitutionId)
    institutions_df = pd.read_csv(institutions_file)

    # 表2与表3关联
    table2_with_authors = table2_df.merge(
        authors_df,
        on=["cn_name", "en_name", "mail"],
        how="left"
    )

    # 表2与表4关联
    table2_with_institutions = table2_with_authors.merge(
        institutions_df,
        on=["cn_address"],
        how="left"
    )

    # 将关联结果导出为 CSV
    table2_with_institutions.to_csv(output_file, index=False, encoding="utf-8-sig")

if __name__ == '__main__':
    merge_table2_with_others(
        table2_file=r"C:\Users\PY-01\Documents\local\renHeHuiZhi\author_info.csv",
        authors_file=r"D:\output\csv\author_info.csv",
        institutions_file=r"D:\output\csv\institution_info.csv",
        output_file=r"D:\output\csv\author_info_id.csv"
    )