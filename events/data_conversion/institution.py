#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/23 14:46
# @Author  : AllenWan
# @File    : institution.py
import pandas as pd


def process_csv(file_path, output_path):
    """
    读取 CSV 文件并处理指定列，连接处理后的列到第一列并保存结果。

    :param file_path: str，输入 CSV 文件路径。
    :param output_path: str，输出 CSV 文件路径。
    """
    # 读取 CSV 文件
    df = pd.read_csv(file_path)

    df['institution_id'] = ""
    df['location'] = ""

    # 指定需要的列并重命名
    columns_mapping = {
        "InstitutionId": "id",
        "cn_address": "name",
        "en_address": "en_name",

    }

    df = df.rename(columns=columns_mapping)

    # 将列名和数据分别用 § 连接
    column_names = "§".join(df.columns)
    data_rows = df.fillna("").astype(str).apply("§".join, axis=1)

    # 合并列名和数据
    result = pd.DataFrame(data_rows.tolist(), columns=[column_names])

    # 保存到新的 CSV 文件
    result.to_csv(output_path, index=False,encoding="utf-8-sig")


if __name__ == '__main__':
    input_file = r"D:\output\csv\institution_info.csv"
    output_file = r'D:\output\csv\Institution.csv'
    process_csv(input_file, output_file)