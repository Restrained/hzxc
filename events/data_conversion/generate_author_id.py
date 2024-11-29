#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/23 15:10
# @Author  : AllenWan
# @File    : generate_author_id.py

import pandas as pd
import uuid


def process_authors_csv(input_path, output_path):
    """
    读取 CSV 文件，取出指定列，去重后根据 cn_name, en_name, mail 生成 authorId。

    :param input_path: str，输入 CSV 文件路径。
    :param output_path: str，输出处理后的 CSV 文件路径。
    """
    # 读取 CSV 文件
    df = pd.read_csv(input_path)

    # 指定需要的列
    selected_columns = [
        "cn_name", "en_name", "mail",
        "cn_about_author"
    ]
    df = df[selected_columns]

    # 去重
    df = df.drop_duplicates(subset=["cn_name", "en_name", "mail"])

    # 生成 authorId
    def generate_author_id():
        return str(uuid.uuid4()).replace("-", "")[:24]

    df["authorId"] = df.apply(lambda row: generate_author_id(), axis=1)

    # 保存结果到文件
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

if __name__ == '__main__':
    input_file = r'C:\Users\PY-01\Documents\local\renHeHuiZhi\author_info.csv'
    output_file = r'D:\output\csv\author_info.csv'
    process_authors_csv(input_file, output_file)