#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/23 17:21
# @Author  : AllenWan
# @File    : achievement_.py
import csv
from tkinter.constants import FIRST

import pandas as pd
import json


def process_data(input_file, output_file):
    """
    处理数据，按 id 分组后，将 cn_name, authorId, cn_address, InstitutionId 合并为 authors 列。
    如果 cn_name 为空，则用 en_name 填充。
    所有列使用 ⌘ 连接，并保存最终合并的结果。

    :param input_file: str, 输入文件路径。
    :param output_file: str, 输出文件路径。
    """
    # 读取数据
    df = pd.read_csv(input_file)

    # 处理作者信息，按 id 分组并合并相关字段
    def process_authors(group):

        # 格式化为所需的结构 [{"id": "", "name": "", "org": "", "org_id": ""}]
        authors = [
            {
                "id": row['authorId'],
                "name": row['name'],
                "org": row['cn_address'],
                "org_id": row['InstitutionId']
            }
            for _, row in group.iterrows()
        ]

        # 将作者信息转换为 JSON 字符串格式
        json_string = json.dumps(authors, ensure_ascii=False)

        # 还原双引号，去掉转义字符
        return json_string.replace(r'\"', '"')

    # 删除 'authorId' 列为 NaN 的行
    df_cleaned = df.dropna(subset=['authorId'])

    authors_data = df_cleaned.groupby('id').apply(process_authors).reset_index(name='authors')

    # 对其他列去重，确保每个 id 的其他列保留一行
    other_columns = df_cleaned.drop(['name', 'authorId', 'cn_address', 'InstitutionId'], axis=1).drop_duplicates(
        subset=['id'], keep=FIRST)


    # 合并 authors 列和其他列
    result_df = pd.merge(other_columns, authors_data, on='id', how='left')

    # 将所有列用 ⌘ 连接
    column_names = "⌘".join(result_df.columns)
    data_rows = result_df.fillna("").astype(str).apply("⌘".join, axis=1)

    # 合并列名和数据
    final_result = pd.DataFrame([column_names] + data_rows.tolist(), columns=["data"])

    # 直接将处理好的数据写入文本文件
    with open(output_file, 'w', encoding='utf-8') as f:
        for row in final_result.itertuples(index=False, name=None):
            # 将每一行的列数据按需要格式化并写入
            # 假设数据是 JSON 格式化的字符串，先去掉可能存在的双引号转义
            row_data = '⌘'.join(map(str, row))  # 将每一行的数据用 "⌘" 分隔
            f.write(row_data + '\n')  # 逐行写入
    # # 保存到输出文件
    # final_result.to_csv(output_file, index=False, header=False, quoting=csv.QUOTE_NONE, escapechar=None, quotechar='')
    # print(f"处理后的数据已保存至: {output_file}")

if __name__ == '__main__':
    input_file = r'D:\output\csv\achievement_info.csv'
    output_file = r'D:\output\csv\Achievement.csv'
    process_data(input_file, output_file)