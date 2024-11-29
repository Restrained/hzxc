#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/23 14:37
# @Author  : AllenWan
# @File    : achievement_join.py
import pandas as pd

import pandas as pd

from events.data_conversion.clean_data import clean_text


def process_table1_with_authors(table1_file, table2_file, output_file):
    """
    处理表1，将指定列与表2进行关联后生成最终数据，并修正空值和引号问题。

    :param table1_file: str, 表1的文件路径。
    :param table2_file: str, 表2的文件路径 (包含作者信息)。
    :param output_file: str, 处理后的文件路径。
    """
    # 读取表1
    table1 = pd.read_csv(table1_file)

    # 读取表2
    table2 = pd.read_csv(table2_file)


    # 表1与表2关联，选择表2需要的列
    table2_selected_columns = ["authorId", "cn_name", "en_name", "cn_address", "InstitutionId", "article_id",
                               "journal_title", "issn"]
    table2 = table2[table2_selected_columns]

    # 合并表1和表2，基于 article_id, journal_title, issn
    merged = table1.merge(
        table2,
        on=["article_id", "journal_title", "issn"],  # 关联主键
        how="left"
    )

    # 处理作者信息
    # 如果 cn_name 为空，则使用 en_name，最终重命名为 name
    merged['name'] = merged['cn_name'].fillna(merged['en_name'])

    # 选择最终需要的列，并重命名
    final_columns = [
        "id", "title", "en_title", "doi", "source", "abstracts", "en_abstracts",
        "keywords", "en_keywords", "lang", "status", "publish_date", "type",
        "authorId", "name", "cn_address", "InstitutionId"
    ]

    merged_final = merged[final_columns]


    #
    merged_final["name"] = merged_final["name"].apply(clean_text)
    merged_final["cn_address"] = merged_final["cn_address"].apply(clean_text)

    # 保存合并后的数据到输出文件
    merged_final.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"处理后的数据已保存至: {output_file}")


if __name__ == '__main__':
    input_file = r"D:\output\csv\article_info.csv"
    input_file2 = r"D:\output\csv\author_info_id.csv"
    output_file = r'D:\output\csv\achievement_info.csv'

    process_table1_with_authors(input_file, input_file2, output_file)