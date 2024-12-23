#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/22 19:06
# @Author  : AllenWan
# @File    : pdf_download_bupa.py
import pandas as pd




def filter_and_save_csv(file1, file2, output_file, key_column="pdf_link"):
    """
    从第一个 CSV 文件中过滤出 key_column 在第二个 CSV 中不存在的行，并保存到一个新文件中。

    :param file1: 第一个 CSV 文件路径（主表）
    :param file2: 第二个 CSV 文件路径（过滤依据表）
    :param output_file: 输出文件路径
    :param key_column: 用于匹配的列名，默认为 "pdf_link"
    """
    try:
        # 读取两个 CSV 文件
        df1 = pd.read_csv(file1)
        df2 = pd.read_csv(file2)

        # 检查 key_column 是否在两个表中
        if key_column not in df1.columns or key_column not in df2.columns:
            raise ValueError(f"列 '{key_column}' 不存在于文件中，请检查输入数据。")

        # 找到表一中 key_column 不在表二中的数据
        filtered_df = df1[~df1[key_column].isin(df2[key_column])]

        # 保存到新文件
        filtered_df.to_csv(output_file, index=False)
        print(f"结果已保存到 '{output_file}'")
    except Exception as e:
        print(f"发生错误: {e}")


if __name__ == '__main__':
    file1 = r"C:\Users\PY-01\Documents\local\renHeHuiZhi\article_info_v2.csv_data"
    file2 = r"C:\Users\PY-01\Documents\local\renHeHuiZhi\download_pdf_log.csv_data"
    output_file = r'/events/modules\chineseoptics\seeds\pdf_seeds.csv_data'

    filter_and_save_csv(file1, file2, output_file)