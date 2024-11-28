#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/25 16:29
# @Author  : AllenWan
# @File    : citation_data_concatenate.py


import pandas as pd


def concatenate_csv(csv1_path, csv2_path, output_path=None):
    """
    拼接两个具有相同列的 CSV 文件数据。

    Args:
        csv1_path (str): 第一个 CSV 文件路径。
        csv2_path (str): 第二个 CSV 文件路径。
        output_path (str, optional): 如果指定，将结果保存到此路径的 CSV 文件。

    Returns:
        pd.DataFrame: 拼接后的 DataFrame。
    """
    # 读取两个 CSV 文件
    df1 = pd.read_csv(csv1_path)
    df2 = pd.read_csv(csv2_path)

    # 拼接两个 DataFrame
    combined_df = pd.concat([df1, df2], ignore_index=True)

    # 保存结果到文件（如果指定了 output_path）
    if output_path:
        combined_df.to_csv(output_path, index=False, encoding='utf-8-sig')

    return combined_df


if __name__ == '__main__':
    result = concatenate_csv(
        csv1_path=r"C:\Users\PY-01\Documents\local\renHeHuiZhi\citation_info\citation_info.csv",
        csv2_path=r"C:\Users\PY-01\Documents\local\renHeHuiZhi\citation_info_v2.csv",
        output_path="D:\output\csv\citation_info.csv"
    )

    print(result)