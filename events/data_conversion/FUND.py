#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/26 11:04
# @Author  : AllenWan
# @File    : FUND.py
import uuid

import pandas as pd


def extract_funds_info(input_file, output_file, output_file2):
    df = pd.read_csv(input_file)
    # 筛选出 cn_funds 列不为空或不为 NaN 的行
    df_filtered = df[df['cn_funds'].str.strip().notna() & (df['cn_funds'].str.strip() != '')]

    selected_list = ["cn_funds", "en_funds"]
    df_selected = df_filtered[selected_list].drop_duplicates(subset=['cn_funds'], keep='first')
    df_selected["id"] = [str(uuid.uuid4()).replace("-", "")[:24] for _ in range(len(df_selected))]


    df_selected.to_csv(output_file2, index=False, encoding='utf-8-sig')



def get_funds(input_file,output_file):
    df = pd.read_csv(input_file)
    df_join = df[["id", "cn_funds", "en_funds"]].fillna("").astype(str).apply("⌘".join, axis=1)
    colunmn_name = "⌘".join(["id", "name", "en_name"])

    result = pd.DataFrame([colunmn_name] + df_join.tolist(), columns=["data"])



    # 直接将处理好的数据写入文本文件
    with open(output_file, 'w', encoding='utf-8') as f:
        for row in result.itertuples(index=False, name=None):
            # 将每一行的列数据按需要格式化并写入
            # 假设数据是 JSON 格式化的字符串，先去掉可能存在的双引号转义
            row_data = '⌘'.join(map(str, row))  # 将每一行的数据用 "⌘" 分隔
            f.write(row_data + '\n')  # 逐行写入



if __name__ == '__main__':
    get_funds(r"D:\output\csv\funds_info_clean.csv", r"D:\output\csv\Fund.csv")



