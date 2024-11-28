#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/26 11:04
# @Author  : AllenWan
# @File    : Fund.py
import uuid

import pandas as pd


def get_funds_info(input_file, output_file, output_file2):
    df = pd.read_csv(input_file)
    # 筛选出 cn_funds 列不为空或不为 NaN 的行
    df_filtered = df[df['cn_funds'].str.strip().notna() & (df['cn_funds'].str.strip() != '')]

    selected_list = ["cn_funds", "en_funds"]
    df_selected = df_filtered[selected_list].drop_duplicates(subset=['cn_funds'], keep='first')
    df_selected["id"] = [str(uuid.uuid4()).replace("-", "")[:24] for _ in range(len(df_selected))]



    data_rows = df_selected[["id", "cn_funds", "en_funds"]].fillna("").astype(str).apply("§".join, axis=1)
    colunmn_name = "§".join(["id", "name", "en_name"])
    result = pd.DataFrame(data_rows, columns=[colunmn_name])
    df_selected.to_csv(output_file2, index=False, encoding='utf-8-sig')
    result.to_csv(output_file, index=False, encoding='utf-8-sig')
    return result

if __name__ == '__main__':
    get_funds_info(r"D:\output\csv\article_info.csv", r"D:\output\csv\Fund.csv", r"D:\output\csv\funds_info.csv")



