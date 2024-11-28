#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/25 11:08
# @Author  : AllenWan
# @File    : get_venue_info.py
import uuid

import pandas as pd

def merge_journal_info(input_file, outputfile):
    df = pd.read_csv(input_file)

    columns_mapping = {
        "nameOfTheJournal": "name",
    }
    df = df.rename(columns=columns_mapping)
    df = df.drop_duplicates(subset=['name'])
    df['en_name']  = ''
    df['type']  = 'J'
    # 生成唯一 ID 并添加为新列
    df["id"] = [str(uuid.uuid4()).replace("-", "")[:24] for _ in range(len(df))]

    column_list = ['id', 'name', 'en_name', 'type']

    df_selected  = df[column_list]
    # 保存到新的 CSV 文件
    df_selected.to_csv(outputfile, index=False, encoding="utf-8-sig")

if __name__ == '__main__':
    input_file = r'C:\Users\PY-01\Documents\local\renHeHuiZhi\journal_list.csv'
    output_file = r'D:\output\csv\venue_info.csv'
    merge_journal_info(input_file, output_file)