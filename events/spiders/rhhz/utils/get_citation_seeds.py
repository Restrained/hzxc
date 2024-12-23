#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/25 14:46
# @Author  : AllenWan
# @File    : get_citation_seeds.py
import sys

import pandas as pd

def get_citation_seeds(input_file1, input_file2, output_file):
    df1 = pd.read_csv(input_file1)
    df2 = pd.read_csv(input_file2)

    merged = df1.merge(df2, how="left", on='journal_title')

    merged.to_csv(output_file, index=False)

if __name__ == '__main__':
    input_file1 = r"C:\Users\PY-01\Documents\local\renHeHuiZhi\citation_info\find_query.csv_data"
    input_file2 = r"D:\output\csv_data\journal_website_info.csv_data"
    output_file = r"/events/modules/rhhz/seeds/citation_seeds.csv_data"
    get_citation_seeds(input_file1, input_file2, output_file)