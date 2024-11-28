#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/25 14:23
# @Author  : AllenWan
# @File    : extract_domain_info.py
import pandas as pd
from urllib.parse import urlparse

# 读取包含 'url' 和 'journal_title' 的 CSV 文件
input_file = r"C:\Users\PY-01\Documents\local\renHeHuiZhi\article_info.csv"  # 替换为你的文件路径
output_file = 'D:\output\csv\journal_website_info.csv'  # 输出文件路径
df = pd.read_csv(input_file)

# 提取域名
df['domain'] = df['url'].apply(lambda x:  f"{urlparse(x).scheme}://{urlparse(x).netloc}")

# 根据 'domain' 和 'journal_title' 去重
df_unique = df.drop_duplicates(subset=['domain', 'journal_title'])

result = df_unique.drop(columns=['url'])

# 输出去重后的结果
result.to_csv(output_file, index=False, encoding='utf-8-sig')