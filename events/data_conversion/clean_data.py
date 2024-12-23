#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/28 16:55
# @Author  : AllenWan
# @File    : clean_data.py

import re
import html
import sys

import pandas as pd
from bs4 import BeautifulSoup

def clean_text(text):
    # 检查 text 是否是字符串类型，如果不是则返回空字符串
    if not isinstance(text, str):
        return ''

    # 去除开头和结尾的双引号
    text = text.strip('"')
    # 去除多余的换行符和空白字符
    text = text.replace('\r\n', ' ').replace('\n', ' ') # 替换换行符
    text = re.sub(r'\s+', ' ', text)  # 将多个空格替换为一个空格

    html_text = html.unescape(text)
    soup = BeautifulSoup(html_text, 'html.parser')
    return soup.get_text(separator=' ', strip=True)

def clean_authors(input_file, output_file):
    df = pd.read_csv(input_file)

    df["cn_about_author"] = df["cn_about_author"].apply(clean_text)
    df["en_about_author"] = df["cn_about_author"].apply(clean_text)

    df.to_csv(output_file, index=False)

def clean_institution(input_file, output_file):
    df = pd.read_csv(input_file)

    df["cn_address"] = df["cn_address"].apply(clean_text)
    df["en_address"] = df["en_address"].apply(clean_text)

    df.to_csv(output_file, index=False)

def clean_achievement(input_file, output_file):
    df = pd.read_csv(input_file)

    df["cn_address"] = df["cn_address"].apply(clean_text)
    df["en_address"] = df["en_address"].apply(clean_text)

    df.to_csv(output_file, index=False)

def clean_fund(input_file, output_file):
    df = pd.read_csv(input_file)

    df["cn_funds"] = df["cn_funds"].apply(clean_text)
    df["en_funds"] = df["en_funds"].apply(clean_text)
    df.to_csv(output_file, index=False)

if __name__ == '__main__':
    # clean_authors(
    #     r"D:\output\csv_data\author_info.csv_data",
    #     r"D:\output\csv_data\author_info_clean.csv_data",
    # )

    # clean_institution(
    #     r"D:\output\csv_data\institution_info.csv_data",
    #     r"D:\output\csv_data\institution_info_clean.csv_data",
    # )

    # clean_author_institution_sets(
    #     r"D:\output\csv_data\institution_info.csv_data",
    #     r"D:\output\csv_data\institution_info_clean.csv_data",
    # )

    clean_fund(
        r"D:\output\csv\funds_info.csv",
        r"D:\output\csv\funds_info_clean.csv",
    )