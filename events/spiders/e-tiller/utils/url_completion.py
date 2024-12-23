#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/11 17:10
# @Author  : AllenWan
# @File    : url_completion.py
# @Desc    ：
import pandas as pd
import requests

def check_protocol(domain):
    """
    检测域名是支持 http 还是 https
    :param domain: 域名（不带协议）
    :return: 支持的协议类型，'http' 或 'https'
    """
    domain = domain.strip()  # 清理前后空格
    protocols = ["https", "http"]
    for protocol in protocols:
        url = f"{protocol}://{domain}"
        try:
            response = requests.head(url, timeout=5)  # 尝试 HEAD 请求
            if response.status_code < 400:  # 成功响应
                return url
        except requests.exceptions.RequestException:
            continue  # 尝试下一个协议
    return None  # 如果都不支持

def process_csv(file_path, output_path):
    # 读取 CSV 文件
    df = pd.read_csv(file_path, encoding='utf-8-sig')

    # 确保 'officialWebsite' 列存在
    if 'officialWebsite' not in df.columns:
        raise ValueError("The CSV file must contain a column named 'officialWebsite'.")

    # 检查并更新协议
    def update_website(website):
        if isinstance(website, str) and not website.startswith("http"):
            return check_protocol(website) or website  # 如果无法判断协议，则保留原始值
        return website  # 已以 http/https 开头，保持不变

    # 更新列
    df['updatedWebsite'] = df['officialWebsite'].apply(update_website)

    # 保存结果到新的 CSV 文件
    df.to_csv(output_path, index=False)
    print(f"Processed data saved to {output_path}")

# 示例调用
input_file = r"C:\Users\PY-01\Documents\e-tiller.csv"  # 输入文件路径
output_file = r"C:\Users\PY-01\Documents\e-tiller_output.csv"  # 输出文件路径

process_csv(input_file, output_file)
