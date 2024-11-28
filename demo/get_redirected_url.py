#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/14 11:54
# @Author  : AllenWan
# @File    : get_redirected_url.py
import loguru
import pandas as pd
import requests


headers = {
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
  'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
  'Connection': 'keep-alive',
  'Referer': 'http://www.chineseoptics.net.cn/',
  'Upgrade-Insecure-Requests': '1',
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0'
}

def get_redirected_url(url: str) -> str:
    if not url.startswith(('http://', 'https://')):
        url = 'http://' + url

    try:
        # 发起请求并获取重定向后的URL，allow_redirects=True表示自动跟随重定向
        response = requests.get(url, headers=headers, allow_redirects=True, timeout=15)
        loguru.logger.info(f"{url} 已经下载完毕")
        return response.url  # 返回重定向后的最终URL
    except requests.RequestException as e:
        # 如果请求失败，返回原始URL（或者你可以返回None）
        print(f"请求错误: {e} 对于URL: {url}")
        return url


def process_csv_and_get_redirected_urls(input_file: str, output_file: str):
    # 读取 CSV 文件
    df = pd.read_csv(input_file)

    # 检查是否包含 'officialWebsite' 列
    if 'officialWebsite' not in df.columns:
        raise ValueError("CSV文件中找不到 'officialWebsite' 列")

    # 使用 get_redirected_url 方法获取每个 URL 的重定向地址
    df['redirectedUrl'] = df['officialWebsite'].apply(get_redirected_url)


    # 将结果保存到新的 CSV 文件
    df.to_csv(output_file, index=False)
    print(f"处理后的文件已保存为 {output_file}")


# 示例使用
input_file = r'/events/journalClassification\output\output_v3.csv'  # 输入文件路径
output_file = r'/events/journalClassification\output\output_v3_redirect.csv'  # 输出文件路径
process_csv_and_get_redirected_urls(input_file, output_file)
