#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/14 11:06
# @Author  : AllenWan
# @File    : url.py
from urllib.parse import urlparse


def normalize_url(url: str) -> str:
    # 如果 URL 不以 http:// 或 https:// 开头，添加 http://
    if not url.startswith(('http://', 'https://')):
        url = 'http://' + url

    # 解析 URL
    parsed_url = urlparse(url)

    # 提取主机部分（hostname），即域名部分，保留子域名和一级域名
    domain = parsed_url.hostname

    # 构建标准化的 URL（仅保留协议和域名部分）
    normalized_url = f'http://{domain}'

    return normalized_url


def get_base_url(url: str) -> str:
    """
    从给定的 URL 中提取协议和域名，去除路径部分。

    :param url: 完整的 URL 地址
    :return: 只包含协议和域名的基础 URL
    """
    # 如果没有协议头，添加默认的 HTTP 协议
    if not urlparse(url).scheme:
        url = 'http://' + url

    parsed_url = urlparse(url)
    base_url = f'{parsed_url.scheme}://{parsed_url.netloc}'
    return base_url

if __name__ == '__main__':
    href = "http://jlxb.china-csm.org:81/"
    print(get_base_url(href))