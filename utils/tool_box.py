#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/21 08:50
# @Author  : AllenWan
# @File    : tool_box.py
# @Desc    ：
import hashlib



def generate_id(encrypt_str: str) -> str:
    # 使用 SHA256 算法生成哈希值
    hash_object = hashlib.sha256(encrypt_str.encode())

    # 获取十六进制的哈希值
    hex_digest = hash_object.hexdigest()

    # 截取前 24 个字符作为 ID
    return hex_digest[:24]