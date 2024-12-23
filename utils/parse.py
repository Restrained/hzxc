#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/16 17:07
# @Author  : AllenWan
# @File    : parse.py
# @Desc    ：
import re

def contains_chinese(s: str) -> bool:
    # 正则表达式匹配中文字符
    return bool(re.search(r'[\u4e00-\u9fa5]', s))

def contains_no_chinese(s: str) -> bool:
    # 如果不包含中文字符，返回True
    return not contains_chinese(s)