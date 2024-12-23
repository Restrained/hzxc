#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/20 15:00
# @Author  : AllenWan
# @File    : dates.py
from datetime import datetime, timezone, timedelta

def timestamp_to_date(ts):
    """
    将时间戳转换为日期对象，支持秒级和毫秒级时间戳。
    :param ts: 时间戳
    :return: 返回格式为：%Y-%m-%d %H:%M:%S 的日期
    """


    if ts < 0:
        if len(str(ts)) >= 13:
            ts = ts / 1000
        # 使用 UTC 时区，确保时间戳正确转换
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        dt = epoch + timedelta(seconds=ts)

        return dt.strftime('%Y-%m-%d')

    try:
        ts = int(ts)
    except ValueError:
        return "时间戳输入错误，请检查后重试！"

    if len(str(ts)) == 10:
        date_obj = datetime.fromtimestamp(ts)
    elif len(str(ts)) == 13:
        date_obj = datetime.fromtimestamp(ts / 1000)
    else:
        return "时间戳输入错误，请检查后重试！"


    return date_obj.strftime("%Y-%m-%d")


from datetime import datetime


def format_date(date_str: str) -> str:
    """
    将日期字符串 'YYYY-MM-DD HH:MM:SS' 转换为 'YYYY-MM-DD' 格式。

    :param date_str: 输入的日期时间字符串
    :return: 格式化后的日期字符串
    """

    if not date_str:
        return ""
    # 解析字符串为 datetime 对象
    dt = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')

    # 格式化为所需的日期格式
    return dt.strftime('%Y-%m-%d')


