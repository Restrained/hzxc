#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/24 17:18
# @Author  : AllenWan
# @File    : test.py
# @Desc    ：


from scheduler.tasks import run_spider

result = run_spider.delay()  # 异步调用任务
print(result.get())       # 获取任务结果