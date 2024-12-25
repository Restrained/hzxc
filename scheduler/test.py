#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/24 17:18
# @Author  : AllenWan
# @File    : test.py
# @Desc    ：


from scheduler.tasks import run_spider, chain_task

# result = run_spider.delay()  # 异步调用任务
# print(result.get())       # 获取任务结果

# 通过调用链式任务来启动爬虫
chain_task.apply_async()

print("爬虫调度启动成功！")