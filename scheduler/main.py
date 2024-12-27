#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/24 17:18
# @Author  : AllenWan
# @File    : main.py
# @Desc    ：
from celery import chain

from scheduler.tasks import run_spider, post_process, run_spider_with_monitor

# result = run_spider.delay()  # 异步调用任务
# print(result.get())       # 获取任务结果

# 通过调用链式任务来启动爬虫
# chain_task = chain(
#     run_spider.si(),
#     post_process.si(),
#
# )
# chain_task.apply_async()

run_spider_with_monitor()

print("爬虫调度启动成功！")