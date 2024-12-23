#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/6 16:40
# @Author  : AllenWan
# @File    : task_schedule.py
# @Desc    ：
import base64
from celery import Celery, chain
import time
from config.config_info import RedisConfig
from celery.schedules import crontab

# 创建 Celery 应用实例
app = Celery('tasks', broker='redis://localhost:6379/0')

@app.task
def task1():
    print("Running Task 1...")
    time.sleep(3)  # 模拟爬取数据
    print("Task 1 completed.")
    return "Task 1 Result"

@app.task
def middle_task(task1_result):
    print(f"Running Middle Task with input: {task1_result}")
    time.sleep(3)
    print("Middle Task completed.")
    return "Middle Task Result"

@app.task
def task2(middle_task_result):
    print(f"Running Task 2 with input: {middle_task_result}")
    time.sleep(3)
    print("Task 2 completed.")
    return "Task 2 Result"

# 配置 Celery 定时任务
app.conf.beat_schedule = {
    'task1-every-30-minutes': {
        'task': 'tasks.task1',
        'schedule': crontab(minute="0", hour='*/1'),  # 每小时执行一次任务1
    },
}

# 任务链：任务1 -> 中间任务 -> 任务2
@app.task
def run_tasks():
    # 任务1 -> 中间任务 -> 任务2
    chain(task1.s(), middle_task.s(), task2.s()).apply_async()




