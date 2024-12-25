#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/24 17:30
# @Author  : AllenWan
# @File    : celery_config.py
# @Desc    ：
from celery import Celery


# celery_config.py
from celery import Celery
from celery.schedules import crontab



# 初始化 Celery 实例，指定任务队列的 broker（使用 Redis）
app = Celery('spider', broker='redis://:Hzxc2023@192.168.0.41:16379/9')

# 配置 Celery 的结果存储（可以使用 Redis 或其他存储）
app.conf.result_backend = 'redis://:Hzxc2023@192.168.0.41:16379/9'

# 自动发现任务模块中的任务
app.autodiscover_tasks(['scheduler.tasks'])

# 配置定时任务，每分钟执行一次任务
app.conf.beat_schedule = {
    'run-spider-daily': {
        'task': 'scheduler.tasks.run_spider',
        'schedule': crontab(hour='0', minute="0"),  # 每60秒运行一次
    },
    'run-spider-every-minute': {
        'task': 'tasks.run_spider',
        'schedule': 60.0,  # 每60秒运行一次
    },
}
