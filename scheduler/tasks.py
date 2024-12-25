#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/24 17:29
# @Author  : AllenWan
# @File    : tasks.py
# @Desc    ：
# tasks.py
from loguru import logger
from bricks.downloader import requests_
from bricks.lib.queues import RedisQueue

from celery_config import app
import time

# from config.config_info import RedisConfig
# from events.spiders.rhhz.modules.article_incremental import ArticleIncrementalCrawler

@app.task
def run_spider():
    """
    启动爬虫接口
    :return:
    """
    # 打印说明
    logger.info(f"启动爬虫任务：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")

    # # 初始化爬虫
    # spider = ArticleIncrementalCrawler(
    #     concurrency=1,
    #     **{
    #         "init_queue_size": 1000000
    #     },
    #     downloader=requests_.Downloader(),
    #     task_queue=RedisQueue(
    #         host=RedisConfig.host,
    #         port=RedisConfig.port,
    #         password=RedisConfig.password,
    #         database=RedisConfig.database
    #     )
    # )
    #
    # # 启动爬虫
    # spider.run()

    # 爬虫完成后执行下一个任务
    # loguru.logger.info(f"爬虫任务完成，开始下一个任务：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")

    return 'Spider Finished'

@app.task
def post_process():
    logger.info("后续任务执行")
    # 后处理逻辑，比如数据保存到数据库等
    return 'Post Processing Finished'


# 任务链
@app.task
def chain_task():
    result = run_spider.apply_async(link=post_process.s())
    return result