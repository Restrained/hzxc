#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/24 17:29
# @Author  : AllenWan
# @File    : tasks.py
# @Desc    ：
# tasks.py
import base64
import subprocess

from celery import chain
from celery.result import AsyncResult
from loguru import logger
from bricks.downloader import requests_
from pymongo import MongoClient

from celery_config import app
import time

from config.config_info import RedisConfig, MongoConfig
from db.mongo import MongoInfo
from events.spiders.rhhz.modules.article_incremental import ArticleIncrementalCrawler
from notice.feishu import FeiShu


@app.task(ignore_result=True)
def run_spider():
    """
    启动爬虫接口
    :return:
    """
    # # 打印说明
    logger.info(f"启动爬虫任务：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")


    spider = ArticleIncrementalCrawler(
        concurrency=1,
        **{"init_queue_size": 1000000},
        queue_name="article_incremental",
        downloader=requests_.Downloader(),

        # proxy=proxy

    )
    # 启动爬虫
    spider.run(task_name="all")

    """
      启动爬虫接口，通过 screen 启动任务
      :return:
      """
    # 打印说明
    logger.info(f"启动爬虫任务：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")

    # 设置screen会话的名字
    session_name = f"spider_task_{int(time.time())}"  # 使用时间戳作为唯一标识
    command = f"screen -dmS {session_name} python -c 'from your_crawler_module import ArticleIncrementalCrawler; from your_downloader_module import requests_; spider = ArticleIncrementalCrawler(concurrency=1, init_queue_size=1000000, queue_name=\"article_incremental\", downloader=requests_.Downloader()); spider.run(task_name=\"all\")'"

    # 使用 subprocess 启动 screen 会话并执行爬虫
    subprocess.run(command, shell=True)

    logger.info(f"爬虫任务已启动，screen 会话名: {session_name}")

    # 爬虫完成后执行下一个任务
    # loguru.logger.info(f"爬虫任务完成，开始下一个任务：{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")

    return 'Spider Finished'

@app.task
def monitor_task(task_id):
    logger.info(f"监控任务启动，监控爬虫任务{task_id}")
    mongo = MongoInfo(
        host=MongoConfig.host,
        port=MongoConfig.port,
        username=base64.b64decode(MongoConfig.username).decode("utf-8"),
        password=base64.b64decode(MongoConfig.password).decode("utf-8"),
        database=MongoConfig.database,
        auth_database=MongoConfig.auth_database
    )
    while True:
        # 获取爬虫任务结果
        result = AsyncResult(task_id)
        if result.ready():
            if result.successful():
                logger.info("爬虫任务执行成功，监控停止")
            else:
                logger.warning(f"爬虫任务失败， 错误信息： {result.result}")
            break
        else:
            logger.info("爬虫任务还在执行，继续监控......")

            # 当前时间
            current_time = int(time.time() * 1000)

            # 过去5分钟的时间
            past_5_minutes = current_time - 2 * 60 * 1000

            check_data_changes(mongo, past_5_minutes, current_time, collection_name="article_list_incremental",
                               column_name="time_stamp", task_id=task_id)
            time.sleep(2 * 60)



def send_msg(msg: str, title: str) -> None:
    notice_instance = FeiShu()
    notice_instance.send(msgs=msg, title=title)

# 数据库查询（暂时从mongo中查询）
def check_data_changes(client:MongoClient, start_time: int, end_time: int, collection_name: str, column_name: str, task_id):
    db = client[MongoConfig.database]  # 替换为实际的数据库名
    collection = db[collection_name]  # 替换为实际的集合名
    query_result = collection.count_documents(
        filter={
            column_name: {
                "$gte": start_time,  # >= 过去5分钟的时间戳
                "$lte": end_time  # <= 当前时间戳
            }}
    )
    if query_result == 0:
        msg = f"增量爬取数据增长异常，请及时修复"
        title = f"增量爬取监控,任务id：{task_id}"
        send_msg(msg, title)

    return query_result

@app.task
def run_spider_with_monitor():
    # 启动爬虫任务

    spider_task = run_spider.apply_async()

    # 启动监控任务，传递爬虫任务的ID, 120秒后延迟启动
    monitor_task.apply_async(args=[spider_task.id], countdown=2*60)



@app.task(ignore_result=True)
def post_process():
    logger.info("后续任务执行")
    # 后处理逻辑，比如数据保存到数据库等
    return 'Post Processing Finished'


# 任务链
@app.task
def run_spider_chain():
    # 调用任务链
    chain(
        run_spider.si(),
        post_process.si(),
    ).apply_async()

