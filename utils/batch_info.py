#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/27 15:52
# @Author  : AllenWan
# @File    : batch_info.py
import datetime



class BatchProcessor:

    @staticmethod
    def get_weekly_batch(input_date: str) -> str:
        # 解析日期输入，假设输入是 YYYY-MM-DD 格式
        date = datetime.datetime.strptime(input_date, "%Y-%m-%d")

        # 获取当前日期的月份和年份
        year = date.year
        month = date.month

        # 计算当月的第几天
        day_of_month = date.day

        # 如果日期是22号之后，则为最后一批次
        if day_of_month >= 22:
            batch_start_date = datetime.datetime(year, month, 22)
        else:
            # 计算批次开始日期（每周的起始日期）
            week_start_day = (day_of_month - 1) // 7 * 7 + 1
            batch_start_date = datetime.datetime(year, month, week_start_day)

        # 返回批次名称，批次开始日期格式化为 YYYY-MM-DD
        batch_name = batch_start_date.strftime("%Y-%m-%d")

        return batch_name

    @staticmethod
    def get_month_batch(input_date: str) -> str:
        date = datetime.datetime.strptime(input_date, "%Y-%m-%d")

        # 获取当前月份的第一天
        first_day_of_month = datetime.datetime(date.year, date.month, 1)

        # 返回格式化后的日期，YYYY-MM-DD
        return first_day_of_month.strftime("%Y-%m-%d")

    @staticmethod
    def get_half_month_batch(input_date: str) -> str:
        date = datetime.datetime.strptime(input_date, "%Y-%m-%d")

        day = date.day
        return datetime.datetime(date.year, date.month, 1).strftime("%Y-%m-%d") if day < 15 \
            else datetime.datetime(date.year, date.month, 15).strftime("%Y-%m-%d")

    @classmethod
    def get_batch_id(
            cls,
            batch_key: str = "DAILY",
            input_date: str = datetime.datetime.now().strftime("%Y-%m-%d")) -> str:

        batch_key = batch_key.upper()
        # 判断日期格式是否为"%Y-%m-%d"
        try:
            datetime.datetime.strptime(input_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError("Input date must be YYYY-MM-DD")

        if batch_key == "WEEKLY":
            return cls.get_weekly_batch(input_date)
        elif batch_key == "MONTHLY":
            return cls.get_month_batch(input_date)
        elif batch_key == "HALF-MONTHLY":
            return cls.get_half_month_batch(input_date)
        else:
            return input_date


if __name__ == '__main__':
    # batch_processor = BatchProcessor()
    # print(batch_processor.get_batch_id(
    #     batch_key="HalfMonthly",
    # ))
    print(BatchProcessor.get_batch_id())