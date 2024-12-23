#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/23 17:24
# @Author  : AllenWan
# @File    : founder_author.py
# @Desc    ：
import time

import loguru

from csv_data.file import CSVFile
from csv_data.operation import CSVProcessor, SpecificOperationStrategy, DefaultValueOperation, CompositeOperation, \
    DuplicateOperation


class OperationStrategy(CSVProcessor):
    """
    Description:
    1. 类本身继承自CSVProcessor，该父类主要定义了csv常规的一些处理方法
    2. init传参分为CSVFile和SpecificOperationStrategy两部分，
        CSVFil用于父类继承传参，是一个csv file的基础属性定义对象；
        SpecificOperationStrategy主要是针对该文件一些特殊的操作方法， strategy是一个方法列表，可以包括多个方法
    3. specific_operation主要是用于执行传入的策略方法
    4. process用于定义方法处理的具体流程

    notice:
    整体操作为流式执行，不支持异步

    """
    def __init__(self, csv_file: CSVFile, strategy: SpecificOperationStrategy):
        super().__init__(csv_file)
        self.strategy = strategy

    def specific_operation(self):
        self.data = self.strategy.exec(self.data)

    def process(self):
        """
        定义默认处理流程
        :return:
        """
        start_time = time.time()

        self.drop_columns()
        self.fill_na()

        fill_na_end_time = time.time()
        loguru.logger.info(f"self.fill_na()去除nan操作完成，耗时：{fill_na_end_time - start_time:.6f}")
        loguru.logger.info("*" * 100)

        self.rename_columns()
        rename_columns_end_time = time.time()
        loguru.logger.info(f"self.rename_columns()重命名操作完成，耗时：{rename_columns_end_time - fill_na_end_time:.6f}")
        loguru.logger.info("*" * 100)



        self.specific_operation()
        specific_operation_end_time = time.time()
        loguru.logger.info(f"self.specific_operation()特定操作集合操作完成，耗时：{specific_operation_end_time - rename_columns_end_time:.6f}")
        loguru.logger.info("*" * 100)

        self.join_columns()

        self.output_file()
        output_file_end_time = time.time()
        loguru.logger.info(f"self.batch_clean_text_end_time()输出文件操作完成，耗时：{output_file_end_time - specific_operation_end_time:.6f}")
        loguru.logger.info("*" * 100)




def main():
    achievement_file_path = r"D:\pyProject\hzcx\output\founder_article_info.csv"
    achievement = CSVFile(file_path=achievement_file_path)


    # 指定列重命名
    achievement.mapping_list = {
        "article_id": "id",
        "title": "article_title",
        "en_article_title": "en_title"
    }

    achievement.primary_key = ["id", "source"]
    achievement.drop_columns = ["_id"]
    achievement.default_value_list = [{"status": 0}]


    achievement.output_path=r"D:\pyProject\hzcx\output\founder_article_info_output.csv"

    achievement_strategy = CompositeOperation([
        DefaultValueOperation(achievement.default_value_list),
        DuplicateOperation(primary_key=achievement.primary_key),
    ])

    achievement.output_columns = [
        "id", "title", "en_title", "doi", "authors", "source", "abstracts", "en_abstracts",
        "keywords", "en_keywords", "lang", "status","type", "publish_date",

    ]


    achievement_processor = OperationStrategy(achievement, achievement_strategy)
    achievement_processor.process()
    achievement_processor.output_file()


if __name__ == '__main__':
    main()