#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/23 15:30
# @Author  : AllenWan
# @File    : founder_achievement.py
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
        loguru.logger.info(
            f"self.specific_operation()特定操作集合操作完成，耗时：{specific_operation_end_time - rename_columns_end_time:.6f}")
        loguru.logger.info("*" * 100)

        self.filter_columns()
        self.join_columns()

        self.output_file()
        output_file_end_time = time.time()
        loguru.logger.info(
            f"self.batch_clean_text_end_time()输出文件操作完成，耗时：{output_file_end_time - specific_operation_end_time:.6f}")
        loguru.logger.info("*" * 100)


class Files:
    file_list = [
        {
            "input_file_path": r"D:\pyProject\hzcx\db\utils\founder_article_info.csv",
            "output_path": r"D:\pyProject\hzcx\db\utils\founder_article_info_output.csv",
            "mapping_list": {
                "article_id": "id",
                "article_title": "title",
                "en_article_title": "en_title"
            },
            "drop_columns": ["_id"],
            "default_value_list": [{"status": 0}],
            "primary_key": ["id", "source"],
            "columns":  ["id", "title", "en_title", "doi", "authors", "source", "abstracts", "en_abstracts",
                                "keywords", "en_keywords", "lang", "status", "type", "publish_date"],
        },
        {
            "input_file_path": r"D:\pyProject\hzcx\db\utils\founder_author_info.csv",
            "output_path": r"D:\pyProject\hzcx\db\utils\founder_author_info_output.csv",
            "mapping_list": {
                "author_id": "id",
                "author_name": "name",
                "en_author_name": "en_name",
                "about_author": "profile"
            },
            "drop_columns": ["_id"],
            "default_value_list": [{"type": 0}],
            "primary_key": ["id"],

            "columns": ["id",
                               "name",
                               "en_name",
                               "type",
                               "email",
                               "profile",
                               "orc_id",
                               ],
        },
        {
            "input_file_path": r"D:\pyProject\hzcx\db\utils\founder_fund_info.csv",
            "output_path": r"D:\pyProject\hzcx\db\utils\founder_fund_info_output.csv",
            "mapping_list": {
                "fund_id": "id",
                "fund_name": "name",
            },
            "drop_columns": ["_id"],
            "primary_key": ["id"],

            "columns": ["id",
                               "name",
                               "en_name",
                               ],
        },
        {
            "input_file_path": r"D:\pyProject\hzcx\db\utils\founder_institution_info.csv",
            "output_path": r"D:\pyProject\hzcx\db\utils\founder_institution_info_output.csv",
            "mapping_list": {
                "aff_name": "name",
                "en_aff_name": "en_name",
            },
            "drop_columns": ["_id"],
            "primary_key": ["id"],

            "columns": ["id",
                       "institution_id",
                       "name",
                       "en_name",
                       "location",
                               ],
        },
        {
            "input_file_path": r"D:\pyProject\hzcx\db\utils\founder_venue.csv",
            "output_path": r"D:\pyProject\hzcx\db\utils\founder_venue_output.csv",
            "mapping_list": {

                "journal_id": "id",
                "journal_title": "name",
            },
            "drop_columns": ["_id"],
            "primary_key": ["id"],

            "columns": ["id",
                        "name",
                        "en_name",
                        "type",
                        ],
        },
        {
            "input_file_path": r"D:\pyProject\hzcx\db\utils\founder_publish.csv",
            "output_path": r"D:\pyProject\hzcx\db\utils\founder_publish_output.csv",
            "mapping_list": {
                "author_id": "id",

            },
            "drop_columns": ["_id"],
            "primary_key": ["id", "achievement_id"],
            "columns": ["id",
                        "achievement_id",
                        "order_of_authors",
                        "corresponding_author",
                        ],
        },
        {
            "input_file_path": r"D:\pyProject\hzcx\db\utils\founder_published_in.csv",
            "output_path": r"D:\pyProject\hzcx\db\utils\founder_published_in_output.csv",
            "mapping_list": {
                "achievement_id": "id",

            },
            "drop_columns": ["_id"],
            "primary_key": ["id", "journal_id"],

            "columns": ["id",
                        "journal_id",
                        "year",
                        "volume",
                        "period",
                        "page_range",
                        ],
        },
        {
            "input_file_path": r"D:\pyProject\hzcx\db\utils\founder_claimed_by.csv",
            "output_path": r"D:\pyProject\hzcx\db\utils\founder_claimed_by_output.csv",
            "mapping_list": {
                "achievement_id": "id",
            },
            "drop_columns": ["_id"],
            "primary_key": ["id", "institution_id"],

            "columns": ["id",
                        "institution_id",
                        ],
        },
        {
            "input_file_path": r"D:\pyProject\hzcx\db\utils\founder_supported_by.csv",
            "output_path": r"D:\pyProject\hzcx\db\utils\founder_supported_by_output.csv",
            "mapping_list": {
                "achievement_id": "id",

            },
            "drop_columns": ["_id"],
            "primary_key": ["id", "fund_id"],

            "columns": ["id",
                        "fund_id",

                        ],
        },
        {
            "input_file_path": r"D:\pyProject\hzcx\db\utils\founder_work_at.csv",
            "output_path": r"D:\pyProject\hzcx\db\utils\founder_work_at_output.csv",
            "mapping_list": {
                "author_id":"id",
            },
            "drop_columns": ["_id"],
            "primary_key": ["id", "institution_id"],

            "columns": ["id",
                        "institution_id",
                        ],
        },
    ]


def main():
    for file_info in Files.file_list:
        achievement = CSVFile(file_path=file_info["input_file_path"])

        achievement.columns = file_info["columns"]
        achievement.drop_columns = file_info["drop_columns"]
        achievement.mapping_list = file_info["mapping_list"]
        achievement.output_path = file_info["output_path"]

        if file_info.get("default_value_list"):
            achievement_strategy = CompositeOperation([
                DefaultValueOperation(file_info["default_value_list"]),
                DuplicateOperation(primary_key=file_info["primary_key"]),
            ])
        else:
            achievement_strategy = CompositeOperation([
                DuplicateOperation(primary_key=file_info["primary_key"]),
            ])
        achievement_processor = OperationStrategy(achievement, achievement_strategy)
        achievement_processor.process()
        achievement_processor.output_file()


if __name__ == '__main__':
    main()
