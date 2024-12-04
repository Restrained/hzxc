#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/3 11:42
# @Author  : AllenWan
# @File    : _achievement.py
# @Desc    ：
import time
from typing import Literal

import loguru

from events.data_conversion.file import CSVFile
from events.data_conversion.operation import CSVProcessor, SpecificOperationStrategy, CompositeOperation, \
    DefaultValueOperation, StrReplaceOperation, DuplicateOperation, KeywordSplitOperation, TableJoinOperation


class AchievementWithStrategy(CSVProcessor):
    """
    通过specific_operation调用策略集合，进而在通用逻辑之外拓展其它功能
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

        self.add_random_id()
        add_random_id_end_time = time.time()
        loguru.logger.info(
            f"self.add_random_id_end_time()生成id操作完成，耗时：{add_random_id_end_time - start_time:.6f}")
        loguru.logger.info("*" * 100)

        self.filter_columns(self.csv_file.columns)
        filter_columns_end_time = time.time()
        loguru.logger.info(f"self.filter_columns()过滤字段操作完成，耗时：{filter_columns_end_time - add_random_id_end_time:.6f}")
        loguru.logger.info("*" * 100)

        self.fill_na()
        fill_na_end_time = time.time()
        loguru.logger.info(f"self.fill_na()去除nan操作完成，耗时：{fill_na_end_time - filter_columns_end_time:.6f}")
        loguru.logger.info("*" * 100)

        self.rename_columns()
        rename_columns_end_time = time.time()
        loguru.logger.info(f"self.rename_columns()重命名操作完成，耗时：{rename_columns_end_time - fill_na_end_time:.6f}")
        loguru.logger.info("*" * 100)

        self.batch_clean_text()
        batch_clean_text_end_time = time.time()
        loguru.logger.info(f"self.batch_clean_text_end_time()批量清洗文本操作完成，耗时：{batch_clean_text_end_time - rename_columns_end_time:.6f}")
        loguru.logger.info("*" * 100)


        self.specific_operation()
        specific_operation_end_time = time.time()
        loguru.logger.info(f"self.specific_operation()特定操作集合操作完成，耗时：{specific_operation_end_time - batch_clean_text_end_time:.6f}")
        loguru.logger.info("*" * 100)


        # self.output_file()
        # output_file_end_time = time.time()
        # loguru.logger.info(f"self.batch_clean_text_end_time()输出文件操作完成，耗时：{output_file_end_time - specific_operation_end_time:.6f}")
        # loguru.logger.info("*" * 100)
        # self.drop_columns()

def main():
    achievement_file_path = r"C:\Users\PY-01\Documents\local\renHeHuiZhi\article_info.csv"
    achievement = CSVFile(file_path=achievement_file_path)

    achievement.columns = [
        "id", "article_id", "journal_title", "issn",  # 用于关联
        "title", "en_title", "doi", "source",
        "abstract", "abstract_en", "keywords", "en_keywords",
        "lang", "status", "publish_date", "article_type"
    ]

    achievement.mapping_list = {
        "abstract": "abstracts",
        "abstract_en": "en_abstracts",
        "article_type": "type"
    }

    achievement.primary_key = ["article_id", "journal_title"]
    achievement.default_value_list = [{"status": 0}]
    achievement.clean_columns_list = ["abstracts", "en_abstracts", "title", "en_title", "keywords", "en_keywords"]
    achievement.replace_column_info = [
        {"publish_date": ('时间戳输入错误，请检查后重试！', '1900-01-01')}
    ]

    author_file_path = r"D:\output\csv\author_info_id.csv"
    author = CSVFile(file_path=author_file_path)
    join_df = author.data
    left_columns = ["article_id", "journal_title"]
    right_columns = ["article_id", "journal_title"]
    how:Literal["left", "right", "inner", "outer", "cross"] = "left"

    achievement.output_path=r"D:\output\csv\achievement_info_output.csv"

    achievement_strategy = CompositeOperation([
        StrReplaceOperation(achievement.replace_column_info),
        DefaultValueOperation(achievement.default_value_list),
        DuplicateOperation(primary_key=achievement.primary_key),
        KeywordSplitOperation(),
        TableJoinOperation(join_df, left_columns, right_columns, how)
    ])

    achievement.output_columns = [
        "id", "title", "en_title", "doi", "source", "abstracts", "en_abstracts",
        "keywords", "en_keywords", "lang", "status", "publish_date", "type",
        "authorId", "cn_address", "InstitutionId"
    ]

    achievement_processor = AchievementWithStrategy(achievement, achievement_strategy)
    achievement_processor.process()
    achievement_processor.filter_columns(achievement.output_columns)
    achievement_processor.output_file()


if __name__ == '__main__':
    main()