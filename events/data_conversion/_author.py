#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/3 11:45
# @Author  : AllenWan
# @File    : _author.py
# @Desc    ：
from events.data_conversion.file import CSVFile
from events.data_conversion.operation import SpecificOperationStrategy, CSVProcessor, CompositeOperation, \
    DuplicateOperation


class AuthorWithStrategy(CSVProcessor):
    """
    继承通用处理流程
    """
    def __init__(self, csv: CSVFile, strategy: SpecificOperationStrategy):
        """
        初始化主要传入两部分：
        1. 文件对象的具体实例
        2. 文件一些特定的操作集合
        :param csv:
        :param strategy:
        """
        super().__init__(csv)
        self.strategy = strategy

    def specific_operation(self):
        """
        定义该文件特定操作执行的入口
        :return:
        """
        self.strategy.exec(self.data)

    def process(self):
        """
        文件通用操作的接口
        :return:
        """

        # 1.过滤出特定列
        self.filter_columns(self.csv_file.columns)
        # 2.通过策略集合先进行去重
        self.specific_operation()
        # 3.生成id
        self.add_random_id(column_name="authorId")




def main():
    # 特定文件对象实例
    author_file_path = r'C:\Users\PY-01\Documents\local\renHeHuiZhi\author_info.csv'
    author = CSVFile(file_path=author_file_path)

    # 补充额外属性
    # 主键
    author.primary_key = ["cn_name", "en_name", "mail"]
    # 保留列
    author.columns = [
        "cn_name", "en_name", "mail", "cn_about_author"
    ]

    # 定义要执行的特定策略集合
    author_strategy = CompositeOperation([
        DuplicateOperation(author.primary_key)
    ])
