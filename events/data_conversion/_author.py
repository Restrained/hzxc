#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/3 11:45
# @Author  : AllenWan
# @File    : _author.py
# @Desc    ：
import re

from csv_data.file import CSVFile
from csv_data.operation import SpecificOperationStrategy, CSVProcessor, CompositeOperation, \
     ReSubstringStrStrategy, CnNameSplitOperation, EnNameSplitOperation


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
        self.data = self.strategy.exec(self.data)

    def process(self):
        """
        文件通用操作的接口
        :return:
        """

        # 1.过滤出特定列
        # self.filter_columns(self.csv_file.columns)
        # 2.通过策略集合先进行去重
        self.specific_operation()
        # 3.生成id
        # self.add_random_id(column_name="authorId")




def main():
    # 特定文件对象实例
    author_file_path = r"D:\output\csv\author_info_clean.csv"
    author_output_file_path = r"D:\output\csv\author_info_clean_test.csv"
    author = CSVFile(file_path=author_file_path)

    # 补充额外属性
    # 主键
    author.output_path = author_output_file_path

    author_cn_name_sub_pattern =  re.compile(r'[^a-zA-Z\u4e00-\u9fa5 ,•·<>\d/.()；;，]') # 清除名字中其他无关字符
    author_en_name_sub_pattern =  re.compile(r'[^a-zA-Z ,•·<>\d/.()；;，-]') # 清除名字中其他无关字符
    # 定义要执行的特定策略集合
    author_strategy = CompositeOperation([
        ReSubstringStrStrategy(column_name="cn_name", pattern=author_cn_name_sub_pattern),
        ReSubstringStrStrategy(column_name="en_name", pattern=author_en_name_sub_pattern),
        CnNameSplitOperation(column_name="cn_name", split_rule=' '),
        EnNameSplitOperation(column_name="en_name", split_rule=',')
    ])

    author_processor = AuthorWithStrategy(author, author_strategy)
    author_processor.process()
    author_processor.output_file()


if __name__ == '__main__':
    main()