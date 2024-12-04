#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/30 15:29
# @Author  : AllenWan
# @File    : operation.py
# @Desc    ：该代码使用结合模板方法模式与策略模式
import html
import re
import time
import uuid
from abc import ABC, abstractmethod
from inspect import stack
from typing import Union, Any, TypeVar, List, Literal

import loguru
import pandas as pd
from bs4 import BeautifulSoup
from pandas import DataFrame
from events.data_conversion.file import CSVFile

T = TypeVar('T', bound=DataFrame)

class CSVProcessor(ABC):
    """
    文件操作抽象方法集合
    """
    def __init__(self, csv_file: CSVFile):
        """
        对象构造方法
        :param csv_file: CSVFile
        """
        self.csv_file = csv_file
        self.data: Union[T, None] = self.csv_file.data
        self._id = None


    def rename_columns(self):
        """
        根据映射关系重命名某些列
        :return:
        """
        self.data = self.data.rename(columns=self.csv_file.mapping_list)

    def filter_columns(self, columns:List[str]):
        """
        根据输入的列名列表过滤出特定的列
        :return:
        """
        self.data = self.data[columns]

    def fill_na(self):
        """
        将DataFrame对象所有na单元格替换为''
        :return:
        """
        self.data = self.data.fillna('')

    @staticmethod
    def clean_text(text:str) -> str:
        """
        针对文本中存在的html标签，通过获取text属性的方式取出，另外去掉相关转义符，多个连续空格则替换为一个空格
        :param text: str
        :return: str
        """
        try:
            # 去除多余的换行符和空白字符
            text = text.replace('\r\n', ' ').replace('\n', ' ')  # 替换换行符
            text = re.sub(r'\s+', ' ', text)  # 将多个空格替换为一个空格

            html_text = html.unescape(text)
            soup = BeautifulSoup(html_text, 'html.parser')
            return soup.get_text(separator=' ', strip=True)
        except Exception as e:
            raise ValueError(e)

    def batch_clean_text(self):
        """
        批量输入列值进行html标签清洗
        :return:
        """
        for column_name in self.csv_file.clean_columns_list:
            self.data[column_name] = self.data[column_name].apply(self.clean_text)



    def drop_columns(self, columns):
        """
        删除指定列
        :param columns:
        :return:
        """
        if self.data is not None:
            self.data.drop(columns=columns, inplace=True)


    def add_random_id(self, column_name: str = "id") -> None:
        """
        为每一行生成唯一 ID，并添加到指定列
        :param column_name: 新列的名称，默认为 'id'
        """
        self.data[column_name] = self.data.apply(
            lambda _: str(uuid.uuid4()).replace("-", "")[:24], axis=1
        )

    def output_file(self, index=False, encoding="utf-8-sig"):
        """
        输出一个处理后的文件到指定路径
        :param index:
        :param encoding:
        :return:
        """
        self.data.to_csv(self.csv_file.output_path, index=index, encoding=encoding)


class SpecificOperationStrategy(ABC):
    @abstractmethod
    def exec(self, data:DataFrame):
        """
        策略接口，传参标准为一个DataFrame对象
        :param data:  DataFrame
        :return:
        """
        pass

class DuplicateOperation(SpecificOperationStrategy):
    """
    去重策略，根据主键操作，通过exec方法统一为鸭子类型
    """
    def __init__(self, primary_key: [List[str], str]):
        self.primary_key =primary_key

    def exec(self, data: DataFrame) -> DataFrame:
        data = data.drop_duplicates(subset=self.primary_key, keep='first')
        return data

class CnNameSplitOperation(SpecificOperationStrategy):
    def __init__(self, column_name: str, split_rule: str):
        self.column_name = column_name
        self.split_rule = split_rule

    def exec(self, data: DataFrame) -> DataFrame:
        def is_two_chinese(value: str) -> bool:
            # 提取所有中文字符
            chinese_chars = re.findall(r'[\u4e00-\u9fa5]', value)

            # 判断提取的中文字符数是否为 2
            return len(chinese_chars) == 2

        def single_join(single_list: list):
            nonlocal result
            result = []
            i = 0
            while i < len(single_list) - 1:
                # 如果当前元素和下一个元素的第一个元素（数字）是连续的
                if single_list[i][0] + 1 == single_list[i + 1][0]:
                    # 合并这两个元素
                    result.append((single_list[i][0], single_list[i][1] + single_list[i + 1][1]))
                    # 跳过下一个元素
                    i += 2
                else:
                    # 如果不连续，跳过当前元素
                    i += 1
            return result

        def remove_html_tags(text: str) -> str:
            # 使用正则表达式去除所有 HTML 标签
            return re.sub(r'<.*?>.*?</.*?>', '', text)

        def rejoin(split_list: List[str]):
            nonlocal result
            single_list = []
            for index, item in enumerate(split_list):
                if len(item) == 1:
                    single_list.append([index, item])
            result = single_join(single_list)
            single_dict = dict(result)
            # 替换操作
            for i, val in enumerate(split_list):
                if i in single_dict:  # 注意索引从1开始
                    split_list[i] = single_dict[i]
            filter_list = [item for item in split_list if len(item) != 1]
            return filter_list

        def process_cell(value:str) -> List:
            # 判断是否是纯英文字符
            is_en_name = re.match(r'^[a-zA-Z\s]+$', value)

            # 删除文中标签
            value = remove_html_tags(value)
            # 多个连续空格转换成一个
            value = re.sub(r'\s+', ' ', value)
            # 多种分隔符统一为,号
            value = re.sub(r'；;，', ',', value)

            if ',' in value:
                if not is_en_name:
                    value = value.replace(' ', '')
                return value.split(',')
            elif is_en_name:
                return [value]
            elif ' ' in value:
                if is_two_chinese(value):
                    return [value.replace(' ', '')]
                else:
                    return rejoin(value.split(' '))
            else:
                return [value]

        process_data = data[self.column_name].fillna('').apply(lambda x: process_cell(str(x)))
        stacked_reset = process_data.apply(pd.Series).stack().reset_index(level=1, drop=True).to_frame("cn_name")
        # 删除原表中的 `cn_name` 列
        data = data.drop(columns=['cn_name'])
        result = stacked_reset.merge(data, left_index=True, right_index=True, how='left')
        return result

class EnNameSplitOperation(SpecificOperationStrategy):
    def __init__(self, column_name: str, split_rule: str):
        self.column_name = column_name
        self.split_rule = split_rule



    def exec(self, data: DataFrame) -> DataFrame:
        def remove_html_entities(text: str) -> str:
            # 匹配 HTML 实体编码
            pattern = r'&[a-zA-Z0-9#]+;'
            return re.sub(pattern, '', text)

        def process_cell(value: str) -> List:

            value = remove_html_entities(value)
            # 删除文中标签
            value = re.sub(r'and', ' ', value)
            # 多个连续空格转换成一个
            value = re.sub(r'\s+', ' ', value)
            # 多种分隔符统一为,号
            value = re.sub(r'；;，', ',', value)

            return value.split(',')

        process_data = data[self.column_name].fillna('').apply(lambda x: process_cell(str(x)))
        stacked_reset = process_data.apply(pd.Series).stack().reset_index(level=1, drop=True).to_frame("en_name")
        # 删除原表中的 `en_name` 列
        data = data.drop(columns=['en_name'])
        result = stacked_reset.merge(data, left_index=True, right_index=True, how='left')
        return result


class DefaultValueOperation(SpecificOperationStrategy):
    """
    设置默认值策略
    """
    def __init__(self, default_list: list[dict]):
        self.default_list = default_list

    def exec(self, data: DataFrame) -> DataFrame:
        for item in self.default_list:
            for k, v in item.items():
                data[k] = v
        return data

class StrReplaceOperation(SpecificOperationStrategy):
    """
    字符串替换策略, 通过构造方法控制传参，保持exec标准化
    """
    def __init__(self, replace_info_list: list[dict[str, tuple[str, str]]]):
        """
        替换信息的格式，举例：[{"eng": (0,1)}]
        :param replace_info_list:
        """
        self.replace_info_list = replace_info_list

    def exec(self, data: DataFrame) -> DataFrame:
        for list_item in self.replace_info_list:
            for k,v in list_item.items():
                data[k] = data[k].replace(v[0], v[1])
            return data

class ReSubstringStrStrategy(SpecificOperationStrategy):
    def __init__(self, column_name:str, pattern: re.Pattern):
        self.column_name = column_name
        self.pattern = pattern


    def exec(self, data: DataFrame) -> DataFrame:
        data[self.column_name] = data[self.column_name].fillna('').apply(lambda x: re.sub(self.pattern, '', x.strip()) if x else '')
        return data

class TableJoinOperation(SpecificOperationStrategy):
    def __init__(self, join_df: DataFrame,  left_columns: Union[str, List], right_columns: Union[str, List], how:Literal["left", "right", "inner", "outer", "cross"]='left') -> None:
        self.join_df = join_df
        self.left_columns = left_columns
        self.right_columns = right_columns
        self.how = how

    def exec(self, data: DataFrame) -> DataFrame:
        merge = pd.merge(data, self.join_df, left_on=self.left_columns, right_on=self.right_columns, how=self.how)
        merge = self.resolve_column_conflicts(merge, data)
        return merge

    @staticmethod
    def resolve_column_conflicts(df:DataFrame, left_df:DataFrame) -> DataFrame:
        for column in left_df.columns:
            if f"{column}_y" in df.columns:
                df.drop(columns=f"{column}_y", axis=1, inplace=True, errors='ignore')
                df.rename(columns={f"{column}_x":column}, inplace=True)
        return df

class CompositeOperation(SpecificOperationStrategy):
    """
    策略集合，所有具体策略统一交由该对象进行执行
    """
    def __init__(self, strategies: list[SpecificOperationStrategy]):
        self.strategies = strategies

    def exec(self, data: DataFrame) -> DataFrame:
        for strategy in self.strategies:
            data = strategy.exec(data)
        return data

class KeywordSplitOperation(SpecificOperationStrategy):
    """
    策略集合，所有具体策略统一交由该对象进行执行
    """
    def exec(self, data: DataFrame) -> DataFrame:
        data["keywords"].apply(lambda x: str([item.strip() for item in re.split(r"[,，\s]+", x) if item.strip()]) if pd.notna(x) else '' )
        data["en_keywords"].apply(lambda x: str([item.strip() for item in re.split(r"[,，]+", x) if item.strip()]) if pd.notna(x) else '' )
        return data












if __name__ == '__main__':
    # achievement_main()
    ps = '张  磊'
    print(ord(ps[1]))