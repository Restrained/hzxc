#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/29 16:45
# @Author  : AllenWan
# @File    : file.py
from typing import List, Union, Dict

import pandas as pd
from pandas.core.interchange.dataframe_protocol import DataFrame


class CSVFile:
    """
    基础的所有导出文件的父类。

    公有属性汇总：
    1.文件路径
    2.输出列list
    3.主键集合

    导出文件需要有的所有规则：
    1. 文件路径
    2. 需要提取出来的字段
    3. 行数据根据指定主键去重
    4. 根据主键生成id
    3. 列名映射：如果字段需要重命名，需要的对应关系
    4. 字段清洗：部分字段需要清洗，对应的清洗规则
    5. 数据拆分：部分行数据，可能通过清洗，需要转换为多行，比如针对名字粘连到一起的情况
    6. 多表关联：多个文件可能需要关联来建立图数据库的关系表
    7. 数据导出：通过to_csv还是直接写入文件

    文件处理流程梳理：
    实体表
    1. 读取数据：这个没啥好纠结的，都是通过pd.read_csv()，返回的是DateFrame对象
    2. 判断是否存在列名映射表，有则进行列名转换
    3. 抽取字段，有些列不是需要的字段，此时需要过滤掉
    3. 字段清洗，不同字段针对不同情况，分别调用清洗函数 Series apply?
    4. 判断是否需要关联，如果需要，新建Table对象,一般通过左关联
    5. 判断此时字段是否符合输出要求，一般因为关联的原因可能此处再次需要数据抽取


    按照输出文件类型区分
    1.应当分为实体表和关系表。关系表字段及结构相对较简单。
    """
    def __init__(self, file_path: str):
        self._output_columns = None
        self._default_value_list = None
        self._replace_column_info = None
        self._output_path = None
        self._clean_columns_list = None
        self._mapping_list = None
        self._primary_key = None
        self._columns = None
        self._drop_columns = None
        self.file_path = file_path
        self.data: DataFrame = pd.read_csv(file_path)

    @property
    def columns(self):
        """
        这个属性是从文件中只取列表中存在的列
        :return:
        """

        return self._columns

    @columns.setter
    def columns(self, value: List[str]):
        if not isinstance(value, list):
            raise TypeError("列名集合必须是要给列表")
        self._columns = value

    @property
    def primary_key(self):
        return self._primary_key

    @primary_key.setter
    def primary_key(self, value: Union[str, List[str]]):
        if not value:
            raise ValueError("table must have a primary key")
        if not(isinstance(value, str) or isinstance(value, list)):
            raise TypeError("parameter type must be str or list")
        self._primary_key = value

    @property
    def mapping_list(self):
        return self._mapping_list

    @mapping_list.setter
    def mapping_list(self, value: Dict):
        if not isinstance(value, dict):
            raise TypeError("parameter type must be dict")
        self._mapping_list = value

    @property
    def clean_columns_list(self):
        return self._clean_columns_list

    @clean_columns_list.setter
    def clean_columns_list(self, value: List[str]):
        if not isinstance(value, List):
            raise TypeError("parameter type must be dict")
        self._clean_columns_list = value

    @property
    def output_path(self):
        return self._output_path

    @output_path.setter
    def output_path(self, value: str):
        if not isinstance(value, str):
            raise TypeError("parameter type must be dict")
        self._output_path = value

    @property
    def drop_columns(self) -> list:
        return self._drop_columns

    @drop_columns.setter
    def drop_columns(self, value: str):
        if not isinstance(value, list):
            raise TypeError("parameter drop_columns must be list")
        self._drop_columns = value

    @property
    def replace_column_info(self):
        return self._replace_column_info

    @replace_column_info.setter
    def replace_column_info(self, value: List[Dict[str, set]]):
        if not isinstance(value, List):
            raise TypeError("parameter type must be dict")
        self._replace_column_info = value

    @property
    def default_value_list(self):
        return self._default_value_list

    @default_value_list.setter
    def default_value_list(self, value: List[Dict]):
        if not isinstance(value, List):
            raise TypeError("parameter type must be dict")
        self._default_value_list = value

    @property
    def output_columns(self):
        return self._output_columns

    @output_columns.setter
    def output_columns(self, value: List[str]):
        if not isinstance(value, List):
            raise TypeError("parameter type must be dict")
        self._output_columns = value








