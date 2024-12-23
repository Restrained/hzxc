#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/6 15:30
# @Author  : AllenWan
# @File    : notice.py
# @Desc    ：
from abc import ABC
from typing import Union, List


class Observer(ABC):
    """
        接收对象基类
    这个基类的作用是抽象出接收对象的一些共同特征，具体实现类比如企微、钉钉、飞书等

    """
    def __init__(self, url):
        self.url = url

    def update(self):
        pass

class Notice(ABC):
    """
    消息通知基类
    """
    def send(self, msg: Union[str, List[str]]):
        return msg