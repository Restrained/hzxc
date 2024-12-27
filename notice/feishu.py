#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/6 14:55
# @Author  : AllenWan
# @File    : feishu.py
# @Desc    ：

"""
飞书消息推送库
"""
import json
import requests


class FeiShu(object):
    robots = {
        "spider_man": "https://open.feishu.cn/open-apis/bot/v2/hook/affb4538-a02f-4bc6-be1a-db9052009542"
    }

    msg_type = ''

    msg_levels = {
        'critical': '重要通知',
        'error': '错误',
        'warning': '警告',
        'info': '通知',
        'debug': '测试通知'
    }

    def send(self, msgs=None, msg:str = "", robot='spider_man', level='info', title=''):
        """
        发送消息
        """

        msgs = msgs or msg
        url = self.robots.get(robot)
        if not url:
            return False, '找不到此机器人'

        msg_level = level

        msg = {
            'a': '',
            'at': '',
            'text': msgs,
            'img': ''
        }
        msgs = [msg]


        title = title or robot + '发出 ' + self.msg_levels[msg_level]

        contents = []
        for row in msgs:
            if not isinstance(row, list):
                row = [row]

            for i, msg in enumerate([*row]):
                content = {}
                for tag in msg.keys():
                    if msg[tag]:
                        if tag == 'a':
                            content['tag'] = tag
                            content['href'] = msg[tag]
                            content['text'] = msg[tag]
                        elif tag == 'at':
                            content['tag'] = tag
                            user = msg[tag].split('=')
                            content[user[0]] = user[1]
                        else:
                            content['tag'] = tag
                            content[tag] = msg[tag]
                row[i] = content

            contents.append(row)

        # 添加消息头
        contents[0].insert(0, {'tag': 'text', 'text': self.msg_levels[msg_level] + ": "})

        data = {
            "msg_type": "post",
            "content": {
                "post": {
                    "zh_cn": {
                        "title": title,
                        "content": contents
                    }
                }
            }
        }
        msg = json.dumps(data, ensure_ascii=True).encode("utf-8")
        header = {
            "Content-type": "application/json",
            "charset": "utf-8"
        }
        res = requests.post(url, data=msg, headers=header)
        res = res.json()
        return int(res.get('StatusCode')) == 0, res.get('StatusMessage')



if __name__ == '__main__':
    notice_instance = FeiShu()
    result= notice_instance.send(msgs="爬取完成", title="测试")
    print(result)