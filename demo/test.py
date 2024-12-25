#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/23 09:21
# @Author  : AllenWan
# @File    : main.py
import requests
import json

msg = {
	"msg_type": "post",
	"content": {
	        "post": {
	                "zh_cn": {
	                        "title": "富文本信息",
	                        "content": [
	                                [{ "tag": "text", "text": "富文本信息: "},
	                                 { "tag": "a", "text": "请查看","href": "http://www.example.com/"},
	                                 {"tag": "at","user_id": "ou_18eac8********17ad4f02e8bbbb"}]
	                                 ]
	                         }
	                }
	            }
}

webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/affb4538-a02f-4bc6-be1a-db9052009542"

headers = {
"Content-type": "application/json",
"charset":"utf-8"
}

msg_encode=json.dumps(msg,ensure_ascii=True).encode("utf-8")
reponse=requests.post(url=webhook_url,data=msg_encode,headers=headers)
print(reponse)
