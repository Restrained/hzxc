#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/11/23 09:21
# @Author  : AllenWan
# @File    : test.py
import requests

url = "http://www.ysxb.ac.cn/data/article/export-pdf"

payload = 'id=d6e697d3-e378-4ea6-ac5c-0c971b0ce8e1'
headers = {
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
  'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
  'Cache-Control': 'max-age=0',
  'Connection': 'keep-alive',
  'Content-Type': 'application/x-www-form-urlencoded',
  'Cookie': '_sowise_user_sessionid_=6c482ae9-eb5e-4055-8e36-0d378309c1ac; JSESSIONID=1E01F794D5EC20CD05A28C55EF21638D',
  'Origin': 'http://www.ysxb.ac.cn',
  'Referer': 'http://www.ysxb.ac.cn//article/doi/10.18654/1000-0569/2021.07.12',
  'Upgrade-Insecure-Requests': '1',
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0'
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)
