#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@Project ：hzcx
@File    ：search.py
@IDE     ：PyCharm
@Author  ：Allen.Wan
@Date    ：2024/11/12 下午13:43
@explain : 文件说明
"""
import os

from bricks import Request, const, Response
from bricks.db.mongo import Mongo
from bricks.db.redis_ import Redis
from bricks.downloader import requests_
from bricks.lib.queues import RedisQueue
from bricks.plugins import scripts
from bricks.plugins.make_seeds import by_redis, by_csv, by_mongo
from bricks.plugins.storage import to_csv, to_mongo
from bricks.spider import air, template
from bricks.core.context import Context
from bricks.spider.template import Config
from bs4 import BeautifulSoup
from bricks.utils.fake import user_agent

path = r"D:\projectOutput\baidu\html"


class Search(template.Spider):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 需自行配置 redis
        self.redis = Redis()
        self.mongo = Mongo(
            database='baidu'
        )

    @property
    def config(self) -> Config:
        return Config(
            init=[
                template.Init(

                    func=by_csv,
                    kwargs={
                        'path': r"D:\pyProject\hzcx\baidu\input\csv\search_seeds.csv",
                        'query': 'select nameOfTheJournal as searchWord, ISSN, libraryID  from <TABLE>',
                        'batch_size': 5000

                    },
                    layout=template.Layout(
                        factory={"searchWord": lambda x: x + " 期刊"}
                    )

                )
            ],
            download=[
                template.Download(
                    url="https://www.baidu.com/ssid=5198c8fdc4beeacf4cac/from=844b/s",
                    params={
                        "word": "{searchWord}",
                        "ts": "0",
                        "t_kt": "0",
                        "ie": "utf-8",
                        "rsv_iqid": "10697887281610526680",
                        "rsv_t": "caddfVW6bd1sCWFtaPri2WSosMD5YpsHzsxQKXxrVFpYm6kqr2g%2F1CGSbQ",
                        "sa": "ih_1_d",
                        "ms": "1",
                        "rsv_pq": "10697887281610526680",
                        "rsv_sug4": "1731383578896",
                        "tj": "1",
                        "ss": "010",
                        "rqid": "10697887281610526680",
                        "rfrom": "1040158w",
                        "rchannel": "1040295a"
                    },
                    timeout=15,
                    headers={
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                        'Cache-Control': 'max-age=0',
                        'Connection': 'keep-alive',
                        # 'Cookie': 'wpr=0; BIDUPSID=76DC48351D98219BD034C0F2C4404CC2; PSTM=1731285498; BAIDUID=76DC48351D98219B3113ADB98522BF3D:FG=1; BDUSS=nFkRDExbm9YSU5HTWMyTTBXektFMjVyTVZGUXhaNHBNZTh6Z1p1RFpFb2ktVmhuRVFBQUFBJCQAAAAAAAAAAAEAAABRmEysyP3EvurPAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACJsMWcibDFnc; BDUSS_BFESS=nFkRDExbm9YSU5HTWMyTTBXektFMjVyTVZGUXhaNHBNZTh6Z1p1RFpFb2ktVmhuRVFBQUFBJCQAAAAAAAAAAAEAAABRmEysyP3EvurPAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACJsMWcibDFnc; H_PS_PSSID=61027_61053_61071_61099_60851_61130_61127_61114_61141_61116; BDORZ=B490B5EBF6F3CD402E515D22BCDA1598; BA_HECTOR=048h8h8l212505a481a1akah2jgc061jj632l1v; BAIDUID_BFESS=76DC48351D98219B3113ADB98522BF3D:FG=1; ZFY=dcEe:ASSydzB:B46UuPW1xsoFysLW0Rx5uClNU7kN4jN0:C; delPer=0; BDRCVFR[feWj1Vr5u3D]=mk3SLVN4HKm; BDPASSGATE=IlPT2AEptyoA_yiU4SO43lIN8eDEUsCD34OtVnti3ECGh67BmhH74rJlVV8pUWytB50l-YyfmqxPpjrFV6xjg0N_gRsLkG67jiyqtM3d7Mn2L0tesKMeIKbcYiY4trzIfRhL-3MEF3VEVFoKhAP2pvwQehTT7xBIhR4I7FKyh1rs2CCD17n_xHyQ19tKLHvfKtiFwvrXnHlKXyafA4HiSC8yhCIiCHcrzcyOatMlDA_DuX5XGurSRvAa3GjQHWl3EQy61eaQkL8TCkocwpdYSkUtdEiV5tD6Mk960Bz7j_haQqjhMKb4OIe3_1o2nNnbLQdWKQ3zmtkGDV9Y95ssJo2rRa0FIWzGKpwKLMigiBjZCXwVqlOEIwDLvIA5RrXoFOIXZApDSVBaiyefnNWmpSr2HwPgi0FvM0wGCXOYyZl46pN3Gm4K6Hqzc7Jv-p_dT9OKG6zd3cHlT7VFquKyJaXaVKr82VEbqimagCXNek8MDwzdFdB7kzgCwnV7TWiB9QeHNTCqX4vt5MF9srXqvszDu9SUqzaZwEipLc374wB5YUI1jR6rCmjJzGy_eXx_O4Gduo4wWD0fm5FDstyIjyZnlPA01NJKO0L4BR-b7cSIkWwV0yYto_Sz3wNf1-zkjuBUIcT4T9af2l0VZnxmgrtA2V4vIJTzvtLgQDVmEj_VYzzaC_RxwF-2eHIWORf1OCy-odUQXi4tosQKOcaip7diFOEjEi25vXzj4mf2CRnv70JGkl58th1nOHGsZvTU6Y55iLgqUyCuyrbWCwf5gzE25nrebNB4ALaMWF858FKyIBEq-nnzEFyXxK; H_WISE_SIDS=110085_307086_621397_610631_1992049_620483_623536_607027_625597_625576_623878_623874_625784_625965_625971_626068_626121_1991790_626436_626376_626725_624023_626775_626905_626942_626986_626930_624663_627135_627169_626981_627238_625020_627213_627286_625250_627455_1991947_625012_627789_627702_627491_627496_627899_614025_626243_612952_628175_628198_625432_623990_627934_628154_628364_628308_628257_628314_628437_628240_628347_628463_622875_628539_628546_628541_626320_623211_628632_628294_628558_628761_628781_628782_628785_628849_628764_627319_628874_628854_628562_628904_628902_628899_628886_628887_628897_628967_629000_629015_629021_628943_627484_628927_626070; H_WISE_SIDS_BFESS=110085_307086_621397_610631_1992049_620483_623536_607027_625597_625576_623878_623874_625784_625965_625971_626068_626121_1991790_626436_626376_626725_624023_626775_626905_626942_626986_626930_624663_627135_627169_626981_627238_625020_627213_627286_625250_627455_1991947_625012_627789_627702_627491_627496_627899_614025_626243_612952_628175_628198_625432_623990_627934_628154_628364_628308_628257_628314_628437_628240_628347_628463_622875_628539_628546_628541_626320_623211_628632_628294_628558_628761_628781_628782_628785_628849_628764_627319_628874_628854_628562_628904_628902_628899_628886_628887_628897_628967_629000_629015_629021_628943_627484_628927_626070; SE_LAUNCH=5%3A1731401434; MSA_WH=1897_954; MSA_PBT=96; MSA_ZOOM=1000; MSA_PHY_WH=1080_1920; POLYFILL=0; COOKIE_SESSION=32_0_0_1_0_t1_1_1_0_0_0_0_1_1731401465%7C2%230_0_0_0_0_0_0_0_1731401433%7C1; FC_MODEL=0_0_0_0_0_0_0_0_0_0_0_0_1_1_1_0_0_0_1731401465%7C1%230_0_0_1_1_0_1731401465%7C1%230_0_0_0_0_0_0_0; BDSVRTM=11; kleck=700dda57fbfe7f76a0a3de30475b6017; __bsi=9504539959178242415_00_18_R_R_6_0303_c02f_Y; H_PS_PSSID=61027_61053_61071_61099_60851_61130_61127_61114_61141_61116; H_WISE_SIDS=110085_307086_621397_610631_1992049_620483_623536_607027_625597_625576_623878_623874_625784_625965_625971_626068_626121_1991790_626436_626376_626725_624023_626775_626905_626942_626986_626930_624663_627135_627169_626981_627238_625020_627213_627286_625250_627455_1991947_625012_627789_627702_627491_627496_627899_614025_626243_612952_628175_628198_625432_623990_627934_628154_628364_628308_628257_628314_628437_628240_628347_628463_622875_628539_628546_628541_626320_623211_628632_628294_628558_628761_628781_628782_628785_628849_628764_627319_628874_628854_628562_628904_628902_628899_628886_628887_628897_628967_629000_629015_629021_628943_627484_628927_626070; PSINO=6; delPer=0',
                        'Referer': 'https://wappass.baidu.com/',
                        'Sec-Fetch-Dest': 'document',
                        'Sec-Fetch-Mode': 'navigate',
                        'Sec-Fetch-Site': 'same-site',
                        'Sec-Fetch-User': '?1',
                        'Upgrade-Insecure-Requests': '1',
                        'User-Agent': user_agent.mobile(),
                        'sec-ch-ua': '"Chromium";v="130", "Microsoft Edge";v="130", "Not?A_Brand";v="99"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"Windows"'
                    }

                )
            ],
            parse=[
                template.Parse(
                    func=self._parse,

                )
            ],
            pipeline=[
                template.Pipeline(
                    func=to_mongo,
                    kwargs={
                        "path": "search_result_v2",
                        "conn": self.mongo,

                    },
                    success=True

                )
            ],
            events={

                const.AFTER_REQUEST: [
                    template.Task(
                        func=scripts.is_success,
                        kwargs={
                            "match": [
                                "'c-result result' in context.response.text"
                            ]
                        }
                    )
                ],

                const.BEFORE_PIPELINE: [
                    template.Task(
                        func=self.storage_source_code,

                    )
                ]

            }
        )

    @staticmethod
    def init_seeds():
        return [

            {"skill_id": "3", "page": 0}

        ]

    @staticmethod
    def parse_html_from_file(html_content):

        # 创建 BeautifulSoup 对象
        soup = BeautifulSoup(html_content, 'html.parser')

        # 查找所有 class 为 "c-result result" 的 div
        divs = soup.find_all('div', class_='c-result result')

        # 初始化一个列表来存储符合条件的文本内容
        result_texts = []

        # 遍历找到的 div 元素
        for div in divs:
            # 检查是否包含 <span>官方</span>
            if div.find('span', string='官方'):
                # 在包含“官方”的 div 中查找 data-module 为 "lgsoe" 的子 div
                target_div = div.find('div', {'data-module': 'lgsoe'})

                if target_div:
                    url_div = target_div.find('div', class_='single-text')
                    if url_div:
                        result_texts.append(url_div.get_text(strip=True))
        # 如果没有找到符合条件的 URL，返回 None
        return result_texts if result_texts else None

    def _parse(self, context: template.Context):
        result = []
        seeds = context.seeds
        response = context.response
        url_list = self.parse_html_from_file(response.text)
        if url_list:
            official_website = url_list[0]
        else:
            official_website = None
        result.append({
            'nameOfTheJournal': seeds['searchWord'],
            'ISSN': seeds["ISSN"],
            'libraryID': seeds["libraryID"],
            "officialWebsite": official_website,
        })

        return result

    def storage_source_code(self, context: template.Context):
        seeds = context.seeds
        file_name = f"{seeds['searchWord']}.txt"
        save_path = os.path.join(path, file_name)
        f = open(save_path, mode='w', encoding='utf-8')
        f.write(context.response.text)
        f.close()
        context.success()


if __name__ == '__main__':
    # proxy = {
    #     'ref': "bricks.lib.proxies.RedisProxy",
    #     'key': 'proxy_set',
    #
    # }

    proxy = {
        'ref': "bricks.lib.proxies.RedisProxy",
        'key': 'proxy_21',
        'options': {'host': '1.14.193.46', 'password': 'sandalwood_SWA_2021-06-01!'},
        "threshold": 10,
        # "scheme": "socks5"

        # 'ref': "bricks.lib.proxies.CustomProxy",
        # # 'key': "115.223.31.91:34428",
        # 'key': "127.0.0.1:7897",
        # # "threshold": 10,
        # # "scheme": "socks5"
    }
    #
    # spider = Comment(
    #     concurrency=50,
    #     downloader=requests_.Downloader(),
    #     task_queue=RedisQueue(),
    #     # proxy=proxy,
    # )
    #

    spider = Search(
        concurrency=100,
        downloader=requests_.Downloader(),
        **{"init.queue.size": 5000000},
        task_queue=RedisQueue(),
        proxy=proxy
    )

    spider.run(task_name='spider')
