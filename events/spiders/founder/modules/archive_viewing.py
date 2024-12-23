#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/19 16:31
# @Author  : AllenWan
# @File    : archive_viewing.py
# @Desc    ：
#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/12/19 11:57
# @Author  : AllenWan
# @File    : journal_info.py
# @Desc    ：
import base64

from bricks import const
from bricks.core import signals
from bricks.core.signals import Success
from bricks.db.redis_ import Redis
from bricks.lib.queues import RedisQueue
from bricks.plugins import scripts
from bricks.plugins.storage import to_mongo
from bricks.spider import template
from bricks.spider.template import Config, Context
from jinja2.lexer import Failure

from config.config_info import RedisConfig, MongoConfig
from db.mongo import MongoInfo


class ArchiveViewing(template.Spider):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # 种子、存储配置定义
        self.redis = Redis(
            host=RedisConfig.host,
            port=RedisConfig.port,
            password=base64.b64decode(RedisConfig.password).decode("utf-8"),
            database=RedisConfig.database,
        )
        self.mongo = MongoInfo(
            host=MongoConfig.host,
            port=MongoConfig.port,
            username=base64.b64decode(MongoConfig.username).decode("utf-8"),
            password=base64.b64decode(MongoConfig.password).decode("utf-8"),
            database=MongoConfig.database,
            auth_database=MongoConfig.auth_database
        )

    @property
    def config(self) -> Config:
        return Config(
            init=[
                template.Init(
                    func=self._init_seeds,
                    layout=template.Layout(
                        default={
                            "page": 0,
                        }
                    )
                ),

            ],
            download=[
                template.Download(
                    url="https://shzyyzz.shzyyzz.com/rc-pub/front/front-period/getPeriodTreeForPage",
                    params={
                        "siteId": "{site_id}",
                        "isBack": "false",
                        "magType": "1",
                        "page": "{page}",
                        "size": "10",
                        "publicationIndexId": "{publication_index_id}",
                        "timestamps": "1734577187352"
                    },
                    headers={
                        'accept': 'application/json, text/plain, */*',
                        'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
                        'cookie': '_pk_testcookie..undefined=1; _pk_ses.1.d3d5=1; language=zh; _pk_id.1.d3d5=80ec63d4917498b8.1731479262.2.1734577187.1734577060.',
                        'language': 'zh',
                        'priority': 'u=1, i',
                        'sec-ch-ua': '"Microsoft Edge";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"Windows"',
                        'sec-fetch-dest': 'empty',
                        'sec-fetch-mode': 'cors',
                        'sec-fetch-site': 'same-origin',
                        # 'siteid': '398',
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0'
                    },
                    ok={"response.status_code == 500": signals.Pass}
                )
            ],
            parse=[
                template.Parse(
                    func="json",
                    kwargs={
                        "rules": {
                            "csv_data.content": {
                                "periods": {
                                    "archive_id": "id",
                                    "site_id": "siteId",
                                    "journal_name": "publicationName",
                                    "year": "year",
                                    "volume": "volume",
                                    "issue": "issue",
                                    "publication_index_id": "publicationIndexId",
                                    "rc_period_lib_id": "rcPeriodLibId",
                                    "rc_periodid": "rcPeriodId",
                                    "sys_created": "sysCreated",
                                    "sys_last_modified": "sysLastmodified",
                                    "publication_id": "publicationId",
                                }


                            }
                        }

                    },
                    layout=template.Layout(
                        default={"page": "{page}",
                                 },

                    )
                )
            ],
            pipeline=[
                template.Pipeline(
                    func=to_mongo,
                    kwargs={
                        "path": "archive_viewing_founder",
                        "conn": self.mongo
                    },
                    success=False
                )
            ],
            events={
                const.AFTER_REQUEST: [
                    template.Task(
                        func=self.is_success,

                    )
                ],
                const.AFTER_PIPELINE: [
                    template.Task(
                        func=scripts.turn_page,
                        kwargs={
                            "match": [
                                "context.seeds['page'] + 1 < context.response.get('csv_data.totalPages')"
                            ],
                            "success": True
                        }
                    ),
                ]
            }
        )

    def _init_seeds(self):

        iter_data = self.mongo.batch_data(
            collection="journal_info_founder",
            query={},
            projection={
                "site_id": 1,
                "publication_index_id": 1,
            }
        )
        return iter_data

    def is_success(self, context: Context):
        response = context.response
        if response.status_code == 500:
            raise Success
        elif response.get("message") == "操作成功":
            return True

        raise Failure


if __name__ == '__main__':
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

    spider = ArchiveViewing(
        concurrency=10,
        # downloader=requests_.Downloader(),
        **{"init.queue.size": 5000000},
        task_queue=RedisQueue(
            host=RedisConfig.host,
            port=RedisConfig.port,
            password=base64.b64decode(RedisConfig.password).decode("utf-8"),
            database=RedisConfig.database,
        ),
        proxy=proxy
    )
    spider.run(task_name='spider')
    # spider.survey({
    #     "site_id": 0,
    #     "publication_index_id": 2232,
    #     "page": 0
    # })
