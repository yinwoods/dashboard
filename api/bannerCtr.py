import json
import asyncio
from dashboard.model.DB import DB
from dashboard.config import DBCONFIG
from dashboard.config import DATABASE
from dashboard.config import IDLOCAL
from dashboard.config import BRLOCAL
from dashboard.handler.basehandler import BaseHandler


class IDBannerCTRHandler(BaseHandler):

    LOCAL = IDLOCAL

    async def getBannerCtr(self):
        # 新闻ID 展示数 点击数 CTR
        res = []
        table = '{}_bannerCtr'.format(self.LOCAL)

        sql = '''
            SELECT
                news_id,
                title,
                start_time,
                end_time,
                impression_cnt,
                click_cnt,
                page_id
            FROM
                {db}.{table}
            ORDER BY
                end_time DESC
        '''.format(
                db=DATABASE,
                table=table
            )

        query_res = DB(**DBCONFIG).query(sql)
        for item in query_res:
            news_id = item['news_id']
            title = item['title']
            start_time = item['start_time']
            end_time = item['end_time']
            impression_cnt = int(item['impression_cnt'])
            click_cnt = int(item['click_cnt'])
            page_id = item['page_id']
            ctr = '{:.3%}'.format(click_cnt / impression_cnt)

            ret = {}
            ret.update({
                'news_id': news_id,
                'title': title,
                'page_id': page_id,
                'start_time': start_time,
                'end_time': end_time,
                'impression_cnt': impression_cnt,
                'click_cnt': click_cnt,
                'ctr': ctr
            })
            res.append(ret)

        return res

    async def getData(self):
        data = []

        tasks = [
            self.getBannerCtr()
        ]

        for task in asyncio.as_completed(tasks):
            data += await task

        return data

    def get(self):

        errid = 0
        errmsg = 'SUCCESS'
        data = None

        event_loop = asyncio.get_event_loop()
        try:
            data = event_loop.run_until_complete(
                    self.getData())
        except Exception as e:
            errid = -3
            errmsg = str(e)

        res = dict()
        res['errid'] = errid
        res['errmsg'] = errmsg
        res['data'] = data
        self.write(json.dumps(res))


class BRBannerCTRHandler(BaseHandler):

    LOCAL = BRLOCAL

    async def getBannerCtr(self):
        # 新闻ID 展示数 点击数 CTR
        res = []
        table = '{}_bannerCtr'.format(self.LOCAL)

        sql = '''
            SELECT
                news_id,
                title,
                start_time,
                end_time,
                impression_cnt,
                click_cnt,
                page_id
            FROM
                {db}.{table}
            ORDER BY
                end_time DESC
        '''.format(
                db=DATABASE,
                table=table
            )

        query_res = DB(**DBCONFIG).query(sql)
        for item in query_res:
            news_id = item['news_id']
            title = item['title']
            start_time = item['start_time']
            end_time = item['end_time']
            impression_cnt = int(item['impression_cnt'])
            click_cnt = int(item['click_cnt'])
            page_id = item['page_id']
            ctr = '{:.3%}'.format(click_cnt / impression_cnt)

            ret = {}
            ret.update({
                'news_id': news_id,
                'title': title,
                'page_id': page_id,
                'start_time': start_time,
                'end_time': end_time,
                'impression_cnt': impression_cnt,
                'click_cnt': click_cnt,
                'ctr': ctr
            })
            res.append(ret)

        return res

    async def getData(self):
        data = []

        tasks = [
            self.getBannerCtr()
        ]

        for task in asyncio.as_completed(tasks):
            data += await task

        return data

    def get(self):

        errid = 0
        errmsg = 'SUCCESS'
        data = None

        event_loop = asyncio.get_event_loop()
        try:
            data = event_loop.run_until_complete(
                    self.getData())
        except Exception as e:
            errid = -3
            errmsg = str(e)

        res = dict()
        res['errid'] = errid
        res['errmsg'] = errmsg
        res['data'] = data
        self.write(json.dumps(res))
