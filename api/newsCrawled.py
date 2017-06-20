import json
import arrow
import asyncio
from dashboard.model.DB import DB
from dashboard.config import DBCONFIG
from dashboard.config import DATABASE
from dashboard.config import IDLOCAL
from dashboard.config import BRLOCAL
from dashboard.config import MELOCAL
from dashboard.handler.basehandler import BaseHandler


class IDNewsCrawledHandler(BaseHandler):

    LOCAL = IDLOCAL

    async def getNewsCrawled(self):
        # 抓取新闻类型
        # 抓取新闻条数
        ret = {}
        table = '{}_countNewsByType'.format(self.LOCAL)

        sql = '''
            SELECT
                newsType, newsCount
            FROM
                {db}.{table}
            WHERE
                date = {date}
        '''.format(
                db=DATABASE,
                table=table,
                date=self.date
            )

        res = DB(**DBCONFIG).query(sql)

        newsTypeName = {
            '0': 'News',
            '1': 'FunnyPictures',
            '2': 'Gallery',
            '3': 'Video',
            '4': 'Joke',
            '5': 'Blog',
            '6': 'Forum',
            '10000': 'AppWall',
        }

        for item in res:
            newsType = newsTypeName.get(item['newsType'], 'AppWall')
            newsCount = item['newsCount']
            ret.update({newsType: newsCount})
        return ret

    async def getData(self, start_date, end_date):
        data = []

        for date in arrow.Arrow.range(
                frame='day', start=start_date, end=end_date):

            date = str(date).replace('-', '')
            date = int(date[:8])
            self.date = date

            tasks = [
                self.getNewsCrawled(),
            ]

            cur_data = dict()
            for task in asyncio.as_completed(tasks):
                cur_data.update(await task)

            data += [cur_data]
        return data

    def post(self):

        errid = 0
        errmsg = 'SUCCESS'
        data = None

        start_date = self.get_argument('start_date', default=None, strip=True)
        end_date = self.get_argument('end_date', default=None, strip=True)
        if start_date is None or end_date is None:
            errid = -1
            errmsg = 'Need date parameter'

        try:
            start_date = arrow.Arrow.strptime(start_date, '%Y%m%d')
            end_date = arrow.Arrow.strptime(
                end_date, '%Y%m%d').replace(days=-1)
        except Exception as e:
            errid = -2
            errmsg = 'date parameter should be %Y%m%d format like 20170101'

        event_loop = asyncio.get_event_loop()
        try:
            data = event_loop.run_until_complete(
                    self.getData(start_date, end_date))
        except Exception as e:
            errid = -3
            errmsg = str(e)

        res = dict()
        res['errid'] = errid
        res['errmsg'] = errmsg
        res['data'] = data
        self.write(json.dumps(res))


class BRNewsCrawledHandler(BaseHandler):

    LOCAL = BRLOCAL

    async def getNewsCrawled(self):
        # 抓取新闻类型
        # 抓取新闻条数
        ret = {}
        table = '{}_countNewsByType'.format(self.LOCAL)

        sql = '''
            SELECT
                newsType, newsCount
            FROM
                {db}.{table}
            WHERE
                date = {date}
        '''.format(
                db=DATABASE,
                table=table,
                date=self.date
            )

        res = DB(**DBCONFIG).query(sql)

        newsTypeName = {
            '0': 'News',
            '1': 'FunnyPictures',
            '2': 'Gallery',
            '3': 'Video',
            '4': 'Joke',
            '5': 'Blog',
            '6': 'Forum',
            '10000': 'AppWall',
        }

        for item in res:
            newsType = newsTypeName.get(item['newsType'], 'AppWall')
            newsCount = item['newsCount']
            ret.update({newsType: newsCount})
        return ret

    async def getData(self, start_date, end_date):
        data = []

        for date in arrow.Arrow.range(
                frame='day', start=start_date, end=end_date):

            date = str(date).replace('-', '')
            date = int(date[:8])
            self.date = date

            tasks = [
                self.getNewsCrawled(),
            ]

            cur_data = dict()
            for task in asyncio.as_completed(tasks):
                cur_data.update(await task)

            data += [cur_data]
        return data

    def post(self):

        errid = 0
        errmsg = 'SUCCESS'
        data = None

        start_date = self.get_argument('start_date', default=None, strip=True)
        end_date = self.get_argument('end_date', default=None, strip=True)
        if start_date is None or end_date is None:
            errid = -1
            errmsg = 'Need date parameter'

        try:
            start_date = arrow.Arrow.strptime(start_date, '%Y%m%d')
            end_date = arrow.Arrow.strptime(
                end_date, '%Y%m%d').replace(days=-1)
        except Exception as e:
            errid = -2
            errmsg = 'date parameter should be %Y%m%d format like 20170101'

        event_loop = asyncio.get_event_loop()
        try:
            data = event_loop.run_until_complete(
                    self.getData(start_date, end_date))
        except Exception as e:
            errid = -3
            errmsg = str(e)

        res = dict()
        res['errid'] = errid
        res['errmsg'] = errmsg
        res['data'] = data
        self.write(json.dumps(res))


class MENewsCrawledHandler(BaseHandler):

    LOCAL = MELOCAL

    async def getNewsCrawled(self):
        # 抓取新闻类型
        # 抓取新闻条数
        ret = {}
        table = '{}_countNewsByType'.format(self.LOCAL)

        sql = '''
            SELECT
                newsType, newsCount
            FROM
                {db}.{table}
            WHERE
                date = {date}
        '''.format(
                db=DATABASE,
                table=table,
                date=self.date
            )

        res = DB(**DBCONFIG).query(sql)

        newsTypeName = {
            '0': 'News',
            '1': 'FunnyPictures',
            '2': 'Gallery',
            '3': 'Video',
            '4': 'Joke',
            '5': 'Blog',
            '6': 'Forum',
            '10000': 'AppWall',
        }

        for item in res:
            newsType = newsTypeName.get(item['newsType'], 'AppWall')
            newsCount = item['newsCount']
            ret.update({newsType: newsCount})
        return ret

    async def getData(self, start_date, end_date):
        data = []

        for date in arrow.Arrow.range(
                frame='day', start=start_date, end=end_date):

            date = str(date).replace('-', '')
            date = int(date[:8])
            self.date = date

            tasks = [
                self.getNewsCrawled(),
            ]

            cur_data = dict()
            for task in asyncio.as_completed(tasks):
                cur_data.update(await task)

            data += [cur_data]
        return data

    def post(self):

        errid = 0
        errmsg = 'SUCCESS'
        data = None

        start_date = self.get_argument('start_date', default=None, strip=True)
        end_date = self.get_argument('end_date', default=None, strip=True)
        if start_date is None or end_date is None:
            errid = -1
            errmsg = 'Need date parameter'

        try:
            start_date = arrow.Arrow.strptime(start_date, '%Y%m%d')
            end_date = arrow.Arrow.strptime(
                end_date, '%Y%m%d').replace(days=-1)
        except Exception as e:
            errid = -2
            errmsg = 'date parameter should be %Y%m%d format like 20170101'

        event_loop = asyncio.get_event_loop()
        try:
            data = event_loop.run_until_complete(
                    self.getData(start_date, end_date))
        except Exception as e:
            errid = -3
            errmsg = str(e)

        res = dict()
        res['errid'] = errid
        res['errmsg'] = errmsg
        res['data'] = data
        self.write(json.dumps(res))
