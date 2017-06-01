import json
import arrow
import asyncio
from dashboard.model.DB import DB
from dashboard.config import DBCONFIG
from dashboard.config import DATABASE
from dashboard.config import IDLOCAL
from dashboard.config import BRLOCAL
from dashboard.handler.basehandler import BaseHandler


class IDNewsHandler(BaseHandler):

    LOCAL = IDLOCAL

    async def getCrawledNews(self):
        # 抓取新闻数
        # 含相关新闻的新闻比例
        ret = {}
        table = '{}_crawledNews'.format(self.LOCAL)

        sql = '''
            SELECT
                crawledNews,
                crawledNewsWithRelative
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
        for item in res:
            crawledNews = item['crawledNews']
            crawledNewsWithRelative = item['crawledNewsWithRelative']
            relativeRatio = '{:.3%}'.format(
                crawledNewsWithRelative / crawledNews
            )
            ret.update({'crawledNews': crawledNews})
            ret.update({'relativeRatio': relativeRatio})
        return ret

    async def getReadNews(self):
        # 点击新闻数
        ret = {}
        table = '{}_readsUserNews'.format(self.LOCAL)

        sql = '''
            SELECT
                readNews
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
        for item in res:
            ret.update({'readNews': item['readNews']})
        return ret

    async def getDisplayedNews(self):
        # 展示新闻数
        ret = {}
        table = '{}_fetchesUserNews'.format(self.LOCAL)

        sql = '''
            SELECT
                displayedNews
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
        for item in res:
            ret.update({'displayedNews': item['displayedNews']})
        return ret

    async def getDisplayCount(self):
        # 总展示次数
        ret = {}
        table = '{}_fetchReadByTag'.format(self.LOCAL)

        sql = '''
            SELECT
                CAST(SUM(categoryTagFetch) AS UNSIGNED)
            AS
                displayCount,
                CAST(SUM(categoryTagRead) AS UNSIGNED)
            AS
                readCount
            FROM
                {db}.{table}
            WHERE
                date = {date};
        '''.format(
                db=DATABASE,
                table=table,
                date=self.date
            )

        res = DB(**DBCONFIG).query(sql)
        for item in res:
            ret.update({'displayCount': item['displayCount']})
            ret.update({'readCount': item['readCount']})

        return ret

    async def getReadCount(self):
        # 总点击次数
        ret = {}
        table = '{}_fetchReadByTag'.format(self.LOCAL)

        sql = '''
            SELECT
                CAST(SUM(categoryTagFetch) AS UNSIGNED)
            AS
                displayCount
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
        for item in res:
            ret.update({'displayCount': item['displayCount']})
        return ret

    async def getData(self, start_date, end_date):
        data = []

        for date in arrow.Arrow.range(
                frame='day', start=start_date, end=end_date):

            date = str(date).replace('-', '')
            date = int(date[:8])
            self.date = date

            tasks = [
                self.getCrawledNews(),
                self.getDisplayedNews(),
                self.getReadNews(),
                self.getDisplayCount()
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


class BRNewsHandler(BaseHandler):

    LOCAL = BRLOCAL

    async def getCrawledNews(self):
        # 抓取新闻数
        # 含相关新闻的新闻比例
        ret = {}
        table = '{}_crawledNews'.format(self.LOCAL)

        sql = '''
            SELECT
                crawledNews,
                crawledNewsWithRelative
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
        for item in res:
            crawledNews = item['crawledNews']
            crawledNewsWithRelative = item['crawledNewsWithRelative']
            relativeRatio = '{:.3%}'.format(
                crawledNewsWithRelative / crawledNews
            )
            ret.update({'crawledNews': crawledNews})
            ret.update({'relativeRatio': relativeRatio})
        return ret

    async def getReadNews(self):
        # 点击新闻数
        ret = {}
        table = '{}_readsUserNews'.format(self.LOCAL)

        sql = '''
            SELECT
                readNews
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
        for item in res:
            ret.update({'readNews': item['readNews']})
        return ret

    async def getDisplayedNews(self):
        # 展示新闻数
        ret = {}
        table = '{}_fetchesUserNews'.format(self.LOCAL)

        sql = '''
            SELECT
                displayedNews
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
        for item in res:
            ret.update({'displayedNews': item['displayedNews']})
        return ret

    async def getDisplayCount(self):
        # 总展示次数
        ret = {}
        table = '{}_fetchReadByTag'.format(self.LOCAL)

        sql = '''
            SELECT
                CAST(SUM(categoryTagFetch) AS UNSIGNED)
            AS
                displayCount,
                CAST(SUM(categoryTagRead) AS UNSIGNED)
            AS
                readCount
            FROM
                {db}.{table}
            WHERE
                date = {date};
        '''.format(
                db=DATABASE,
                table=table,
                date=self.date
            )

        res = DB(**DBCONFIG).query(sql)
        for item in res:
            ret.update({'displayCount': item['displayCount']})
            ret.update({'readCount': item['readCount']})

        return ret

    async def getReadCount(self):
        # 总点击次数
        ret = {}
        table = '{}_fetchReadByTag'.format(self.LOCAL)

        sql = '''
            SELECT
                CAST(SUM(categoryTagFetch) AS UNSIGNED)
            AS
                displayCount
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
        for item in res:
            ret.update({'displayCount': item['displayCount']})
        return ret

    async def getData(self, start_date, end_date):
        data = []

        for date in arrow.Arrow.range(
                frame='day', start=start_date, end=end_date):

            date = str(date).replace('-', '')
            date = int(date[:8])
            self.date = date

            tasks = [
                self.getCrawledNews(),
                self.getDisplayedNews(),
                self.getReadNews(),
                self.getDisplayCount()
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
