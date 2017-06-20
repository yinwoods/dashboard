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


class IDSpecialClickPositionHandler(BaseHandler):

    LOCAL = IDLOCAL

    sql = '''
            SELECT
                CAST(SUM(countIndex) AS UNSIGNED)
            AS
                {col}
            FROM
                {db}.{table}
            WHERE
                date = {date}
            AND
                newsIndex = {index}
        '''

    async def getReadPushNews(self):
        # 阅读推送新闻数
        ret = {}
        table = '{}_countNewsIndex'.format(self.LOCAL)

        sql = self.sql.format(
                db=DATABASE,
                table=table,
                date=self.date,
                index=-1,
                col='readPushNews'
            )
        res = DB(**DBCONFIG).query(sql)
        for item in res:
            ret.update({'readPushNews': item['readPushNews']})

        sql = self.sql.format(
                db=DATABASE,
                table=table,
                date=self.date,
                index=-2,
                col='newsFromOtherLink'
            )
        res = DB(**DBCONFIG).query(sql)
        for item in res:
            ret.update({'newsFromOtherLink': item['newsFromOtherLink']})

        sql = self.sql.format(
                db=DATABASE,
                table=table,
                date=self.date,
                index=-3,
                col='relatedNews'
            )
        res = DB(**DBCONFIG).query(sql)
        for item in res:
            ret.update({'relatedNews': item['relatedNews']})

        sql = self.sql.format(
                db=DATABASE,
                table=table,
                date=self.date,
                index=-4,
                col='keptNews'
            )
        res = DB(**DBCONFIG).query(sql)
        for item in res:
            ret.update({'keptNews': item['keptNews']})

        sql = self.sql.format(
                db=DATABASE,
                table=table,
                date=self.date,
                index=-5,
                col='readNewsFromCommentLinkByOthers'
            )
        res = DB(**DBCONFIG).query(sql)
        for item in res:
            ret.update({
                'readNewsFromCommentLinkByOthers':
                    item['readNewsFromCommentLinkByOthers']})

        return ret

    async def getData(self, start_date, end_date):
        data = []

        for date in arrow.Arrow.range(
                frame='day', start=start_date, end=end_date):

            date = str(date).replace('-', '')
            date = int(date[:8])
            self.date = date

            tasks = [
                self.getReadPushNews()
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


class BRSpecialClickPositionHandler(BaseHandler):

    LOCAL = BRLOCAL

    sql = '''
            SELECT
                CAST(SUM(countIndex) AS UNSIGNED)
            AS
                {col}
            FROM
                {db}.{table}
            WHERE
                date = {date}
            AND
                newsIndex = {index}
        '''

    async def getReadPushNews(self):
        # 阅读推送新闻数
        ret = {}
        table = '{}_countNewsIndex'.format(self.LOCAL)

        sql = self.sql.format(
                db=DATABASE,
                table=table,
                date=self.date,
                index=-1,
                col='readPushNews'
            )
        res = DB(**DBCONFIG).query(sql)
        for item in res:
            ret.update({'readPushNews': item['readPushNews']})

        sql = self.sql.format(
                db=DATABASE,
                table=table,
                date=self.date,
                index=-2,
                col='newsFromOtherLink'
            )
        res = DB(**DBCONFIG).query(sql)
        for item in res:
            ret.update({'newsFromOtherLink': item['newsFromOtherLink']})

        sql = self.sql.format(
                db=DATABASE,
                table=table,
                date=self.date,
                index=-3,
                col='relatedNews'
            )
        res = DB(**DBCONFIG).query(sql)
        for item in res:
            ret.update({'relatedNews': item['relatedNews']})

        sql = self.sql.format(
                db=DATABASE,
                table=table,
                date=self.date,
                index=-4,
                col='keptNews'
            )
        res = DB(**DBCONFIG).query(sql)
        for item in res:
            ret.update({'keptNews': item['keptNews']})

        sql = self.sql.format(
                db=DATABASE,
                table=table,
                date=self.date,
                index=-5,
                col='readNewsFromCommentLinkByOthers'
            )
        res = DB(**DBCONFIG).query(sql)
        for item in res:
            ret.update({
                'readNewsFromCommentLinkByOthers':
                    item['readNewsFromCommentLinkByOthers']})

        return ret

    async def getData(self, start_date, end_date):
        data = []

        for date in arrow.Arrow.range(
                frame='day', start=start_date, end=end_date):

            date = str(date).replace('-', '')
            date = int(date[:8])
            self.date = date

            tasks = [
                self.getReadPushNews()
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


class MESpecialClickPositionHandler(BaseHandler):

    LOCAL = MELOCAL

    sql = '''
            SELECT
                CAST(SUM(countIndex) AS UNSIGNED)
            AS
                {col}
            FROM
                {db}.{table}
            WHERE
                date = {date}
            AND
                newsIndex = {index}
        '''

    async def getReadPushNews(self):
        # 阅读推送新闻数
        ret = {}
        table = '{}_countNewsIndex'.format(self.LOCAL)

        sql = self.sql.format(
                db=DATABASE,
                table=table,
                date=self.date,
                index=-1,
                col='readPushNews'
            )
        res = DB(**DBCONFIG).query(sql)
        for item in res:
            ret.update({'readPushNews': item['readPushNews']})

        sql = self.sql.format(
                db=DATABASE,
                table=table,
                date=self.date,
                index=-2,
                col='newsFromOtherLink'
            )
        res = DB(**DBCONFIG).query(sql)
        for item in res:
            ret.update({'newsFromOtherLink': item['newsFromOtherLink']})

        sql = self.sql.format(
                db=DATABASE,
                table=table,
                date=self.date,
                index=-3,
                col='relatedNews'
            )
        res = DB(**DBCONFIG).query(sql)
        for item in res:
            ret.update({'relatedNews': item['relatedNews']})

        sql = self.sql.format(
                db=DATABASE,
                table=table,
                date=self.date,
                index=-4,
                col='keptNews'
            )
        res = DB(**DBCONFIG).query(sql)
        for item in res:
            ret.update({'keptNews': item['keptNews']})

        sql = self.sql.format(
                db=DATABASE,
                table=table,
                date=self.date,
                index=-5,
                col='readNewsFromCommentLinkByOthers'
            )
        res = DB(**DBCONFIG).query(sql)
        for item in res:
            ret.update({
                'readNewsFromCommentLinkByOthers':
                    item['readNewsFromCommentLinkByOthers']})

        return ret

    async def getData(self, start_date, end_date):
        data = []

        for date in arrow.Arrow.range(
                frame='day', start=start_date, end=end_date):

            date = str(date).replace('-', '')
            date = int(date[:8])
            self.date = date

            tasks = [
                self.getReadPushNews()
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
