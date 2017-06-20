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


class IDNewsPushedNewUserHandler(BaseHandler):

    LOCAL = IDLOCAL

    async def getFreshPushCount(self):
        # 推送到达数
        # 推送阅读数
        ret = {}
        table = '{}_freshPushRealCtr'.format(self.LOCAL)

        sql = '''
            SELECT
                freshPushSuccessCount,
                freshPushCount,
                freshPushRead
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
            ret.update({
                'freshPushSuccessCount': item.get('freshPushSuccessCount')})
            ret.update({'freshPushCount': item.get('freshPushCount')})
            ret.update({'freshPushRead': item.get('freshPushRead')})
        return ret

    async def getData(self, start_date, end_date):
        data = []

        for date in arrow.Arrow.range(
                frame='day', start=start_date, end=end_date):

            date = str(date).replace('-', '')
            date = int(date[:8])
            self.date = date

            tasks = [
                self.getFreshPushCount()
            ]

            cur_data = dict()
            for task in asyncio.as_completed(tasks):
                cur_data.update(await task)

            freshPushSuccessCount = cur_data.get('freshPushSuccessCount')
            freshPushCount = cur_data.get('freshPushCount')
            freshPushRead = cur_data.get('freshPushRead')
            try:
                pushReachRatio = '{:.3%}'.format(
                            freshPushSuccessCount / freshPushCount)
            except:
                pushReachRatio = 0.00

            try:
                pushRealCtr = '{:.3%}'.format(
                            freshPushRead / freshPushSuccessCount)
            except:
                pushRealCtr = 0.00

            cur_data.update({'pushReachRatio': pushReachRatio})
            cur_data.update({'pushRealCtr': pushRealCtr})

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


class BRNewsPushedNewUserHandler(BaseHandler):

    LOCAL = BRLOCAL

    async def getFreshPushCount(self):
        # 推送到达数
        # 推送阅读数
        ret = {}
        table = '{}_freshPushRealCtr'.format(self.LOCAL)

        sql = '''
            SELECT
                freshPushSuccessCount,
                freshPushCount,
                freshPushRead
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
            ret.update({
                'freshPushSuccessCount': item.get('freshPushSuccessCount', 0)})
            ret.update({'freshPushCount': item.get('freshPushCount', 0)})
            ret.update({'freshPushRead': item.get('freshPushRead', 0)})
        return ret

    async def getData(self, start_date, end_date):
        data = []

        for date in arrow.Arrow.range(
                frame='day', start=start_date, end=end_date):

            date = str(date).replace('-', '')
            date = int(date[:8])
            self.date = date

            tasks = [
                self.getFreshPushCount(),
            ]

            cur_data = dict()
            for task in asyncio.as_completed(tasks):
                cur_data.update(await task)

            freshPushSuccessCount = cur_data.get('freshPushSuccessCount', 0)
            freshPushCount = cur_data.get('freshPushCount', 0)
            freshPushRead = cur_data.get('freshPushRead', 0)

            try:
                pushReachRatio = '{:.3%}'.format(
                            freshPushSuccessCount / freshPushCount)
            except:
                pushReachRatio = 0.00

            try:
                pushRealCtr = '{:.3%}'.format(
                            freshPushRead / freshPushSuccessCount)
            except:
                pushRealCtr = 0.00

            cur_data.update({'pushReachRatio': pushReachRatio})
            cur_data.update({'pushRealCtr': pushRealCtr})

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


class MENewsPushedNewUserHandler(BaseHandler):

    LOCAL = MELOCAL

    async def getFreshPushCount(self):
        # 推送到达数
        # 推送阅读数
        ret = {}
        table = '{}_freshPushRealCtr'.format(self.LOCAL)

        sql = '''
            SELECT
                freshPushSuccessCount,
                freshPushCount,
                freshPushRead
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
            ret.update({
                'freshPushSuccessCount': item.get('freshPushSuccessCount', 0)})
            ret.update({'freshPushCount': item.get('freshPushCount', 0)})
            ret.update({'freshPushRead': item.get('freshPushRead', 0)})
        return ret

    async def getData(self, start_date, end_date):
        data = []

        for date in arrow.Arrow.range(
                frame='day', start=start_date, end=end_date):

            date = str(date).replace('-', '')
            date = int(date[:8])
            self.date = date

            tasks = [
                self.getFreshPushCount(),
            ]

            cur_data = dict()
            for task in asyncio.as_completed(tasks):
                cur_data.update(await task)

            freshPushSuccessCount = cur_data.get('freshPushSuccessCount', 0)
            freshPushCount = cur_data.get('freshPushCount', 0)
            freshPushRead = cur_data.get('freshPushRead', 0)

            try:
                pushReachRatio = '{:.3%}'.format(
                            freshPushSuccessCount / freshPushCount)
            except:
                pushReachRatio = 0.00

            try:
                pushRealCtr = '{:.3%}'.format(
                            freshPushRead / freshPushSuccessCount)
            except:
                pushRealCtr = 0.00

            cur_data.update({'pushReachRatio': pushReachRatio})
            cur_data.update({'pushRealCtr': pushRealCtr})

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
