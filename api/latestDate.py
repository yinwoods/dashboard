import json
import asyncio
from dashboard.model.DB import DB
from dashboard.config import DBCONFIG
from dashboard.config import DATABASE
from dashboard.config import IDLOCAL
from dashboard.config import BRLOCAL
from dashboard.config import MELOCAL
from dashboard.handler.basehandler import BaseHandler


class IDLatestDateHandler(BaseHandler):

    LOCAL = IDLOCAL

    async def getLatestDate(self):
        # 活跃用户
        ret = {}
        table = '{}_fetchesUserNews'.format(self.LOCAL)

        sql = '''
            SELECT
                MAX(date) AS date
            FROM
                {db}.{table}
        '''.format(
                db=DATABASE,
                table=table,
            )

        res = DB(**DBCONFIG).query(sql)
        for item in res:
            ret.update({'date': item['date']})
        return ret

    async def getData(self):
        data = []

        tasks = [
            self.getLatestDate(),
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

        event_loop = asyncio.get_event_loop()
        try:
            data = event_loop.run_until_complete(
                    self.getData())
        except Exception as e:
            errid = -4
            errmsg = str(e)

        res = dict()
        res['errid'] = errid
        res['errmsg'] = errmsg
        res['data'] = data
        self.write(json.dumps(res))


class BRLatestDateHandler(BaseHandler):

    LOCAL = BRLOCAL

    async def getLatestDate(self):
        # 活跃用户
        ret = {}
        table = '{}_fetchesUserNews'.format(self.LOCAL)

        sql = '''
            SELECT
                MAX(date) AS date
            FROM
                {db}.{table}
        '''.format(
                db=DATABASE,
                table=table,
            )

        res = DB(**DBCONFIG).query(sql)
        for item in res:
            ret.update({'date': item['date']})
        return ret

    async def getData(self):
        data = []

        tasks = [
            self.getLatestDate(),
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

        event_loop = asyncio.get_event_loop()
        try:
            data = event_loop.run_until_complete(
                    self.getData())
        except Exception as e:
            errid = -4
            errmsg = str(e)

        res = dict()
        res['errid'] = errid
        res['errmsg'] = errmsg
        res['data'] = data
        self.write(json.dumps(res))


class MELatestDateHandler(BaseHandler):

    LOCAL = MELOCAL

    async def getLatestDate(self):
        # 活跃用户
        ret = {}
        table = '{}_fetchesUserNews'.format(self.LOCAL)

        sql = '''
            SELECT
                MAX(date) AS date
            FROM
                {db}.{table}
        '''.format(
                db=DATABASE,
                table=table,
            )

        res = DB(**DBCONFIG).query(sql)
        for item in res:
            ret.update({'date': item['date']})
        return ret

    async def getData(self):
        data = []

        tasks = [
            self.getLatestDate(),
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

        event_loop = asyncio.get_event_loop()
        try:
            data = event_loop.run_until_complete(
                    self.getData())
        except Exception as e:
            errid = -4
            errmsg = str(e)

        res = dict()
        res['errid'] = errid
        res['errmsg'] = errmsg
        res['data'] = data
        self.write(json.dumps(res))
