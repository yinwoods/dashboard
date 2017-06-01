import json
import arrow
import asyncio
from dashboard.model.DB import DB
from dashboard.config import DBCONFIG
from dashboard.config import DATABASE
from dashboard.config import IDLOCAL
from dashboard.config import BRLOCAL
from dashboard.handler.basehandler import BaseHandler


class IDNewsReadDurationHandler(BaseHandler):

    LOCAL = IDLOCAL

    async def getNewsReadTimeStatistics(self):
        # 新闻阅读时长
        ret = {}
        table = '{}_readsNewsTimeStatistics'.format(self.LOCAL)

        sql = '''
            SELECT
                lessThan5sec,
                moreThan5secLessThan20sec,
                moreThan20secLessThan60sec,
                moreThan60sec,
                avgReadTime
            FROM
                {db}.{table}
            WHERE
                date = {date}
        '''.format(
                db=DATABASE,
                table=table,
                date=self.date,
            )

        res = DB(**DBCONFIG).query(sql)
        for item in res:
            lessThan5sec = int(item['lessThan5sec'])
            moreThan5secLessThan20sec = int(item['moreThan5secLessThan20sec'])
            moreThan20secLessThan60sec =\
                int(item['moreThan20secLessThan60sec'])
            moreThan60sec = int(item['moreThan60sec'])

            totalReadTime = sum((
                    lessThan5sec, moreThan5secLessThan20sec,
                    moreThan20secLessThan60sec, moreThan60sec))

            lessThan5sec = '{:.3%}'.format(lessThan5sec / totalReadTime)
            moreThan5secLessThan20sec = '{:.3%}'.format(
                    moreThan5secLessThan20sec / totalReadTime)
            moreThan20secLessThan60sec = '{:.3%}'.format(
                    moreThan20secLessThan60sec / totalReadTime)
            moreThan60sec = '{:.3%}'.format(
                    moreThan60sec / totalReadTime)

            ret.update({'lessThan5sec': lessThan5sec})
            ret.update({
                'moreThan5secLessThan20sec': moreThan5secLessThan20sec})
            ret.update({
                'moreThan20secLessThan60sec': moreThan20secLessThan60sec})
            ret.update({'moreThan60sec': moreThan60sec})
            ret.update({'totalReadTime': totalReadTime})
            ret.update({'avgReadTime': float(item['avgReadTime'])})

        return ret

    async def getData(self, start_date, end_date):
        data = []

        for date in arrow.Arrow.range(
                frame='day', start=start_date, end=end_date):

            date = str(date).replace('-', '')
            date = int(date[:8])
            self.date = date

            tasks = [
                self.getNewsReadTimeStatistics()
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


class BRNewsReadDurationHandler(BaseHandler):

    LOCAL = BRLOCAL

    async def getNewsReadTimeStatistics(self):
        # 新闻阅读时长
        ret = {}
        table = '{}_readsNewsTimeStatistics'.format(self.LOCAL)

        sql = '''
            SELECT
                lessThan5sec,
                moreThan5secLessThan20sec,
                moreThan20secLessThan60sec,
                moreThan60sec,
                avgReadTime
            FROM
                {db}.{table}
            WHERE
                date = {date}
        '''.format(
                db=DATABASE,
                table=table,
                date=self.date,
            )

        res = DB(**DBCONFIG).query(sql)
        for item in res:
            lessThan5sec = int(item['lessThan5sec'])
            moreThan5secLessThan20sec = int(item['moreThan5secLessThan20sec'])
            moreThan20secLessThan60sec =\
                int(item['moreThan20secLessThan60sec'])
            moreThan60sec = int(item['moreThan60sec'])

            totalReadTime = sum((
                    lessThan5sec, moreThan5secLessThan20sec,
                    moreThan20secLessThan60sec, moreThan60sec))

            lessThan5sec = '{:.3%}'.format(lessThan5sec / totalReadTime)
            moreThan5secLessThan20sec = '{:.3%}'.format(
                    moreThan5secLessThan20sec / totalReadTime)
            moreThan20secLessThan60sec = '{:.3%}'.format(
                    moreThan20secLessThan60sec / totalReadTime)
            moreThan60sec = '{:.3%}'.format(
                    moreThan60sec / totalReadTime)

            ret.update({'lessThan5sec': lessThan5sec})
            ret.update({
                'moreThan5secLessThan20sec': moreThan5secLessThan20sec})
            ret.update({
                'moreThan20secLessThan60sec': moreThan20secLessThan60sec})
            ret.update({'moreThan60sec': moreThan60sec})
            ret.update({'totalReadTime': totalReadTime})
            ret.update({'avgReadTime': float(item['avgReadTime'])})

        return ret

    async def getData(self, start_date, end_date):
        data = []

        for date in arrow.Arrow.range(
                frame='day', start=start_date, end=end_date):

            date = str(date).replace('-', '')
            date = int(date[:8])
            self.date = date

            tasks = [
                self.getNewsReadTimeStatistics()
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
