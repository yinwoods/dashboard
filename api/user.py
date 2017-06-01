import json
import arrow
import asyncio
from dashboard.model.DB import DB
from dashboard.config import DBCONFIG
from dashboard.config import DATABASE
from dashboard.config import IDLOCAL
from dashboard.config import BRLOCAL
from dashboard.handler.basehandler import BaseHandler


class IDUserHandler(BaseHandler):

    LOCAL = IDLOCAL

    async def getActiveUser(self):
        # 活跃用户
        ret = {}
        table = '{}_fetchesUserNews'.format(self.LOCAL)

        sql = '''
            SELECT
                activeUser
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
            ret.update({'activeUser': item['activeUser']})
        return ret

    async def getReadNewsUser(self):
        # 读新闻用户
        ret = {}
        table = '{}_readsUserNews'.format(self.LOCAL)

        sql = '''
            SELECT
                readNewsUser
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
            ret.update({'readNewsUser': item['readNewsUser']})
        return ret

    async def getFreshUser(self):
        # 新用户
        ret = {}
        table = '{}_freshUserCount'.format(self.LOCAL)

        sql = '''
            SELECT
                freshUser
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
            ret.update({'freshUser': item['freshUser']})
        return ret

    async def getLoginUser(self):
        # 登录用户
        ret = {}
        table = '{}_loginUserTotal'.format(self.LOCAL)

        sql = '''
            SELECT
                loginUser
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
            ret.update({'loginUser': item['loginUser']})
        return ret

    async def getCommentUser(self):
        # 评论用户
        # 评论用户比例
        ret = {}
        table = '{}_commentUser'.format(self.LOCAL)

        sql = '''
            SELECT
                commentUser,
                totalComments
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
            ret.update({'commentUser': item['commentUser']})
            ret.update({'totalComments': item['totalComments']})
        return ret

    async def getData(self, start_date, end_date):
        data = []

        for date in arrow.Arrow.range(
                frame='day', start=start_date, end=end_date):

            date = str(date).replace('-', '')
            date = int(date[:8])
            self.date = date

            tasks = [
                self.getActiveUser(),
                self.getReadNewsUser(),
                self.getFreshUser(),
                self.getLoginUser(),
                self.getCommentUser(),
            ]

            cur_data = dict()
            for task in asyncio.as_completed(tasks):
                cur_data.update(await task)

            # 登录用户比例
            loginUser = cur_data.get('loginUser', 0)
            activeUser = cur_data.get('activeUser', 0)

            try:
                loginRation = '{:.3%}'.format(loginUser / activeUser)
            except ZeroDivisionError:
                loginRation = '0.00%'

            cur_data.update({'loginRation': loginRation})
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
            errid = -4
            errmsg = str(e)

        res = dict()
        res['errid'] = errid
        res['errmsg'] = errmsg
        res['data'] = data
        self.write(json.dumps(res))


class BRUserHandler(BaseHandler):

    LOCAL = BRLOCAL

    async def getActiveUser(self):
        # 活跃用户
        ret = {}
        table = '{}_fetchesUserNews'.format(self.LOCAL)

        sql = '''
            SELECT
                activeUser
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
            ret.update({'activeUser': item['activeUser']})
        return ret

    async def getReadNewsUser(self):
        # 读新闻用户
        ret = {}
        table = '{}_readsUserNews'.format(self.LOCAL)

        sql = '''
            SELECT
                readNewsUser
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
            ret.update({'readNewsUser': item['readNewsUser']})
        return ret

    async def getFreshUser(self):
        # 新用户
        ret = {}
        table = '{}_freshUserCount'.format(self.LOCAL)

        sql = '''
            SELECT
                freshUser
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
            ret.update({'freshUser': item['freshUser']})
        return ret

    async def getLoginUser(self):
        # 登录用户
        ret = {}
        table = '{}_loginUserTotal'.format(self.LOCAL)

        sql = '''
            SELECT
                loginUser
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
            ret.update({'loginUser': item['loginUser']})
        return ret

    async def getCommentUser(self):
        # 评论用户
        # 评论用户比例
        ret = {}
        table = '{}_commentUser'.format(self.LOCAL)

        sql = '''
            SELECT
                commentUser,
                totalComments
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
            ret.update({'commentUser': item['commentUser']})
            ret.update({'totalComments': item['totalComments']})
        return ret

    async def getData(self, start_date, end_date):
        data = []

        for date in arrow.Arrow.range(
                frame='day', start=start_date, end=end_date):

            date = str(date).replace('-', '')
            date = int(date[:8])
            self.date = date

            tasks = [
                self.getActiveUser(),
                self.getReadNewsUser(),
                self.getFreshUser(),
                self.getLoginUser(),
                self.getCommentUser(),
            ]

            cur_data = dict()
            for task in asyncio.as_completed(tasks):
                cur_data.update(await task)

            # 登录用户比例
            loginUser = cur_data.get('loginUser', 0)
            activeUser = cur_data.get('activeUser', 0)

            try:
                loginRation = '{:.3%}'.format(loginUser / activeUser)
            except ZeroDivisionError:
                loginRation = '0.00%'

            cur_data.update({'loginRation': loginRation})
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
            errid = -4
            errmsg = str(e)

        res = dict()
        res['errid'] = errid
        res['errmsg'] = errmsg
        res['data'] = data
        self.write(json.dumps(res))
