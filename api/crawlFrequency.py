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


class IDCrawlFrequencyHandler(BaseHandler):

    LOCAL = IDLOCAL

    async def getCrawlFrequency(self, newsType):
        """
        根据newType获取各个时间段的抓取数
        """
        table = '{}_crawlFrequency'.format(self.LOCAL)

        sql = '''
            SELECT
                crawledNewsNum, intervals
            FROM(
                SELECT
                    count(*) AS crawledNewsNum,
                    sec_to_time(
                        time_to_sec(newsCreatedTime)
                        -
                        time_to_sec(newsCreatedTime) % (10 * 60)
                    ) AS intervals
                FROM
                    {db}.{table}
                WHERE
                    date(newsCreatedTime) = '{newsCreatedTime}'
        '''.format(
                db=DATABASE,
                table=table,
                newsCreatedTime=self.date
            )
        if newsType is None:
            sql = sql + '''
                GROUP BY intervals) as tmp ORDER BY intervals
            '''
        else:
            sql = sql + '''
            AND newsType = {newsType} GROUP BY intervals) as tmp
            ORDER BY intervals
            '''.format(
                newsType=newsType
            )

        ret = {}
        res = DB(**DBCONFIG).query(sql)
        crawledNewsNum = 0

        for item in res:
            crawledNewsNum += int(item['crawledNewsNum'])
            newsCreatedTime = str(item['intervals'])
            ret.update({newsCreatedTime: crawledNewsNum})

        interval = arrow.get(str(self.date), 'YYYYMMDD')
        key = str(interval.format('H:mm:ss'))

        if key not in ret.keys():
            ret.update({key: 0})

        for _ in range(144):
            previous_key = key
            interval = interval.replace(minutes=+10)
            key = str(interval.format('H:mm:ss'))
            if key not in ret.keys():
                # {time: crawledNewsNum}
                ret.update({key: ret[previous_key]})

        # 将超过当前时间的数据删去
        utc = arrow.utcnow()
        _to_delete_keys = []
        for key in ret.keys():
            time = str(self.date) + ' ' + key
            if arrow.get(time, 'YYYYMMDD H:mm:ss') >= utc:
                _to_delete_keys.append(key)

        for key in _to_delete_keys:
            ret.pop(key, None)

        ret_lst = [{'time': item[0], 'crawledNewsNum': item[1]}
                   for item in sorted(
                    ret.items(), key=lambda x: arrow.get(x[0], 'H:mm:ss'))]

        return ret_lst

    async def getData(self, start_date, end_date, news_type):
        data = []

        for date in arrow.Arrow.range(
                frame='day', start=start_date, end=end_date):

            date = str(date).replace('-', '')
            date = int(date[:8])
            self.date = date

            tasks = [
                self.getCrawlFrequency(news_type)
            ]

            cur_data = dict({'date': self.date})
            for task in asyncio.as_completed(tasks):
                cur_data.update({'data': await task})

            data += [cur_data]
        return data

    def post(self):

        errid = 0
        errmsg = 'SUCCESS'
        data = None

        start_date = self.get_argument('start_date', default=None, strip=True)
        end_date = self.get_argument('end_date', default=None, strip=True)
        news_type = self.get_argument('news_type', default=None, strip=True)
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
                    self.getData(start_date, end_date, news_type))
        except Exception as e:
            errid = -3
            errmsg = str(e)

        res = dict()
        res['errid'] = errid
        res['errmsg'] = errmsg
        res['data'] = data
        self.write(json.dumps(res))


class BRCrawlFrequencyHandler(BaseHandler):

    LOCAL = BRLOCAL

    async def getCrawlFrequency(self, newsType):
        # 新闻标签-展示新闻数
        table = '{}_crawlFrequency'.format(self.LOCAL)

        sql = '''
            SELECT
                crawledNewsNum, intervals
            FROM(
                SELECT
                    count(*) AS crawledNewsNum,
                    sec_to_time(
                        time_to_sec(newsCreatedTime) -
                        time_to_sec(newsCreatedTime) % (10 * 60)
                    ) AS intervals
                FROM
                    {db}.{table}
                WHERE
                    date(newsCreatedTime) = '{newsCreatedTime}'
        '''.format(
                db=DATABASE,
                table=table,
                newsCreatedTime=self.date
            )
        if newsType is None:
            sql = sql + '''
                GROUP BY intervals) as tmp ORDER BY intervals
            '''
        else:
            sql = sql + '''
            AND newsType = {newsType} GROUP BY intervals) as tmp
            ORDER BY intervals
            '''.format(
                newsType=newsType
            )

        ret = {}
        res = DB(**DBCONFIG).query(sql)
        crawledNewsNum = 0

        for item in res:
            crawledNewsNum += int(item['crawledNewsNum'])
            newsCreatedTime = str(item['intervals'])
            ret.update({newsCreatedTime: crawledNewsNum})

        interval = arrow.get(str(self.date), 'YYYYMMDD')
        key = str(interval.format('H:mm:ss'))

        if key not in ret.keys():
            ret.update({key: 0})

        for _ in range(144):
            previous_key = key
            interval = interval.replace(minutes=+10)
            key = str(interval.format('H:mm:ss'))
            if key not in ret.keys():
                # {time: crawledNewsNum}
                ret.update({key: ret[previous_key]})

        utc = arrow.utcnow()
        _to_delete_keys = []
        for key in ret.keys():
            time = str(self.date) + ' ' + key
            if arrow.get(time, 'YYYYMMDD H:mm:ss') > utc:
                _to_delete_keys.append(key)

        for key in _to_delete_keys:
            ret.pop(key, None)

        ret_lst = [{'time': item[0], 'crawledNewsNum': item[1]}
                   for item in sorted(
                       ret.items(), key=lambda x: arrow.get(x[0], 'H:mm:ss'))]

        return ret_lst

    async def getData(self, start_date, end_date, news_type):
        data = []

        for date in arrow.Arrow.range(
                frame='day', start=start_date, end=end_date):

            date = str(date).replace('-', '')
            date = int(date[:8])
            self.date = date

            tasks = [
                self.getCrawlFrequency(news_type)
            ]

            cur_data = dict({'date': self.date})
            for task in asyncio.as_completed(tasks):
                cur_data.update({'data': await task})

            data += [cur_data]
        return data

    def post(self):

        errid = 0
        errmsg = 'SUCCESS'
        data = None

        start_date = self.get_argument('start_date', default=None, strip=True)
        end_date = self.get_argument('end_date', default=None, strip=True)
        news_type = self.get_argument('news_type', default=None, strip=True)
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
                    self.getData(start_date, end_date, news_type))
        except Exception as e:
            errid = -3
            errmsg = str(e)

        res = dict()
        res['errid'] = errid
        res['errmsg'] = errmsg
        res['data'] = data
        self.write(json.dumps(res))


class MECrawlFrequencyHandler(BaseHandler):

    LOCAL = MELOCAL

    async def getCrawlFrequency(self, newsType):
        # 新闻标签-展示新闻数
        table = '{}_crawlFrequency'.format(self.LOCAL)

        sql = '''
            SELECT
                crawledNewsNum, intervals
            FROM(
                SELECT
                    count(*) AS crawledNewsNum,
                    sec_to_time(
                        time_to_sec(newsCreatedTime) -
                        time_to_sec(newsCreatedTime) % (10 * 60)
                    ) AS intervals
                FROM
                    {db}.{table}
                WHERE
                    date(newsCreatedTime) = '{newsCreatedTime}'
        '''.format(
                db=DATABASE,
                table=table,
                newsCreatedTime=self.date
            )
        if newsType is None:
            sql = sql + '''
                GROUP BY intervals) as tmp ORDER BY intervals
            '''
        else:
            sql = sql + '''
            AND newsType = {newsType} GROUP BY intervals) as tmp
            ORDER BY intervals
            '''.format(
                newsType=newsType
            )

        ret = {}
        res = DB(**DBCONFIG).query(sql)
        crawledNewsNum = 0

        for item in res:
            crawledNewsNum += int(item['crawledNewsNum'])
            newsCreatedTime = str(item['intervals'])
            ret.update({newsCreatedTime: crawledNewsNum})

        interval = arrow.get(str(self.date), 'YYYYMMDD')
        key = str(interval.format('H:mm:ss'))

        if key not in ret.keys():
            ret.update({key: 0})

        for _ in range(144):
            previous_key = key
            interval = interval.replace(minutes=+10)
            key = str(interval.format('H:mm:ss'))
            if key not in ret.keys():
                # {time: crawledNewsNum}
                ret.update({key: ret[previous_key]})

        utc = arrow.utcnow()
        _to_delete_keys = []
        for key in ret.keys():
            time = str(self.date) + ' ' + key
            if arrow.get(time, 'YYYYMMDD H:mm:ss') > utc:
                _to_delete_keys.append(key)

        for key in _to_delete_keys:
            ret.pop(key, None)

        ret_lst = [{'time': item[0], 'crawledNewsNum': item[1]}
                   for item in sorted(
                       ret.items(), key=lambda x: arrow.get(x[0], 'H:mm:ss'))]

        return ret_lst

    async def getData(self, start_date, end_date, news_type):
        data = []

        for date in arrow.Arrow.range(
                frame='day', start=start_date, end=end_date):

            date = str(date).replace('-', '')
            date = int(date[:8])
            self.date = date

            tasks = [
                self.getCrawlFrequency(news_type)
            ]

            cur_data = dict({'date': self.date})
            for task in asyncio.as_completed(tasks):
                cur_data.update({'data': await task})

            data += [cur_data]
        return data

    def post(self):

        errid = 0
        errmsg = 'SUCCESS'
        data = None

        start_date = self.get_argument('start_date', default=None, strip=True)
        end_date = self.get_argument('end_date', default=None, strip=True)
        news_type = self.get_argument('news_type', default=None, strip=True)
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
                    self.getData(start_date, end_date, news_type))
        except Exception as e:
            errid = -3
            errmsg = str(e)

        res = dict()
        res['errid'] = errid
        res['errmsg'] = errmsg
        res['data'] = data
        self.write(json.dumps(res))
