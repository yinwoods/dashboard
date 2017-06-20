import json
import arrow
import asyncio
from dashboard.model.mssql import query
from dashboard.config import DATABASE
from dashboard.config import DBCONFIG
from dashboard.config import IDLOCAL
from dashboard.config import BRLOCAL
from dashboard.config import MELOCAL
from dashboard.model.DB import DB
from dashboard.handler.basehandler import BaseHandler


class IDNewsTitleSearchHandler(BaseHandler):

    LOCAL = IDLOCAL

    async def getNewsByTitle(self, title):
        """
        搜索新闻，使用sql like 匹配标题
        :param title: 搜索新闻标题关键字
        :return: 包含匹配结果的数组
        """
        ret = []

        sql = '''
            SELECT
                TOP 100 News.*, Tags.Name
            FROM
                {table}
            LEFT JOIN
                NewsTags
            ON
                News.NewsId = NewsTags.NewsId
            AND
                NewsTags.Score = 1
            LEFT JOIN
                Tags
            ON
                NewsTags.TagId = Tags.Id
            WHERE
                UPPER(title) LIKE '%{title}%'
            ORDER BY CreatedTime DESC
        '''.format(
                table='News',
                title=title.upper()
            )

        res = query(sql, self.LOCAL)
        for item in res:
            ret.append(item)

        return ret

    async def getData(self, title):
        data = []

        tasks = [
            self.getNewsByTitle(title)
        ]

        for task in asyncio.as_completed(tasks):
            data = data + (await task)

        return data

    def get(self):

        errid = 0
        errmsg = 'SUCCESS'
        data = None

        title = self.get_argument('title', default=None, strip=True)
        if title is None:
            errid = -1
            errmsg = 'Need title parameter'

        event_loop = asyncio.get_event_loop()
        try:
            data = event_loop.run_until_complete(
                    self.getData(title))
        except Exception as e:
            errid = -3
            errmsg = str(e)

        res = dict()
        res['errid'] = errid
        res['errmsg'] = errmsg
        res['data'] = data
        self.write(json.dumps(res))


class BRHotNewsCtrHandler(BaseHandler):

    LOCAL = BRLOCAL

    async def getCategoryTagFetch(self):
        # 新闻标签-展示新闻数
        ret = {}
        table = '{}_hotFetchReadByTag'.format(self.LOCAL)
        partreads = {}
        partfetches = {}

        totalRead = 0
        totalFetch = 0
        validRead = 0
        validFetch = 0
        videoRead = 0
        videoFetch = 0

        sql = '''
            SELECT
                categoryTag,
                hotTagFetch,
                hotTagRead
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
            categoryTag = item['categoryTag']
            hotTagRead = item['hotTagRead']
            hotTagFetch = item['hotTagFetch']

            totalRead += hotTagRead
            totalFetch += hotTagFetch

            validTags = ['JOKE', 'VIDEO', 'FUNNY', 'RELATIVENEWS']
            if all(tag not in categoryTag.upper() for tag in validTags):
                validRead += hotTagRead
                validFetch += hotTagFetch

            if 'VIDEO' in categoryTag.upper():
                videoRead += hotTagRead
                videoFetch += hotTagFetch

            partname = categoryTag.split(':')[0]
            partreads.update({
                partname: (partreads.get(partname, 0) + hotTagRead)
            })
            partfetches.update({
                partname: (partfetches.get(partname, 0) + hotTagFetch)
            })

        for item in res:
            categoryTag = item['categoryTag']
            hotTagRead = item['hotTagRead']
            hotTagFetch = item['hotTagFetch']

            partname = categoryTag.split(':')[0]
            percent = '{:.3%}'.format(hotTagFetch / partfetches[partname])
            ctr = '{:.3%}'.format(hotTagRead / hotTagFetch)

            ret.update({
                categoryTag: {
                    'hotTagRead': hotTagRead,
                    'hotTagFetch': hotTagFetch,
                    'percent': percent,
                    'ctr': ctr
                }
            })

        ret.update({
            'Video': {
                'hotTagFetch': videoFetch,
                'hotTagRead': videoRead,
                'percent': '==',
                'ctr': '{:.3%}'.format(videoRead / videoFetch)
            }
        })

        ret.update({
            'Total': {
                'hotTagFetch': totalFetch,
                'hotTagRead': totalRead,
                'percent': '==',
                'ctr': '{:.3%}'.format(totalRead / totalFetch)
            }
        })

        ret.update({
            'Valid': {
                'hotTagFetch': validFetch,
                'hotTagRead': validRead,
                'percent': '==',
                'ctr': '{:.3%}'.format(validRead / validFetch)
            }
        })

        for partname in partfetches.keys():
            ret.update({
                partname: {
                    'hotTagFetch': partfetches[partname],
                    'hotTagRead': partreads[partname],
                    'percent': '==',
                    'ctr': '{:.3%}'.format(
                        partreads[partname] / partfetches[partname])
                }
            })

        return ret

    async def getData(self, start_date, end_date):
        data = []

        for date in arrow.Arrow.range(
                frame='day', start=start_date, end=end_date):

            date = str(date).replace('-', '')
            date = int(date[:8])
            self.date = date

            tasks = [
                self.getCategoryTagFetch()
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


class MEHotNewsCtrHandler(BaseHandler):

    LOCAL = MELOCAL

    async def getCategoryTagFetch(self):
        # 新闻标签-展示新闻数
        ret = {}
        table = '{}_hotFetchReadByTag'.format(self.LOCAL)
        partreads = {}
        partfetches = {}

        totalRead = 0
        totalFetch = 0
        validRead = 0
        validFetch = 0
        videoRead = 0
        videoFetch = 0

        sql = '''
            SELECT
                categoryTag,
                hotTagFetch,
                hotTagRead
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
            categoryTag = item['categoryTag']
            hotTagRead = item['hotTagRead']
            hotTagFetch = item['hotTagFetch']

            totalRead += hotTagRead
            totalFetch += hotTagFetch

            validTags = ['JOKE', 'VIDEO', 'FUNNY', 'RELATIVENEWS']
            if all(tag not in categoryTag.upper() for tag in validTags):
                validRead += hotTagRead
                validFetch += hotTagFetch

            if 'VIDEO' in categoryTag.upper():
                videoRead += hotTagRead
                videoFetch += hotTagFetch

            partname = categoryTag.split(':')[0]
            partreads.update({
                partname: (partreads.get(partname, 0) + hotTagRead)
            })
            partfetches.update({
                partname: (partfetches.get(partname, 0) + hotTagFetch)
            })

        for item in res:
            categoryTag = item['categoryTag']
            hotTagRead = item['hotTagRead']
            hotTagFetch = item['hotTagFetch']

            partname = categoryTag.split(':')[0]
            percent = '{:.3%}'.format(hotTagFetch / partfetches[partname])
            ctr = '{:.3%}'.format(hotTagRead / hotTagFetch)

            ret.update({
                categoryTag: {
                    'hotTagRead': hotTagRead,
                    'hotTagFetch': hotTagFetch,
                    'percent': percent,
                    'ctr': ctr
                }
            })

        ret.update({
            'Video': {
                'hotTagFetch': videoFetch,
                'hotTagRead': videoRead,
                'percent': '==',
                'ctr': '{:.3%}'.format(videoRead / videoFetch)
            }
        })

        ret.update({
            'Total': {
                'hotTagFetch': totalFetch,
                'hotTagRead': totalRead,
                'percent': '==',
                'ctr': '{:.3%}'.format(totalRead / totalFetch)
            }
        })

        ret.update({
            'Valid': {
                'hotTagFetch': validFetch,
                'hotTagRead': validRead,
                'percent': '==',
                'ctr': '{:.3%}'.format(validRead / validFetch)
            }
        })

        for partname in partfetches.keys():
            ret.update({
                partname: {
                    'hotTagFetch': partfetches[partname],
                    'hotTagRead': partreads[partname],
                    'percent': '==',
                    'ctr': '{:.3%}'.format(
                        partreads[partname] / partfetches[partname])
                }
            })

        return ret

    async def getData(self, start_date, end_date):
        data = []

        for date in arrow.Arrow.range(
                frame='day', start=start_date, end=end_date):

            date = str(date).replace('-', '')
            date = int(date[:8])
            self.date = date

            tasks = [
                self.getCategoryTagFetch()
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
