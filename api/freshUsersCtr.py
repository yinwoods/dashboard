import json
import arrow
import asyncio
from dashboard.model.DB import DB
from dashboard.config import DBCONFIG
from dashboard.config import DATABASE
from dashboard.config import IDLOCAL
from dashboard.config import BRLOCAL
from dashboard.handler.basehandler import BaseHandler


class IDFreshUsersCtrHandler(BaseHandler):

    LOCAL = IDLOCAL

    async def getCategoryTagFetch(self):
        # 新闻标签-展示新闻数
        ret = {}
        table = '{}_freshFetchReadByTag'.format(self.LOCAL)
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
                freshTagFetch,
                freshTagRead
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
            freshTagRead = item['freshTagRead']
            freshTagFetch = item['freshTagFetch']

            totalRead += freshTagRead
            totalFetch += freshTagFetch

            validTags = ['JOKE', 'VIDEO', 'FUNNY', 'RELATIVENEWS']
            if all(tag not in categoryTag.upper() for tag in validTags):
                validRead += freshTagRead
                validFetch += freshTagFetch

            if 'VIDEO' in categoryTag.upper():
                videoRead += freshTagRead
                videoFetch += freshTagFetch

            partname = categoryTag.split(':')[0]
            partreads.update({
                partname: (partreads.get(partname, 0) + freshTagRead)
            })
            partfetches.update({
                partname: (partfetches.get(partname, 0) + freshTagFetch)
            })

        for item in res:
            categoryTag = item['categoryTag']
            freshTagRead = item['freshTagRead']
            freshTagFetch = item['freshTagFetch']

            partname = categoryTag.split(':')[0]
            try:
                percent = '{:.3%}'.format(
                        freshTagFetch / partfetches[partname])
            except ZeroDivisionError:
                percent = '0.00%'

            try:
                ctr = '{:.3%}'.format(freshTagRead / freshTagFetch)
            except ZeroDivisionError:
                ctr = '0.00%'

            ret.update({
                categoryTag: {
                    'freshTagRead': freshTagRead,
                    'freshTagFetch': freshTagFetch,
                    'percent': percent,
                    'ctr': ctr
                }
            })

        try:
            video_ctr = '{:.3%}'.format(videoRead / videoFetch)
        except ZeroDivisionError:
            video_ctr = '0.00%'

        ret.update({
            'Video': {
                'freshTagFetch': videoFetch,
                'freshTagRead': videoRead,
                'percent': '==',
                'ctr': video_ctr,
            }
        })

        try:
            total_ctr = '{:.3%}'.format(totalRead / totalFetch)
        except ZeroDivisionError:
            total_ctr = '0.00%'

        ret.update({
            'Total': {
                'freshTagFetch': totalFetch,
                'freshTagRead': totalRead,
                'percent': '==',
                'ctr': total_ctr
            }
        })

        try:
            valid_ctr = '{:.3%}'.format(validRead / validFetch)
        except ZeroDivisionError:
            valid_ctr = '0.00%'

        ret.update({
            'Valid': {
                'freshTagFetch': validFetch,
                'freshTagRead': validRead,
                'percent': '==',
                'ctr': valid_ctr
            }
        })

        for partname in partfetches.keys():

            try:
                part_ctr = '{:.3%}'.format(
                        partreads[partname] / partfetches[partname])
            except ZeroDivisionError:
                part_ctr = '0.00%'

            ret.update({
                partname: {
                    'freshTagFetch': partfetches[partname],
                    'freshTagRead': partreads[partname],
                    'percent': '==',
                    'ctr': part_ctr
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


class BRFreshUsersCtrHandler(BaseHandler):

    LOCAL = BRLOCAL

    async def getCategoryTagFetch(self):
        # 新闻标签-展示新闻数
        ret = {}
        table = '{}_freshFetchReadByTag'.format(self.LOCAL)
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
                freshTagFetch,
                freshTagRead
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
            freshTagRead = item['freshTagRead']
            freshTagFetch = item['freshTagFetch']

            totalRead += freshTagRead
            totalFetch += freshTagFetch

            validTags = ['JOKE', 'VIDEO', 'FUNNY', 'RELATIVENEWS']
            if all(tag not in categoryTag.upper() for tag in validTags):
                validRead += freshTagRead
                validFetch += freshTagFetch

            if 'VIDEO' in categoryTag.upper():
                videoRead += freshTagRead
                videoFetch += freshTagFetch

            partname = categoryTag.split(':')[0]
            partreads.update({
                partname: (partreads.get(partname, 0) + freshTagRead)
            })
            partfetches.update({
                partname: (partfetches.get(partname, 0) + freshTagFetch)
            })

        for item in res:
            categoryTag = item['categoryTag']
            freshTagRead = item['freshTagRead']
            freshTagFetch = item['freshTagFetch']

            partname = categoryTag.split(':')[0]
            try:
                percent = '{:.3%}'.format(
                        freshTagFetch / partfetches[partname])
            except ZeroDivisionError:
                percent = '0.00%'

            try:
                ctr = '{:.3%}'.format(freshTagRead / freshTagFetch)
            except ZeroDivisionError:
                ctr = '0.00%'

            ret.update({
                categoryTag: {
                    'freshTagRead': freshTagRead,
                    'freshTagFetch': freshTagFetch,
                    'percent': percent,
                    'ctr': ctr
                }
            })

        try:
            video_ctr = '{:.3%}'.format(videoRead / videoFetch)
        except ZeroDivisionError:
            video_ctr = '0.00%'

        ret.update({
            'Video': {
                'freshTagFetch': videoFetch,
                'freshTagRead': videoRead,
                'percent': '==',
                'ctr': video_ctr,
            }
        })

        try:
            total_ctr = '{:.3%}'.format(totalRead / totalFetch)
        except ZeroDivisionError:
            total_ctr = '0.00%'

        ret.update({
            'Total': {
                'freshTagFetch': totalFetch,
                'freshTagRead': totalRead,
                'percent': '==',
                'ctr': total_ctr
            }
        })

        try:
            valid_ctr = '{:.3%}'.format(validRead / validFetch)
        except ZeroDivisionError:
            valid_ctr = '0.00%'

        ret.update({
            'Valid': {
                'freshTagFetch': validFetch,
                'freshTagRead': validRead,
                'percent': '==',
                'ctr': valid_ctr
            }
        })

        for partname in partfetches.keys():

            try:
                part_ctr = '{:.3%}'.format(
                        partreads[partname] / partfetches[partname])
            except ZeroDivisionError:
                part_ctr = '0.00%'

            ret.update({
                partname: {
                    'freshTagFetch': partfetches[partname],
                    'freshTagRead': partreads[partname],
                    'percent': '==',
                    'ctr': part_ctr
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
