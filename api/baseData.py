import re
import json
import arrow
import asyncio
from dashboard.model import mssql
from dashboard.model.DB import DB
from dashboard.config import DBCONFIG
from dashboard.config import IDLOCAL
from dashboard.config import BRLOCAL
from dashboard.config import MELOCAL
from dashboard.config import DATABASE
from dashboard.handler.basehandler import BaseHandler


class IDBaseDataQueryHandler(BaseHandler):

    LOCAL = IDLOCAL

    async def getQueryData(self, parameters):

        start_date = parameters.get('start_date')
        end_date = parameters.get('end_date')

        click_index = parameters.get('click_index')
        tag = parameters.get('tag')
        requestcategoryid = parameters.get('requestcategoryid')
        newstype = parameters.get('newstype')
        mediaid = parameters.get('mediaid')

        print(click_index, tag, requestcategoryid,
              newstype, mediaid)

        categoryid = parameters.get('categoryid')
        with_relative = parameters.get('with_relative')
        gb = parameters.get('gb')
        medias = dict()
        sql = 'SELECT * FROM Media'

        res = mssql.query(sql, IDLOCAL)
        for dt in res:
            medias[str(dt['Id'])] = dt['Name']

        for i, column in enumerate(gb):
            if column == 'date':
                gb[i] = 'left(date, 8)'
        if start_date == '':
            self.finish(json.dumps([]))
        else:

            sfrom = 'FROM {}.{}_dashboard_data'.format(DATABASE, self.LOCAL)
            select = ('CAST(SUM(presents) AS UNSIGNED) AS present,'
                      'CAST(SUM(clicked) AS UNSIGNED) AS click ')
            where = 'WHERE date<=%s and date>=%s' % (end_date, start_date)

            for index, item in enumerate(gb):
                if item == 'clickindex':
                    gb[index] = 'click_index'

            gb = ','.join(gb)
            groupby = '%s ' % gb
            if len(with_relative) > 0 and with_relative[0] == '1':
                where += ' and tag like "RelativeNews:%"'
            elif len(with_relative) > 0 and with_relative[0] == '-1':
                where += ' and tag not like "RelativeNews:%"'

            for vari in ['click_index', 'tag', 'requestcategoryid',
                         'newstype', 'mediaid', 'categoryid']:
                if (eval(vari) is not None
                    and len(eval(vari)) != 0
                        and eval(vari)[0] != ''):

                    where += ' and  %s in (%s)' % (vari, ','.join(
                        ['"%s"' % i for i in eval(vari)[0].split(',')]))

            if gb != '':
                select += ', %s' % gb

            if groupby.strip() != '':
                groupby = 'GROUP BY ' + groupby
            sql = 'SELECT %s  %s %s  %s' % (
                select, sfrom, where, groupby)

            data = DB(**DBCONFIG).query(sql)
            res = []

            if len(data) > 0 and data[0].get('categoryid') is not None:
                sql = '''
                    SELECT
                        CategoryId, Name
                    FROM
                        Categories
                '''.format(categoryid)
                category_id_name = mssql.query(sql, 'id')
                category_id_name_dict = ({
                    item['CategoryId']: item['Name']
                    for item in category_id_name})
                for item in data:
                    item.update({'CategoryName': category_id_name_dict.get(
                        int(item.get('categoryid')))})

            for dts in data:
                if "mediaid" in gb:
                    mid = dts['mediaid']
                    dts['mediaid'] = '%s-%s' % (
                            mid, medias.get(str(mid), 'not know'))
                if dts['present'] == 0:
                    dts['ctr'] = 0
                else:
                    dts['ctr'] = '{0:%}'.format(
                            dts.get('click', 0) / dts.get('present', 1))

                for key in ['date', 'left(date, 8)',
                            'right(date, 2)', 'click_index',
                            'tag', 'requestcategoryid',
                            'newstype', 'mediaid', 'categoryid']:
                    dts.setdefault(key, '0')

                res.append(dts)
            return res

    async def getData(self, parameters):
        data = []

        tasks = [
            self.getQueryData(parameters)
        ]

        for task in asyncio.as_completed(tasks):
            data += await task

        return data

    def post(self):

        errid = 0
        errmsg = 'SUCCESS'
        data = None

        daterange = self.get_argument('daterange', '')
        start_date, end_date = daterange.split(' - ')

        start_date = re.sub('-| ', '', start_date[:11])
        end_date = re.sub('-| ', '', end_date[:11])
        click_index = self.get_arguments('clickindex')
        tag = self.get_arguments('tag')
        requestcategoryid = self.get_arguments('requestcategoryid')
        newstype = self.get_arguments('newstype')
        mediaid = self.get_arguments('mediaid')
        categoryid = self.get_arguments('categoryid')
        gb = self.get_arguments('groupby')
        with_relative = self.get_arguments('with_relative')

        parameters = dict(
                start_date=start_date,
                end_date=end_date,
                click_index=click_index,
                tag=tag,
                requestcategoryid=requestcategoryid,
                newstype=newstype,
                mediaid=mediaid,
                categoryid=categoryid,
                gb=gb,
                with_relative=with_relative,
        )

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
                    self.getData(parameters))
        except Exception as e:
            errid = -3
            errmsg = str(e)

        res = dict()
        res['errid'] = errid
        res['errmsg'] = errmsg
        res['data'] = data
        self.write(json.dumps(res))


class BRBaseDataQueryHandler(BaseHandler):

    LOCAL = BRLOCAL

    async def getQueryData(self, parameters):

        start_date = parameters.get('start_date')
        end_date = parameters.get('end_date')

        click_index = parameters.get('click_index')
        tag = parameters.get('tag')
        requestcategoryid = parameters.get('requestcategoryid')
        newstype = parameters.get('newstype')
        mediaid = parameters.get('mediaid')

        print(click_index, tag, requestcategoryid,
              newstype, mediaid)

        categoryid = parameters.get('categoryid')
        with_relative = parameters.get('with_relative')
        gb = parameters.get('gb')
        medias = dict()
        sql = 'SELECT * FROM Media'

        res = mssql.query(sql, BRLOCAL)
        for dt in res:
            medias[str(dt['Id'])] = dt['Name']

        for i, column in enumerate(gb):
            if column == 'date':
                gb[i] = 'left(date, 8)'
        if start_date == '':
            self.finish(json.dumps([]))
        else:

            sfrom = 'FROM {}.{}_dashboard_data'.format(DATABASE, self.LOCAL)
            select = ('CAST(SUM(presents) AS UNSIGNED) AS present,'
                      'CAST(SUM(clicked) AS UNSIGNED) AS click ')
            where = 'WHERE date<=%s and date>=%s' % (end_date, start_date)

            for index, item in enumerate(gb):
                if item == 'clickindex':
                    gb[index] = 'click_index'

            gb = ','.join(gb)
            groupby = '%s ' % gb
            if len(with_relative) > 0 and with_relative[0] == '1':
                where += ' and tag like "RelativeNews:%"'
            elif len(with_relative) > 0 and with_relative[0] == '-1':
                where += ' and tag not like "RelativeNews:%"'

            for vari in ['click_index', 'tag', 'requestcategoryid',
                         'newstype', 'mediaid', 'categoryid']:
                if (eval(vari) is not None
                    and len(eval(vari)) != 0
                        and eval(vari)[0] != ''):

                    where += ' and  %s in (%s)' % (vari, ','.join(
                        ['"%s"' % i for i in eval(vari)[0].split(',')]))

            if gb != '':
                select += ', %s' % gb

            if groupby.strip() != '':
                groupby = 'GROUP BY ' + groupby
            sql = 'SELECT %s  %s %s  %s' % (select, sfrom, where, groupby)

            data = DB(**DBCONFIG).query(sql)
            res = []

            if len(data) > 0 and data[0].get('categoryid') is not None:
                sql = '''
                    SELECT
                        CategoryId, Name
                    FROM
                        Categories
                '''.format(categoryid)
                category_id_name = mssql.query(sql, 'id')
                category_id_name_dict = ({
                    item['CategoryId']: item['Name']
                    for item in category_id_name})
                for item in data:
                    item.update({'CategoryName': category_id_name_dict.get(
                        int(item.get('categoryid')))})

            for dts in data:
                if "mediaid" in gb:
                    mid = dts['mediaid']
                    dts['mediaid'] = '%s-%s' % (
                            mid, medias.get(str(mid), 'not know'))
                if dts['present'] == 0:
                    dts['ctr'] = 0
                else:
                    dts['ctr'] = '{0:%}'.format(dts['click'] / dts['present'])

                for key in ['date', 'left(date, 8)',
                            'right(date, 2)', 'click_index',
                            'tag', 'requestcategoryid',
                            'newstype', 'mediaid', 'categoryid']:
                    dts.setdefault(key, '0')

                res.append(dts)
            return res

    async def getData(self, parameters):
        data = []

        tasks = [
            self.getQueryData(parameters)
        ]

        for task in asyncio.as_completed(tasks):
            data += await task

        return data

    def post(self):

        errid = 0
        errmsg = 'SUCCESS'
        data = None

        daterange = self.get_argument('daterange', '')
        start_date, end_date = daterange.split(' - ')

        start_date = re.sub('-| ', '', start_date[:11])
        end_date = re.sub('-| ', '', end_date[:11])
        click_index = self.get_arguments('clickindex')
        tag = self.get_arguments('tag')
        requestcategoryid = self.get_arguments('requestcategoryid')
        newstype = self.get_arguments('newstype')
        mediaid = self.get_arguments('mediaid')
        categoryid = self.get_arguments('categoryid')
        gb = self.get_arguments('groupby')
        with_relative = self.get_arguments('with_relative')

        parameters = dict(
                start_date=start_date,
                end_date=end_date,
                click_index=click_index,
                tag=tag,
                requestcategoryid=requestcategoryid,
                newstype=newstype,
                mediaid=mediaid,
                categoryid=categoryid,
                gb=gb,
                with_relative=with_relative
        )

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
                    self.getData(parameters))
        except Exception as e:
            errid = -3
            errmsg = str(e)

        res = dict()
        res['errid'] = errid
        res['errmsg'] = errmsg
        res['data'] = data
        self.write(json.dumps(res))


class MEBaseDataQueryHandler(BaseHandler):

    LOCAL = MELOCAL

    async def getQueryData(self, parameters):

        start_date = parameters.get('start_date')
        end_date = parameters.get('end_date')

        click_index = parameters.get('click_index')
        tag = parameters.get('tag')
        requestcategoryid = parameters.get('requestcategoryid')
        newstype = parameters.get('newstype')
        mediaid = parameters.get('mediaid')

        print(click_index, tag, requestcategoryid,
              newstype, mediaid)

        categoryid = parameters.get('categoryid')
        gb = parameters.get('gb')
        with_relative = parameters.get('with_relative')
        medias = dict()
        sql = 'SELECT * FROM Media'

        res = mssql.query(sql, MELOCAL)
        for dt in res:
            medias[str(dt['Id'])] = dt['Name']

        for i, column in enumerate(gb):
            if column == 'date':
                gb[i] = 'left(date, 8)'
        if start_date == '':
            self.finish(json.dumps([]))
        else:

            sfrom = 'FROM {}.{}_dashboard_data'.format(DATABASE, self.LOCAL)
            select = ('CAST(SUM(presents) AS UNSIGNED) AS present,'
                      'CAST(SUM(clicked) AS UNSIGNED) AS click ')
            where = 'WHERE date<=%s and date>=%s' % (end_date, start_date)

            for index, item in enumerate(gb):
                if item == 'clickindex':
                    gb[index] = 'click_index'

            gb = ','.join(gb)
            groupby = '%s ' % gb
            if len(with_relative) > 0 and with_relative[0] == '1':
                where += ' and tag like "RelativeNews:%"'
            elif len(with_relative) > 0 and with_relative[0] == '-1':
                where += ' and tag not like "RelativeNews:%"'

            for vari in ['click_index', 'tag', 'requestcategoryid',
                         'newstype', 'mediaid', 'categoryid']:
                if (eval(vari) is not None
                    and len(eval(vari)) != 0
                        and eval(vari)[0] != ''):

                    where += ' and  %s in (%s)' % (vari, ','.join(
                        ['"%s"' % i for i in eval(vari)[0].split(',')]))

            if gb != '':
                select += ', %s' % gb

            if groupby.strip() != '':
                groupby = 'GROUP BY ' + groupby
            sql = 'SELECT %s  %s %s  %s' % (select, sfrom, where, groupby)

            data = DB(**DBCONFIG).query(sql)
            res = []

            if len(data) > 0 and data[0].get('categoryid') is not None:
                sql = '''
                    SELECT
                        CategoryId, Name
                    FROM
                        Categories
                '''.format(categoryid)
                category_id_name = mssql.query(sql, 'id')
                category_id_name_dict = ({
                    item['CategoryId']: item['Name']
                    for item in category_id_name})
                for item in data:
                    item.update({'CategoryName': category_id_name_dict.get(
                        int(item.get('categoryid')))})

            for dts in data:
                if "mediaid" in gb:
                    mid = dts['mediaid']
                    dts['mediaid'] = '%s-%s' % (
                            mid, medias.get(str(mid), 'not know'))
                if dts['present'] == 0:
                    dts['ctr'] = 0
                else:
                    dts['ctr'] = '{0:%}'.format(dts['click'] / dts['present'])

                for key in ['date', 'left(date, 8)',
                            'right(date, 2)', 'click_index',
                            'tag', 'requestcategoryid',
                            'newstype', 'mediaid', 'categoryid']:
                    dts.setdefault(key, '0')

                res.append(dts)
            return res

    async def getData(self, parameters):
        data = []

        tasks = [
            self.getQueryData(parameters)
        ]

        for task in asyncio.as_completed(tasks):
            data += await task

        return data

    def post(self):

        errid = 0
        errmsg = 'SUCCESS'
        data = None

        daterange = self.get_argument('daterange', '')
        start_date, end_date = daterange.split(' - ')

        start_date = re.sub('-| ', '', start_date[:11])
        end_date = re.sub('-| ', '', end_date[:11])
        click_index = self.get_arguments('clickindex')
        tag = self.get_arguments('tag')
        requestcategoryid = self.get_arguments('requestcategoryid')
        newstype = self.get_arguments('newstype')
        mediaid = self.get_arguments('mediaid')
        categoryid = self.get_arguments('categoryid')
        gb = self.get_arguments('groupby')
        with_relative = self.get_arguments('with_relative')

        parameters = dict(
                start_date=start_date,
                end_date=end_date,
                click_index=click_index,
                tag=tag,
                requestcategoryid=requestcategoryid,
                newstype=newstype,
                mediaid=mediaid,
                categoryid=categoryid,
                gb=gb,
                with_relative=with_relative
        )

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
                    self.getData(parameters))
        except Exception as e:
            errid = -3
            errmsg = str(e)

        res = dict()
        res['errid'] = errid
        res['errmsg'] = errmsg
        res['data'] = data
        self.write(json.dumps(res))
