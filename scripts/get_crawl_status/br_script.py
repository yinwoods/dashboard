# coding: utf-8
import time
import arrow
import redis
import pymssql
from dashboard.model.DB import DB
from dashboard.config import DBCONFIG
from dashboard.config import cache
from dashboard.config import newscache


def main():

    now = arrow.utcnow()
    now = now.replace(seconds=-now.second)
    while True:
        date = now.format('YYYYMMDD')
        real_time = now.format('HHmmss')
        print(date, real_time, sep='\t')

        pagequeue_length = cache.llen('pagequeue')
        doingsqueue_length = cache.llen('doingsqueue')
        rssqueue_length = cache.llen('rssqueue')
        print(pagequeue_length, doingsqueue_length, rssqueue_length)

        sql = '''
        INSERT INTO daily_report.id_crawlStatus(
            date, time, pagequeue_length,
            doingsqueue_length, rssqueue_length
        ) VALUES ({0}, {1}, {2}, {3}, {4})
        '''.format(
            date,
            real_time,
            pagequeue_length,
            doingsqueue_length,
            rssqueue_length
        )

        DB(**DBCONFIG).insert(sql)


        now = now.replace(seconds=+5)
        time.sleep(5)


if __name__ == '__main__':
    main()
