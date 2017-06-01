# coding: utf-8
import arrow
import asyncio
from dashboard.model.DB import DB
from dashboard.config import DBCONFIG
from dashboard.config import cache


async def main():

    now = arrow.utcnow()
    now = now.replace(seconds=-now.second)
    while True:
        now = arrow.utcnow()
        date = now.format('YYYYMMDD')
        real_time = now.format('HHmmss')

        pagequeue_length = cache.llen('pagequeue')
        doingsqueue_length = cache.llen('doingsqueue')
        rssqueue_length = cache.llen('rssqueue')
        videoimagequeue_length = cache.llen('videoimagequeue')
        writedbnewsqueue_length = cache.llen('writedbnewsqueue')
        videorssqueue_length = cache.llen('videorssqueue')
        print(pagequeue_length, doingsqueue_length, rssqueue_length,
              videoimagequeue_length, writedbnewsqueue_length,
              videorssqueue_length)

        sql = '''
        INSERT INTO daily_report.id_crawlStatus(
            date, time, pagequeue_length,
            doingsqueue_length, rssqueue_length,
            videoimagequeue_length, writedbnewsqueue_length,
            videorssqueue_length
        ) VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7})
        '''.format(
            date,
            real_time,
            pagequeue_length,
            doingsqueue_length,
            rssqueue_length,
            videoimagequeue_length,
            writedbnewsqueue_length,
            videorssqueue_length
        )

        DB(**DBCONFIG).insert(sql)

        sql = '''
            DELETE FROM daily_report.id_crawlStatus
            WHERE date = {date} AND time = {time}
            '''.format(
                date=int(date)-1,
                time=real_time
            )

        DB(**DBCONFIG).query(sql)

        await asyncio.sleep(1)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
