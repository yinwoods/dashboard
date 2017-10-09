import ast
import subprocess
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from dashboard.config import DBSTRING
from dashboard.config import TINY_WORK_HOST
from dashboard.config import TINY_WORK_USERNAME


engine = create_engine(DBSTRING)
conn = engine.connect()


def main():
    """
    获取集群上的文件到本地并插入数据库
    """
    user = TINY_WORK_USERNAME
    host = TINY_WORK_HOST
    for country in ['id', 'br']:
        position = ('/home/yinwoods/tiny_work/banner_ctr/'
                    '{}_news.result').format(country)

        command = "scp {user}@{host}:{position} ./".format(
                        user=user,
                        host=host,
                        position=position)
        print('getting latest result')
        subprocess.call(command, shell=True)

        with open('{}_news.result'.format(country), 'r') as f:
            for line in f:
                # line = line.replace("'", '"')
                line = ast.literal_eval(line)

                news_id = line.get('news_id')
                start_time = line.get('start_time')
                end_time = line.get('end_time')
                impression_cnt = line.get('impression_cnt')
                click_cnt = line.get('click_cnt')
                page_id = line.get('page_id')
                title = line.get('title')

                sql = '''
                    SELECT
                        count(*)
                    FROM
                        daily_report.{}_bannerCtr
                    WHERE
                        page_id = '{}'
                '''.format(country, page_id)

                try:
                    cnt = conn.execute(text(sql)).fetchone()[0]
                except Exception as e:
                    print(e)
                    cnt = 0

                print(page_id, cnt)
                if cnt == 0:
                    sql = '''
                        INSERT INTO
                            daily_report.{0}_bannerCtr(
                                news_id, start_time, end_time,
                                impression_cnt, click_cnt, page_id, title)
                            VALUES(
                                {1!r}, {2!r}, {3!r}, {4}, {5}, {6!r}, {7!r})
                    '''

                    sql = sql.format(
                        country, news_id, start_time, end_time,
                        impression_cnt, click_cnt, page_id, title
                    )
                    try:
                        conn.execute(text(sql))
                    except Exception as e:
                        print(e)
                    print('insert new record with news_id: {}'.format(news_id))


if __name__ == '__main__':
    main()
