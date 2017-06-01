import json
import subprocess
from dashboard.config import HOST
from dashboard.config import PASSWORD
from dashboard.config import USERNAME
from dashboard.config import DBCONFIG
from dashboard.model.DB import DB


def main():
    """
    获取集群上的文件到本地并插入数据库
    """
    user = USERNAME
    host = HOST
    for country in ['id', 'br']:
        position = ('/home/renning/tiny_work/banner_ctr/'
                    '{}_news.result').format(country)

        command = """
            sshpass -p {password} scp {user}@{host}:{position} ./
        """.format(
            user=user,
            host=host,
            password=PASSWORD,
            position=position
        )
        print('getting latest result')
        subprocess.call(command, shell=True)

        with open('{}_news.result'.format(country), 'r') as f:
            for line in f:
                line = line.replace("'", '"')
                line = json.loads(line)

                news_id = line.get('news_id')
                start_time = line.get('start_time')
                end_time = line.get('end_time')
                impression_cnt = line.get('impression_cnt')
                click_cnt = line.get('click_cnt')
                page_id = line.get('page_id')
                title = line.get('title')

                sql = '''
                    SELECT
                        *
                    FROM
                        daily_report.{}_bannerCtr
                    WHERE
                        page_id = '{}'
                '''.format(country, page_id)
                cnt = DB(**DBCONFIG).queryResultCnt(sql)
                print(page_id, cnt)
                if cnt == 0:
                    sql = '''
                        INSERT INTO
                            daily_report.{country}_bannerCtr(
                                news_id, start_time, end_time,
                                impression_cnt, click_cnt, page_id, title)
                            VALUES(
                                '{news_id}', '{start_time}', '{end_time}',
                                {impression_cnt}, {click_cnt}, '{page_id}',
                                '{title}')
                    '''

                    sql = sql.format(
                        country=country,
                        news_id=news_id,
                        start_time=start_time,
                        end_time=end_time,
                        impression_cnt=impression_cnt,
                        click_cnt=click_cnt,
                        page_id=page_id,
                        title=title
                    )
                    DB(**DBCONFIG).insert(sql)
                    print('insert new record with news_id: {}'.format(news_id))


if __name__ == '__main__':
    main()
