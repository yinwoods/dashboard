#coding: utf-8
import pytz
import time
import pymssql
import datetime
from dashboard.model.DB import DB
from dashboard.config import DBCONFIG
from dashboard.config import mssqlconfig_online_id


def getSQLServerNewInfo(maxCreatedTime):
    sql = "SELECT COUNT(NewsId) FROM News WHERE CAST(CreatedTime AS DATETIMEOFFSET) > CAST('{}' AS DATETIMEOFFSET)".format(maxCreatedTime)
    print(sql)
    data = ''
    try:
        with pymssql.connect(**mssqlconfig_online_id) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                data = cursor.fetchall()
                return data[0][0]
    except Exception as e:
        print(e)

def getMySQLMaxCreatedTime():
    sqlstring = 'SELECT MAX(newsCreatedTime) as maxCreatedTime FROM daily_report.id_crawlFrequency'
    data = DB(**DBCONFIG).query(sqlstring)
    return data[0]['maxCreatedTime']

def transforDataFromSQLServerToMysql():

    utc = pytz.utc

    maxDateTime = getMySQLMaxCreatedTime()
    print('mysql most recent time : ', maxDateTime)
    totalSQLServerCnt = getSQLServerNewInfo(maxDateTime)

    print('sql server total info: ' + str(totalSQLServerCnt))

    sql = '''
        SELECT
            NewsId,
            Type,
            DATEDIFF(s, '1970-01-01 00:00:00', CreatedTime) AS time
        FROM
            News
        WHERE
            CAST( CreatedTime AS DATETIMEOFFSET) > CAST('{}' AS DATETIMEOFFSET)
    '''.format(maxDateTime)
    datas = ''
    with pymssql.connect(**mssqlconfig_online_id) as conn:
        with conn.cursor(as_dict=True) as cursor:
            cursor.execute(sql)
            datas = cursor.fetchall()

    for item in datas:
        newsId = item['NewsId']
        newsCreatedTime = item['time']
        newsType = item['Type']
        newsCreatedTime= datetime.datetime.utcfromtimestamp(int(newsCreatedTime)).replace(tzinfo=utc)
         # 如果newsCreatedTIme 比当前大于等于当前时间则跳过（说明是问题数据）
        now = datetime.datetime.utcfromtimestamp(time.time()).replace(tzinfo=utc)
        if newsCreatedTime >= now:
            continue
        newsCreatedTime = newsCreatedTime.strftime('%Y-%m-%d %H:%M:%S')
        sql = ('INSERT INTO daily_report.id_crawlFrequency('
               'newsId, newsCreatedTime, newsType) VALUES ('
               '{}, str_to_date("{}", "%Y-%m-%d %H:%i:%s"), {})').format(
                   newsId, newsCreatedTime, newsType)
        try:
            data = DB(**DBCONFIG).insert(sql)
        except Exception as e:
            print(e)

print(datetime.datetime.now())
transforDataFromSQLServerToMysql()
