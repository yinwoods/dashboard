from dashboard.model.DB import DB
from dashboard.config import DBCONFIG

DATABASE = 'daily_report'


def writeNewsCtrByType(logDate, categoryName, readCount, fetchCount, ctr):
    try:
        sqlstring = '''
        INSERT INTO
            {db}.{table}(logDate, categoryName, readCount, fetchCount, ctr)
        VALUES
            ({logDate}, '{categoryName}', {readCount}, {fetchCount}, {ctr})
        '''.format(
                db=DATABASE,
                table='id_ctr_news',
                logDate=logDate,
                categoryName=categoryName,
                readCount=readCount,
                fetchCount=fetchCount,
                ctr=ctr
            )
        data = DB(**DBCONFIG).insert(sqlstring)
        return data
    except Exception as e:
        print(e, sqlstring)
        return False


def readNewsCtrByType(start, length):
    try:
        sqlstring = '''
        SELECt
            *
        FROM
            {db}.{table}
        LIMIT
            {start}, {length}
        '''.format(
                db=DATABASE,
                table='id_ctr_news',
                start=start,
                length=length
            )
        data = DB(**DBCONFIG).query(sqlstring)
        return data
    except Exception as e:
        print(e, sqlstring)
        return False
