from dashboard.config import mssqlconfig_online_id
from dashboard.config import mssqlconfig_online_br
import pymssql


def query(sql, local):
    if local == 'id':
        try:
            with pymssql.connect(**mssqlconfig_online_id) as conn:
                with conn.cursor(as_dict=True) as cursor:
                    res = cursor.execute(sql)
                    res = cursor.fetchall()
            return res
        except Exception as e:
            print(str(e))
            return False
    elif local == 'br':
        try:
            with pymssql.connect(**mssqlconfig_online_br) as conn:
                with conn.cursor(as_dict=True) as cursor:
                    res = cursor.execute(sql)
                    res = cursor.fetchall()
            return res
        except Exception as e:
            print(str(e))
            return False
    else:
        raise Exception("need id or br as local parameter")


if __name__ == '__main__':
    sql = '''
        SELECT
            *
        FROM
            Categories
    '''
    print(query(sql, 'id'))
