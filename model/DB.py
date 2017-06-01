#  -*- encoding:utf8 -*-
'''
@author: dove
@version: 2014-06-10

封装的mysql常用函数
'''

import pymysql
import time


class DB():
    def __init__(self, **args):
        self.DB_HOST = args.get('DB_HOST')
        self.DB_PORT = args.get('DB_PORT')
        self.DB_USER = args.get('DB_USER')
        self.DB_PWD = args.get('DB_PWD')
        self.DB_DB = args.get('DB_DB')

    def getConnection(self):
        return pymysql.Connect(
                           host=self.DB_HOST,  # 设置MYSQL地址
                           port=self.DB_PORT,  # 设置端口号
                           user=self.DB_USER,  # 设置用户名
                           passwd=self.DB_PWD,  # 设置密码
                           db=self.DB_DB,
                           charset='utf8',
                           cursorclass=pymysql.cursors.DictCursor
                       )

    def query(self, sqlString):
        num = 0
        conn = False
        while num < 50:
            try:
                conn = self.getConnection()
            except:
                time.sleep(1)
                num += 1
            else:
                break
        if conn is not False:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(sqlString)
            returnData = cursor.fetchall()
            cursor.close()
            conn.close()
            return returnData
        else:
            return False

    def queryResultCnt(self, sqlString):
        conn = self.getConnection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        resultCnt = cursor.execute(sqlString)
        cursor.close()
        conn.close()
        return resultCnt

    def delete(self, sqlString):
        conn = self.getConnection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(sqlString)
        conn.commit()
        cursor.close()
        conn.close()
        # return returnData

    def update(self, sqlString, dt=None):
        num = 0
        conn = False
        while num < 50:
            try:
                conn = self.getConnection()
            except:
                time.sleep(1)
                num += 1
            else:
                break
        if conn is not False:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            if dt:
                cursor.execute(sqlString, dt)
            else:
                cursor.execute(sqlString)
            conn.commit()
            cursor.close()
            conn.close()
        else:
            return False

    def insert(self, sqlString, dt=None):
        num = 0
        conn = False
        while num < 50:
            try:
                conn = self.getConnection()
            except:
                time.sleep(1)
                num += 1
            else:
                break
        if conn is not False:
            cursor = conn.cursor(pymysql.cursors.DictCursor)
            if dt:
                cursor.execute(sqlString, dt)
            else:
                cursor.execute(sqlString)
            conn.commit()
            aiid = cursor.lastrowid
            cursor.close()
            conn.close()
            return aiid
        else:
            return False
