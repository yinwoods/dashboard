import os
import time
import datetime
import pymysql
import subprocess
from dashboard.config import SCP_COMMAND
from dashboard.config import ME_TARGET_POS
from dashboard.config import DATABASE
from dashboard.config import MELOCAL


class Operation():

    def __init__(self):
        self.conn = pymysql.connect(
                host='localhost', user='root',
                password='root', db=DATABASE,
                charset='utf8')

        self.LOCAL = MELOCAL
        self.cursor = self.conn.cursor()
        self._date = self.getLatestDate()

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def getLatestDate(self):
        sql = 'SELECT MAX(date) FROM {}_dashboard_data'.format(self.LOCAL)
        self.cursor.execute(sql)
        self.conn.commit()
        latestDate = str(self.cursor.fetchall()[0][0])
        year = int(latestDate[0:4])
        month = int(latestDate[4:6])
        day = int(latestDate[6:8])
        date = datetime.datetime(year, month, day)
        now = date + datetime.timedelta(days=1)
        date = now.strftime('%Y%m%d')

        return date

    def scpGetFile(self):
        scpcommand = SCP_COMMAND
        targetpos = ME_TARGET_POS.format(self._date)
        print('try to get {}'.format(targetpos))
        destpos = './{country}-{date}'.format(
                country=self.LOCAL, date=self._date)
        command = "{scpcommand}:{targetpos} {destpos}".format(
                scpcommand=scpcommand,
                targetpos=targetpos,
                destpos=destpos)
        print(command)
        state = subprocess.call(command, shell=True)
        if state:
            print('File does not exist yet, wait 20 hours')
            time.sleep(72000)
        else:
            print('start inserting.')

    def getAllDashboardFiles(self, basepath):
        # get all the dashboard data files' postion
        for dir in os.listdir(basepath):
            dir = os.path.join(basepath, dir)

            if os.path.isdir(dir) and dir.endswith(
                    '{0}-{1}'.format(self.LOCAL, self._date)):
                print(dir)
                for file in os.listdir(dir):
                    if file.startswith('.'):
                        try:
                            os.remove(os.path.join(dir, file))
                        except Exception as e1:
                            print(os.path.join(dir, file))
                            os.removedirs(os.path.join(dir, file))
                        except Exception as e2:
                            print(e2)
                    else:
                        yield os.path.join(dir, file)

    def writeInfo(self, sql):
        # write data into mysql db
        try:
            res = self.cursor.execute(sql)
            self.conn.commit()
            return res
        except Exception as e:
            print(e)

    def formatSQL(self, sql):
        try:
            sql[0] = int(sql[0])
        except Exception:
            sql[0] = 0
        try:
            sql[1] = int(sql[1])
        except Exception:
            sql[1] = 0

        sql[2:4] = map(str, sql[2:4])

        try:
            sql[4] = int(sql[4])
        except Exception:
            sql[4] = 0
        try:
            sql[5] = int(sql[5])
        except Exception:
            sql[5] = 0
        try:
            sql[6] = int(sql[6])
        except Exception:
            sql[6] = 0
        try:
            sql[7] = int(sql[7])
        except Exception:
            sql[7] = -99
        try:
            sql[8] = int(sql[8])
        except Exception:
            sql[8] = 0
        try:
            sql[9] = int(sql[9])
        except Exception:
            sql[9] = 0

        return tuple(sql)

    def getInfoId(self):
        # get current max info Id from mysql db
        sql = "SELECT MAX(infoid) FROM {}_dashboard_data".format(self.LOCAL)
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            res = self.cursor.fetchall()
            res = res[0][0]
            if res is None:
                res = -1
            return res
        except Exception as e:
            print(e)

    def markDeletableFile(self, filepos):
        with open('.{}_to_delete'.format(self.LOCAL), 'a+') as f:
            f.write(str(filepos) + '\n')

    def deleteDeletableFile(self, basepath):
        with open('.{}_to_delete'.format(self.LOCAL), 'r') as f:
            for line in f.readlines():
                try:
                    os.remove(line.strip())
                except Exception as e:
                    print(e)

        # delete empty dir
        for dir in os.listdir(basepath):
            if os.path.isdir(dir) and not os.listdir(dir) and\
             dir.endswith('{0}-{1}'.format(self.LOCAL, self._date)):
                try:
                    os.rmdir(dir)
                except Exception as e:
                    print(e)

        with open('.{}_to_delete'.format(self.LOCAL), 'w') as f:
            f.write('')

    def run(self):
        infoid = self.getInfoId()
        basepath = os.getcwd()
        files = self.getAllDashboardFiles(basepath)
        for file in files:
            date = str(self._date)
            with open(file, 'r') as f:
                for line in f.readlines():
                    infoid += 1

                    sql = ''
                    try:
                        sql = '''
                            INSERT INTO {}_dashboard_data(infoid, date, tag,
                            newstype, mediaid, categoryid, requestcategoryid,
                            click_index, presents, clicked) VALUES {}
                        '''.format(self.LOCAL, self.formatSQL(
                            [infoid, date] + line.strip().split('\t')))
                    except Exception as e:
                        print(e)
                        return

                    self.writeInfo(sql)
            self.markDeletableFile(file)

        self.deleteDeletableFile(basepath)


while True:
    op = Operation()
    op.scpGetFile()
    op.run()
