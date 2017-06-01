import arrow
import asyncio
from dashboard.model.DB import DB
from dashboard.model import mssql
from dashboard.config import DBCONFIG
from dashboard.config import DATABASE
from dashboard.config import BRLOCAL


LOCAL = BRLOCAL


class DailyStatistics():

    def __init__(self, date):
        self.date = date
        self.BASEPATH = '../../hive/br/'.format(LOCAL)

    def readFile(self, filename):
        filename = '{0}_{1}.log'.format(
            filename,
            str(self.date)
        )
        filePos = self.BASEPATH + filename
        with open(filePos, 'r') as f:
            for line in f:
                yield line.strip().split('\t')

    async def insert(self, **kwargs):
        table = LOCAL + '_' + kwargs['table']
        del kwargs['table']
        sql = '''
            INSERT INTO {db}.{table} ({keys}) VALUES
        '''.format(
            db=DATABASE,
            table=table,
            keys=', '.join(kwargs.keys())
        )
        for item in zip(*kwargs.values()):
            sql += str(item)
            sql += ', '

        sql = sql[:-2]
        print('start inserting')
        try:
            DB(**DBCONFIG).insert(sql)
        except Exception as e:
            print(e)

    async def getFetchesUserNews(self):
        # 活跃用户
        # 展示新闻数
        date = []
        activeUser = []
        displayedNews = []

        content = self.readFile('fetches_user_news')
        for line in content:
            date.append(self.date)
            activeUser.append(int(line[0]))
            displayedNews.append(int(line[1]))

        if len(date) == 0:
            date.append(self.date)
            activeUser.append(0)
            displayedNews.append(0)

        await self.insert(
            table='fetchesUserNews',
            date=date,
            activeUser=activeUser,
            displayedNews=displayedNews
        )

    async def getReadsUserNews(self):
        # 阅读新闻用户
        # 阅读新闻数
        date = []
        readNewsUser = []
        readNews = []

        content = self.readFile('reads_user_news')
        for line in content:
            date.append(self.date)
            readNewsUser.append(int(line[0]))
            readNews.append(int(line[1]))

        if len(date) == 0:
            date.append(self.date)
            readNewsUser.append(0)
            readNews.append(0)

        await self.insert(
            date=date,
            table='readsUserNews',
            readNewsUser=readNewsUser,
            readNews=readNews
        )

    async def getLoginUserTotal(self):
        # 登录用户
        date = []
        loginUser = []

        content = self.readFile('login_user_total')
        for line in content:
            date.append(self.date),
            loginUser.append(int(line[0]))

        if len(date) == 0:
            date.append(self.date)
            loginUser.append(0)

        await self.insert(
            date=date,
            table='loginUserTotal',
            loginUser=loginUser
        )

    async def getFreshUserCount(self):
        # 新用户数
        date = []
        freshUser = []

        content = self.readFile('freshuser_count')
        for line in content:
            date.append(self.date)
            freshUser.append(int(line[0]))

        if len(date) == 0:
            date.append(self.date)
            freshUser.append(0)

        await self.insert(
            date=date,
            table='freshUserCount',
            freshUser=freshUser
        )

    async def getFetchReadByTag(self):
        # 新闻类型-新闻展示数
        # 新闻类型-新闻阅读数
        date = []
        categoryTag = []
        categoryTagFetch = []
        categoryTagRead = []

        content = self.readFile('fetch_read_by_tag')
        for line in content:
            date.append(self.date)
            categoryTagRead.append(int(line[0]))
            categoryTagFetch.append(int(line[1]))
            categoryTag.append(line[2])

        if len(date) == 0:
            date.append(self.date)
            categoryTagRead.append(0)
            categoryTagFetch.append(0)
            categoryTag.append('')

        await self.insert(
            date=date,
            table='fetchReadByTag',
            categoryTagRead=categoryTagRead,
            categoryTagFetch=categoryTagFetch,
            categoryTag=categoryTag
        )

    async def getFreshFetchReadByTag(self):
        # 新用户-新闻展示数
        # 新用户-新闻阅读数
        date = []
        categoryTag = []
        freshTagRead = []
        freshTagFetch = []

        content = self.readFile('fresh_fetch_read_by_tag')
        for line in content:
            date.append(self.date)
            freshTagRead.append(int(line[0]))
            freshTagFetch.append(int(line[1]))
            categoryTag.append(line[2])

        if len(date) == 0:
            date.append(self.date)
            freshTagRead.append(0)
            freshTagFetch.append(0)
            categoryTag.append('')

        await self.insert(
            date=date,
            table='freshFetchReadByTag',
            categoryTag=categoryTag,
            freshTagRead=freshTagRead,
            freshTagFetch=freshTagFetch
        )

    async def getHotFetchReadByTag(self):
        # 热点新闻标签-展示数
        # 热点新闻标签-阅读数
        date = []
        categoryTag = []
        hotTagRead = []
        hotTagFetch = []

        content = self.readFile('hot_fetch_read_by_tag')
        for line in content:
            date.append(self.date)
            hotTagRead.append(int(line[0]))
            hotTagFetch.append(int(line[1]))
            categoryTag.append(line[2])

        if len(date) == 0:
            date.append(self.date)
            hotTagRead.append(0)
            hotTagFetch.append(0)
            categoryTag.append('')

        await self.insert(
            date=date,
            table='hotFetchReadByTag',
            hotTagRead=hotTagRead,
            hotTagFetch=hotTagFetch,
            categoryTag=categoryTag
        )

    async def getFetchReadByNewsType(self):
        # 新闻类别-新闻展示数
        # 新闻类别-新闻点击数
        date = []
        newsType = []
        newsTypeRead = []
        newsTypeFetch = []

        content = self.readFile('fetch_read_by_newstype')
        for line in content:
            date.append(self.date)
            newsTypeRead.append(int(line[0]))
            newsTypeFetch.append(int(line[1]))
            newsType.append(line[2])

        if len(date) == 0:
            date.append(self.date)
            newsTypeRead.append(0)
            newsTypeFetch.append(0)
            newsType.append('')

        await self.insert(
            date=date,
            table='fetchReadByNewsType',
            newsType=newsType,
            newsTypeRead=newsTypeRead,
            newsTypeFetch=newsTypeFetch
        )

    async def getFetchReadByTagIndex(self):
        # 新闻阅读数
        # 新闻展示数
        # 新闻标签
        # 新闻位置
        date = []
        readCount = []
        fetchCount = []
        tag = []
        pageIndex = []

        content = self.readFile('fetch_read_by_tag_index')
        for line in content:
            date.append(self.date)
            readCount.append(int(line[0]))
            fetchCount.append(int(line[1]))
            tag.append(line[2])
            pageIndex.append(int(line[3]))

        if len(date) == 0:
            date.append(self.date)
            readCount.append(0)
            fetchCount.append(0)
            tag.append('')
            pageIndex.append(0)

        await self.insert(
            date=date,
            table='fetchReadByTagIndex',
            readCount=readCount,
            fetchCount=fetchCount,
            tag=tag,
            pageIndex=pageIndex
        )

    async def getFetchReadByCategory(self):
        # 新闻阅读数
        # 新闻展示数
        # 新闻类型
        date = []
        readCount = []
        fetchCount = []
        categoryName = []
        categoryIdNameDict = {-1: 'Recommand'}

        sql = 'SELECT [CategoryId], [Name] FROM Categories'
        res = mssql.query(sql, LOCAL)
        for item in res:
            categoryIdNameDict.update({item['CategoryId']: item['Name']})

        content = self.readFile('fetch_read_by_category')
        for line in content:
            try:
                categoryId = int(line[2])
                if categoryId not in categoryIdNameDict.keys():
                    continue
                date.append(self.date)
                readCount.append(int(line[0]))
                fetchCount.append(int(line[1]))
                categoryName.append(categoryIdNameDict[categoryId])
            except Exception as e:
                continue

        if len(date) == 0:
            date.append(self.date)
            readCount.append(0)
            fetchCount.append(0)
            categoryName.append('')

        await self.insert(
            date=date,
            table='fetchReadByCategory',
            readCount=readCount,
            fetchCount=fetchCount,
            categoryName=categoryName
        )

    async def getPushCtrNews(self):
        # 推送新闻Id
        # 推送新闻数
        # 推送新闻阅读数
        date = []
        newsId = []
        pushNews = []
        pushNewsRead = []

        content = self.readFile('push_ctr_news')
        for line in content:
            date.append(self.date)
            newsId.append(int(line[0]))
            pushNewsRead.append(int(line[1]))
            pushNews.append(int(line[2]))

        if len(date) == 0:
            date.append(self.date)
            newsId.append(0)
            pushNewsRead.append(0)
            pushNews.append(0)

        await self.insert(
            date=date,
            table='pushCtrNews',
            newsId=newsId,
            pushNews=pushNews,
            pushNewsRead=pushNewsRead
        )

    async def getNewsPushedNewClient(self):
        # 成功推送新闻数
        date = []
        validPushCount = []
        validPushSuccess = []
        validPushClick = []

        content = self.readFile('push_arrival_click_rate')
        for line in content:
            date.append(self.date)
            validPushCount.append(int(line[0]))
            validPushSuccess.append(int(line[1]))
            validPushClick.append(int(line[2]))

        if len(date) == 0:
            date.append(self.date)
            validPushCount.append(0)
            validPushSuccess.append(0)
            validPushClick.append(0)

        await self.insert(
            date=date,
            table='pushSummary',
            validPushCount=validPushCount,
            validPushSuccess=validPushSuccess,
            validPushClick=validPushClick
        )

    async def getFreshPushRealCtr(self):
        # 新用户推送到达数
        # 新用户推送阅读数
        date = []
        freshPushCount = []
        freshPushSuccessCount = []
        freshPushRead = []

        content = self.readFile('new_users_arrival_click_rate')
        for line in content:
            date.append(self.date)
            freshPushSuccessCount.append(int(line[0]))
            freshPushCount.append(int(line[1]))
            freshPushRead.append(int(line[2]))

        if len(date) == 0:
            date.append(self.date)
            freshPushSuccessCount.append(0)
            freshPushCount.append(0)
            freshPushRead.append(0)

        await self.insert(
            date=date,
            table='freshPushRealCtr',
            freshPushSuccessCount=freshPushSuccessCount,
            freshPushCount=freshPushCount,
            freshPushRead=freshPushRead
        )

    async def getCountNewsIndex(self):
        # 新闻入口
        date = []
        countIndex = []
        newsIndex = []

        content = self.readFile('reads_news_index')
        for line in content:
            date.append(self.date)
            countIndex.append(int(line[0]))
            newsIndex.append(int(line[1]))

        if len(date) == 0:
            date.append(self.date)
            countIndex.append(0)
            newsIndex.append(1)

        await self.insert(
            date=date,
            table='countNewsIndex',
            newsIndex=newsIndex,
            countIndex=countIndex
        )

    async def getReadNewsTimeStatistics(self):
        # TODO
        date = []
        lessThan5sec = []
        moreThan5secLessThan20sec = []
        moreThan20secLessThan60sec = []
        moreThan60sec = []
        avgReadTime = []

        content = self.readFile('reads_news_time_statistics')
        line = list(content)
        try:
            date.append(self.date)
            lessThan5sec.append(int(line[0][0]))
            moreThan5secLessThan20sec.append(int(line[1][0]))
            moreThan20secLessThan60sec.append(int(line[2][0]))
            moreThan60sec.append(int(line[3][0]))
            avgReadTime.append(line[4][0])
        except Exception:
            date.append(self.date)
            lessThan5sec.append(0)
            moreThan5secLessThan20sec.append(0)
            moreThan20secLessThan60sec.append(0)
            moreThan60sec.append(0)
            avgReadTime.append(0)

        await self.insert(
            date=date,
            table='readsNewsTimeStatistics',
            lessThan5sec=lessThan5sec,
            moreThan5secLessThan20sec=moreThan5secLessThan20sec,
            moreThan20secLessThan60sec=moreThan20secLessThan60sec,
            moreThan60sec=moreThan60sec,
            avgReadTime=avgReadTime
        )

    async def getFetchNewsCountWithoutRelative(self):
        # TODO
        date = []
        displayNewsWithoutRelative = []

        content = self.readFile('fetches_news_without_relative')
        for line in content:
            date.append(self.date)
            displayNewsWithoutRelative.append(int(line[0]))

        if len(date) == 0:
            date.append(self.date)
            displayNewsWithoutRelative.append(0)

        await self.insert(
            date=date,
            table='fetchNewsCountWithoutRelative',
            displayNewsWithoutRelative=displayNewsWithoutRelative
        )

    async def getPullCount(self):
        # TODO
        date = []
        pullNewsCount = []

        content = self.readFile('pull_total')
        for line in content:
            date.append(self.date)
            pullNewsCount.append(int(line[0]))

        if len(date) == 0:
            date.append(self.date)
            pullNewsCount.append(0)

        await self.insert(
            date=date,
            table='pullCount',
            pullNewsCount=pullNewsCount
        )

    async def getReadNewsCountWithoutRelative(self):
        # TODO
        date = []
        readNewsWithoutRelative = []

        content = self.readFile('read_news_without_relative')
        for line in content:
            date.append(self.date)
            readNewsWithoutRelative.append(int(line[0]))

        if len(date) == 0:
            date.append(self.date)
            readNewsWithoutRelative.append(0)

        await self.insert(
            date=date,
            table='readNewsCountWithoutRelative',
            readNewsWithoutRelative=readNewsWithoutRelative
        )

    async def getTrendingCtr(self):
        # TODO
        date = []
        click = []
        impression = []

        content = self.readFile('ctr_trending')
        for line in content:
            date.append(self.date)
            click.append(int(line[1]))
            impression.append(int(line[2]))

        if len(date) == 0:
            date.append(self.date)
            click.append(0)
            impression.append(0)

        await self.insert(
            date=date,
            table='trendingCtr',
            click=click,
            impression=impression
        )

    async def getPushCtrNewsHour(self):
        # TODO
        date = []
        newsId = []
        hour = []
        readCount = []
        pushCount = []

        content = self.readFile('push_ctr_news_hour')
        for line in content:
            date.append(self.date)
            newsId.append(int(line[0]))
            hour.append(line[1])
            readCount.append(int(line[2]))
            pushCount.append(int(line[3]))

        if len(date) == 0:
            date.append(self.date)
            newsId.append(0)
            hour.append('')
            readCount.append(0)
            pushCount.append(0)

        await self.insert(
            date=date,
            table='pushCtrNewsHour',
            newsId=newsId,
            hour=hour,
            readCount=readCount,
            pushCount=pushCount
        )

    async def getFetchReadByTagHour(self):
        # TODO
        date = []
        readCount = []
        fetchCount = []
        tag = []
        hour = []

        content = self.readFile('fetch_read_by_tag_hour')
        for line in content:
            date.append(self.date)
            readCount.append(int(line[0]))
            fetchCount.append(int(line[1]))
            tag.append(line[2])
            hour.append(line[3])

        if len(date) == 0:
            date.append(self.date)
            readCount.append(0)
            fetchCount.append(0)
            tag.append('')
            hour.append('')

        await self.insert(
            date=date,
            table='fetchReadByTagHour',
            readCount=readCount,
            fetchCount=fetchCount,
            tag=tag,
            hour=hour
        )

    async def getCtrNews(self):
        # TODO
        date = []
        newsId = []
        readCount = []
        fetchCount = []

        content = self.readFile('ctr_news')
        for line in content:
            date.append(self.date)
            newsId.append(int(line[0]))
            readCount.append(int(line[1]))
            fetchCount.append(int(line[2]))

        if len(date) == 0:
            date.append(self.date)
            newsId.append(0)
            readCount.append(0)
            fetchCount.append(0)

        await self.insert(
            date=date,
            table='ctrNews',
            newsId=newsId,
            readCount=readCount,
            fetchCount=fetchCount
        )

    async def getCommentUser(self):
        # 评论用户
        # 评论用户比例
        date = []
        commentUser = []
        totalComments = []
        commentsDict = dict()

        start_time = arrow.get(
                str(self.date) + ' 03:00:00+00:00',
                'YYYYMMDD HH:mm:ss')
        end_time = start_time.replace(days=+1)

        sql = '''
            SELECT
                [UserId],
                count(*) AS Count
            FROM
                Comments
            WHERE
                [Timestamp]>='{start}'
            AND
                [Timestamp]<'{end}'
            GROUP BY [UserId]
        '''.format(
                start=start_time,
                end=end_time
            )

        res = mssql.query(sql, LOCAL)
        for item in res:
            commentsDict.update({item['UserId']: item['Count']})

        date.append(self.date)
        commentUser.append(len(commentsDict))
        totalComments.append(sum(commentsDict.values()))

        await self.insert(
            date=date,
            table='commentUser',
            commentUser=commentUser,
            totalComments=totalComments
        )

    async def getCrawledNews(self):
        # 抓取新闻数
        # 含相关新闻的抓取新闻数
        date = []
        crawledNews = []
        crawledNewsWithRelative = []

        start_time = arrow.get(
                str(self.date) + ' 03:00:00+00:00',
                'YYYYMMDD HH:mm:ss')
        end_time = start_time.replace(days=+1)

        sql = '''
            SELECT
                count(*) AS crawledNews
            FROM
                News
            WHERE
                [CreatedTime]>='{start}'
            AND
                [CreatedTime]<'{end}';
        '''.format(
                start=start_time,
                end=end_time
            )

        res = mssql.query(sql, LOCAL)
        for item in res:
            crawledNews.append(int(item['crawledNews']))

        sql = '''
            SELECT
                count(distinct(RelativeNews.LeftNewsId))
            AS
                crawledNewsWithRelative
            FROM
                RelativeNews
            JOIN
                News
            ON
                RelativeNews.LeftNewsId=News.NewsId
            WHERE
                News.CreatedTime>='{start}'
            AND
                News.CreatedTime<'{end}'
        '''.format(
                start=start_time,
                end=end_time)
        res = mssql.query(sql, LOCAL)
        for item in res:
            date.append(self.date)
            crawledNewsWithRelative.append(
                    int(item['crawledNewsWithRelative']))

        await self.insert(
            date=date,
            table='crawledNews',
            crawledNews=crawledNews,
            crawledNewsWithRelative=crawledNewsWithRelative
        )

    async def getCountNewsByType(self):
        # 抓取新闻类型
        # 抓取新闻数量
        date = []
        newsType = []
        newsCount = []

        start_time = arrow.get(
                str(self.date) + ' 03:00:00+00:00',
                'YYYYMMDD HH:mm:ss')
        end_time = start_time.replace(days=+1)

        sql = '''
            SELECT
                count(*) AS Count,
                [Type]
            FROM
                [News]
            WHERE
                [CreatedTime]>='{start}'
            AND
                [CreatedTime]<'{end}'
            GROUP BY [Type]
        '''.format(
                start=start_time,
                end=end_time)

        res = mssql.query(sql, LOCAL)
        for item in res:
            date.append(self.date)
            newsType.append(int(item['Type']))
            newsCount.append(int(item['Count']))

        if len(date) == 0:
            date.append(self.date)
            newsType.append(0)
            newsCount.append(0)

        await self.insert(
            date=date,
            table='countNewsByType',
            newsType=newsType,
            newsCount=newsCount
        )

    async def getPushNewsTotalNormalReadCount(self):
        # 非推送阅读数
        date = []
        totalNormalReadCount = []

        sql = '''
                SELECT
                    CAST(SUM(readCount) AS UNSIGNED) AS totalNormalReadCount
                FROM
                    {table1}
                INNER JOIN {table2} ON {table1}.date = {date}
                AND {db}.{table1}.date = {db}.{table2}.date
                AND {db}.{table1}.newsId = {db}.{table2}.newsId
        '''.format(
            db=DATABASE,
            table1='{}_ctrNews'.format(LOCAL),
            table2='{}_pushCtrNews'.format(LOCAL),
            date=self.date
        )

        res = DB(**DBCONFIG).query(sql)
        for item in res:
            try:
                date.append(self.date)
                totalNormalReadCount.append(int(item['totalNormalReadCount']))
            except Exception:
                continue

        if len(date) == 0:
            date.append(self.date)
            totalNormalReadCount.append(0)

        await self.insert(
            date=date,
            table='pushNewsTotalNormalReadCount',
            totalNormalReadCount=totalNormalReadCount
        )

    def getKeywordSearchCountDesc(self):
        # 搜索关键词以及搜索次数
        date = []
        keyword = []
        keywordCount = []

        content = self.readFile('keywordSearchCountDesc')
        for line in content:
            if line[0].startswith('""'):
                continue
            else:
                date.append(self.date)
                line[0] = '"' + line[0] + '"'
            keyword.append(line[0])
            try:
                keywordCount.append(int(line[1]))
            except IndexError:
                keywordCount.append(0)

        table = 'br_keywordSearchCountDesc'
        sql_base = '''
            INSERT INTO {db}.{table} (date, keyword, keywordCount) VALUES (
        '''.format(
            db=DATABASE,
            table=table,
        )

        for index, dt in enumerate(date):
            sql = sql_base + ', '.join([
                str(dt), keyword[index], str(keywordCount[index])])
            sql += ')'

            try:
                DB(**DBCONFIG).insert(sql)
            except Exception:
                continue


async def dealTasks():
    dates = arrow.now().replace(days=-2).format('YYYYMMDD')
    for date in [dates, ]:
        dS = DailyStatistics(date)

        tasks = [
            dS.getFetchesUserNews(),
            dS.getReadsUserNews(),
            dS.getLoginUserTotal(),
            dS.getFreshUserCount(),
            dS.getFetchReadByTag(),
            dS.getFreshFetchReadByTag(),
            dS.getHotFetchReadByTag(),
            dS.getFetchReadByNewsType(),
            dS.getFetchReadByTagIndex(),
            dS.getFetchReadByCategory(),
            dS.getPushCtrNews(),
            dS.getNewsPushedNewClient(),
            dS.getFreshPushRealCtr(),
            dS.getCountNewsIndex(),
            dS.getReadNewsTimeStatistics(),
            dS.getFetchNewsCountWithoutRelative(),
            dS.getPullCount(),
            dS.getTrendingCtr(),
            dS.getPushCtrNewsHour(),
            dS.getFetchReadByTagHour(),
            dS.getCtrNews(),
            dS.getCommentUser(),
            dS.getCrawledNews(),
            dS.getCountNewsByType(),
        ]

        '''
        tasks = [
            dS.getNewsPushedNewClient(),
            dS.getFreshPushRealCtr(),
        ]
        '''

        for task in asyncio.as_completed(tasks):
            await task

        dS.getKeywordSearchCountDesc(),
        await dS.getPushNewsTotalNormalReadCount(),


def main():
    event_loop = asyncio.get_event_loop()
    try:
        event_loop.run_until_complete(dealTasks())
    finally:
        event_loop.close()


if __name__ == '__main__':
    main()
