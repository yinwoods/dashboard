import json
import asyncio
from dashboard.config import IDLOCAL
from dashboard.config import BRLOCAL
from dashboard.config import MELOCAL
from dashboard.model.DB import DB
from dashboard.config import DBCONFIG
from dashboard.handler.basehandler import BaseHandler
from nip_common.api import NIPcommonAPI


class IDQueryNewsHandler(BaseHandler):

    def initialize(self):
        self.LOCAL = IDLOCAL

    async def getNewsInfo(self):
        """
        根据query_by的值来区分用哪种方式获取News信息
        一共有三种方式：按news_id_list、media_id以及category_name
        return: res 包含news信息的一个dict
        """

        res = list()
        news_info = dict()
        keys = ['url', 'media_id', 'title',
                'author', 'news_id', 'created_time']

        query_by = self.get_argument('query_by', None)
        if query_by == 'news_id_list':
            news_ids = self.get_argument('value', None)
            news_id_list = []
            for news_id in news_ids.split(','):
                news_id_list.append(int(news_id))
            print(news_id_list)
            news_list = NIPcommonAPI(self.LOCAL).get_news_list(news_id_list)
            for news in news_list:
                news_info = dict()
                for key in keys:
                    news_info.update({
                        key: news.__dict__.get(key) for key in keys})
                for key in ['url', 'title']:
                    news_info.update({key: news_info.get(key).strip()})
                res.append(news_info)

        elif query_by == 'media_id':
            media_ids = self.get_argument('value', None)
            start_time = self.get_argument('start_time', None)
            end_time = self.get_argument('end_time', None)
            all_news = []
            for media_id in media_ids.split(','):
                all_news.append(
                        NIPcommonAPI(self.LOCAL).get_news_list_by_media_id(
                            media_id, start_time, end_time))
            for news_list in all_news:
                for news in news_list:
                    news_info = dict()
                    for key in keys:
                        news_info.update({
                            key: news.__dict__.get(key) for key in keys})
                    for key in ['url', 'title']:
                        news_info.update({key: news_info.get(key).strip()})
                    res.append(news_info)

        elif query_by == 'category_name':
            category_name = self.get_argument('value', None)
            start_time = self.get_argument('start_time', None)
            end_time = self.get_argument('end_time', None)
            news_list = NIPcommonAPI(
                    self.LOCAL).get_news_list_by_category_name(
                            category_name, start_time, end_time)
            for news in news_list:
                news_info = dict()
                for key in keys:
                    news_info.update({
                        key: news.__dict__.get(key) for key in keys})
                for key in ['url', 'title']:
                    news_info.update({key: news_info.get(key).strip()})
                res.append(news_info)
        return res

    async def getData(self):
        """
        规范输出格式
        """
        data = dict()

        data.update({'lists': list()})
        data['lists'] = await self.getNewsInfo()
        return data

    def get(self):

        errid = 0
        errmsg = 'SUCCESS'
        data = None

        event_loop = asyncio.get_event_loop()
        # try:
        data = event_loop.run_until_complete(
                    self.getData())
        # except Exception as e:
        #     errid = -1
        #     errmsg = str(e)

        res = dict()
        res['errid'] = errid
        res['errmsg'] = errmsg
        res['data'] = data
        self.write(json.dumps(res))


class IDRemoveNewsHandler(BaseHandler):
    """
    删除新闻
    """
    def initialize(self):
        self.LOCAL = IDLOCAL

    async def removeNews(self):
        """
        删除给定news_id的新闻
        :param news_id 待删除新闻的news_id
        :return: bool 操作是否成功
        """
        info = []

        delete_by = self.get_argument('delete_by', None)
        if delete_by == 'news_id':
            res_dict = dict()
            news_id = self.get_argument('value', None)
            res_dict.update({'news_id': int(news_id)})
            remove_statu = NIPcommonAPI(self.LOCAL).delete_news(news_id)
            res_dict.update({'remove_statu': remove_statu})
            info.append(res_dict)
        elif delete_by == 'news_id_list':
            news_ids = self.get_argument('value', None)
            for news_id in news_ids.strip().split(','):
                res_dict = dict()
                res_dict.update({'news_id': int(news_id)})
                remove_statu = NIPcommonAPI(self.LOCAL).delete_news(news_id)
                res_dict.update({'remove_statu': remove_statu})
                info.append(res_dict)
        return {'info': info}

    async def getData(self):
        data = dict()

        data.update(await self.removeNews())
        return data

    def delete(self):

        errid = 0
        errmsg = 'SUCCESS'
        data = None

        event_loop = asyncio.get_event_loop()
        try:
            data = event_loop.run_until_complete(
                    self.getData())
        except Exception as e:
            errid = -1
            errmsg = str(e)

        res = dict()
        res['errid'] = errid
        res['errmsg'] = errmsg
        res['data'] = data
        self.write(json.dumps(res))


class BRQueryNewsHandler(BaseHandler):

    def initialize(self):
        self.LOCAL = BRLOCAL

    async def getNewsInfo(self):
        """
        根据query_by的值来区分用哪种方式获取News信息
        一共有三种方式：按news_id_list、media_id以及category_name
        return: res 包含news信息的一个dict
        """

        res = list()
        news_info = dict()
        keys = ['url', 'media_id', 'title',
                'author', 'news_id', 'created_time']

        query_by = self.get_argument('query_by', None)

        if query_by == 'news_id_list':
            news_ids = self.get_argument('value', None)
            news_id_list = []
            for news_id in news_ids.split(','):
                news_id_list.append(int(news_id))
            news_list = NIPcommonAPI(self.LOCAL).get_news_list(news_id_list)
            for news in news_list:
                news_info = dict()
                for key in keys:
                    news_info.update({
                        key: str(news.__dict__.get(key)) for key in keys})
                for key in ['url', 'title']:
                    news_info.update({key: news_info.get(key).strip()})
                res.append(news_info)

        elif query_by == 'media_id':
            media_ids = self.get_argument('value', None)
            start_time = self.get_argument('start_time', None)
            end_time = self.get_argument('end_time', None)
            all_news = []
            for media_id in media_ids.split(','):
                all_news.append(NIPcommonAPI(
                    self.LOCAL).get_news_list_by_media_id(
                        media_id, start_time, end_time))
            for news_list in all_news:
                for news in news_list:
                    news_info = dict()
                    for key in keys:
                        news_info.update({
                            key: str(news.__dict__.get(key)) for key in keys})
                    for key in ['url', 'title']:
                        news_info.update({key: news_info.get(key).strip()})
                    res.append(news_info)

        elif query_by == 'category_name':
            category_name = self.get_argument('value', None)
            start_time = self.get_argument('start_time', None)
            end_time = self.get_argument('end_time', None)
            news_list = NIPcommonAPI(
                    self.LOCAL).get_news_list_by_category_name(
                            category_name, start_time, end_time)
            for news in news_list:
                news_info = dict()
                for key in keys:
                    news_info.update({
                        key: str(news.__dict__.get(key)) for key in keys})
                for key in ['url', 'title']:
                    news_info.update({key: news_info.get(key).strip()})
                res.append(news_info)
        elif query_by == 'keyword':
            keyword = self.get_argument('value', None)
            sql = """
                select * from crawlsystem.crawl_rss_data as r,
                    crawlsystem.crawl_rss_extract_info as i
                where r.rssid=i.id and r.title like \"%%{}%%\"
                order by r.id desc limit 1 """.format(keyword)
            print(res)
            res = DB(**DBCONFIG).query(sql)
        return res

    async def getData(self):
        """
        规范输出格式
        """
        data = dict()

        data.update({'lists': list()})
        data['lists'] = await self.getNewsInfo()
        return data

    def get(self):

        errid = 0
        errmsg = 'SUCCESS'
        data = None

        event_loop = asyncio.get_event_loop()
        try:
            data = event_loop.run_until_complete(
                    self.getData())
        except Exception as e:
            errid = -1
            errmsg = str(e)

        res = dict()
        res['errid'] = errid
        res['errmsg'] = errmsg
        res['data'] = data
        print(res)
        self.write(json.dumps(res))


class BRRemoveNewsHandler(BaseHandler):
    """
    删除新闻
    """
    def initialize(self):
        self.LOCAL = BRLOCAL

    async def removeNews(self):
        """
        删除给定news_id的新闻
        :param news_id 待删除新闻的news_id
        :return: bool 操作是否成功
        """
        info = []

        delete_by = self.get_argument('delete_by', None)
        if delete_by == 'news_id':
            res_dict = dict()
            news_id = self.get_argument('value', None)
            res_dict.update({'news_id': int(news_id)})
            remove_statu = NIPcommonAPI(self.LOCAL).delete_news(news_id)
            res_dict.update({'remove_statu': remove_statu})
            info.append(res_dict)
        elif delete_by == 'news_id_list':
            news_ids = self.get_argument('value', None)
            for news_id in news_ids.strip().split(','):
                res_dict = dict()
                res_dict.update({'news_id': int(news_id)})
                remove_statu = NIPcommonAPI(self.LOCAL).delete_news(news_id)
                res_dict.update({'remove_statu': remove_statu})
                info.append(res_dict)
        return {'info': info}

    async def getData(self):
        data = dict()

        data.update(await self.removeNews())
        return data

    def delete(self):

        errid = 0
        errmsg = 'SUCCESS'
        data = None

        event_loop = asyncio.get_event_loop()
        try:
            data = event_loop.run_until_complete(
                    self.getData())
        except Exception as e:
            errid = -1
            errmsg = str(e)

        res = dict()
        res['errid'] = errid
        res['errmsg'] = errmsg
        res['data'] = data
        self.write(json.dumps(res))


class MEQueryNewsHandler(BaseHandler):

    def initialize(self):
        self.LOCAL = MELOCAL

    async def getNewsInfo(self):
        """
        根据query_by的值来区分用哪种方式获取News信息
        一共有三种方式：按news_id_list、media_id以及category_name
        return: res 包含news信息的一个dict
        """

        res = list()
        news_info = dict()
        keys = ['url', 'media_id', 'title',
                'author', 'news_id', 'created_time']

        query_by = self.get_argument('query_by', None)
        if query_by == 'news_id_list':
            news_ids = self.get_argument('value', None)
            news_id_list = []
            for news_id in news_ids.split(','):
                news_id_list.append(int(news_id))
            news_list = NIPcommonAPI(self.LOCAL).get_news_list(news_id_list)
            for news in news_list:
                news_info = dict()
                for key in keys:
                    news_info.update({
                        key: news.__dict__.get(key) for key in keys})
                for key in ['url', 'title']:
                    news_info.update({key: news_info.get(key).strip()})
                res.append(news_info)

        elif query_by == 'media_id':
            media_ids = self.get_argument('value', None)
            start_time = self.get_argument('start_time', None)
            end_time = self.get_argument('end_time', None)
            all_news = []
            for media_id in media_ids.split(','):
                all_news.append(NIPcommonAPI(
                    self.LOCAL).get_news_list_by_media_id(
                        media_id, start_time, end_time))
            for news_list in all_news:
                for news in news_list:
                    news_info = dict()
                    for key in keys:
                        news_info.update({
                            key: news.__dict__.get(key) for key in keys})
                    for key in ['url', 'title']:
                        news_info.update({key: news_info.get(key).strip()})
                    res.append(news_info)

        elif query_by == 'category_name':
            category_name = self.get_argument('value', None)
            start_time = self.get_argument('start_time', None)
            end_time = self.get_argument('end_time', None)
            news_list = NIPcommonAPI(
                    self.LOCAL).get_news_list_by_category_name(
                            category_name, start_time, end_time)
            for news in news_list:
                news_info = dict()
                for key in keys:
                    news_info.update({
                        key: news.__dict__.get(key) for key in keys})
                for key in ['url', 'title']:
                    news_info.update({key: news_info.get(key).strip()})
                res.append(news_info)
        return res

    async def getData(self):
        """
        规范输出格式
        """
        data = dict()

        data.update({'lists': list()})
        data['lists'] = await self.getNewsInfo()
        return data

    def get(self):

        errid = 0
        errmsg = 'SUCCESS'
        data = None

        event_loop = asyncio.get_event_loop()
        try:
            data = event_loop.run_until_complete(
                    self.getData())
        except Exception as e:
            errid = -1
            errmsg = str(e)

        res = dict()
        res['errid'] = errid
        res['errmsg'] = errmsg
        res['data'] = data
        self.write(json.dumps(res))


class MERemoveNewsHandler(BaseHandler):
    """
    删除新闻
    """
    def initialize(self):
        self.LOCAL = MELOCAL

    async def removeNews(self):
        """
        删除给定news_id的新闻
        :param news_id 待删除新闻的news_id
        :return: bool 操作是否成功
        """
        info = []

        delete_by = self.get_argument('delete_by', None)
        if delete_by == 'news_id':
            res_dict = dict()
            news_id = self.get_argument('value', None)
            res_dict.update({'news_id': int(news_id)})
            remove_statu = NIPcommonAPI(self.LOCAL).delete_news(news_id)
            res_dict.update({'remove_statu': remove_statu})
            info.append(res_dict)
        elif delete_by == 'news_id_list':
            news_ids = self.get_argument('value', None)
            for news_id in news_ids.strip().split(','):
                res_dict = dict()
                res_dict.update({'news_id': int(news_id)})
                remove_statu = NIPcommonAPI(self.LOCAL).delete_news(news_id)
                res_dict.update({'remove_statu': remove_statu})
                info.append(res_dict)
        return {'info': info}

    async def getData(self):
        data = dict()

        data.update(await self.removeNews())
        return data

    def delete(self):

        errid = 0
        errmsg = 'SUCCESS'
        data = None

        event_loop = asyncio.get_event_loop()
        try:
            data = event_loop.run_until_complete(
                    self.getData())
        except Exception as e:
            errid = -1
            errmsg = str(e)

        res = dict()
        res['errid'] = errid
        res['errmsg'] = errmsg
        res['data'] = data
        self.write(json.dumps(res))
