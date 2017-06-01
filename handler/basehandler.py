import json
import pyDes
import base64
import urllib
import arrow
import tornado.web
from config import DES_KEY


class BaseHandler(tornado.web.RequestHandler):

    def get_current_user(self):
        name = self.get_secure_cookie("user")
        return '' if name is None else tornado.escape.json_decode(name)

    def check_origin(self, origin):
        parsed_origin = urllib.parse.urlparse(origin)
        return parsed_origin.netloc.endswith("http://localhost:8080")

    def set_default_headers(self):
        self.set_header('Access-Control-Allow-Origin', 'http://localhost:8080')
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS, DELETE')
        self.set_header('Access-Control-Allow-Credentials', 'true')
        self.set_header('Access-Control-Max-Age', 1000)
        self.set_header('Access-Control-Allow-Headers', '*')

    def write(self, chunk):
        '''
        token = self.get_cookie('token')
        token = base64.b64decode(token)
        des = pyDes.des(DES_KEY, pyDes.ECB, padmode=pyDes.PAD_PKCS5)
        res = des.decrypt(token)
        token = json.loads(res.decode())

        userid = token.get('id', None)
        username = token.get('username', None)
        roleids = token.get('roleids', None)
        time = token.get('time', None)

        if any([userid is None, username is None,
                roleids is None, time is None]):
            chunk = dict(
                errid='-99',
                errmsg="Token Error",
                data=[]
            )

        now = int(arrow.utcnow().timestamp) * 1000
        if now > int(time):
            chunk = dict(
                errid='-98',
                errmsg='Token Outdate',
                data=[]
            )
        '''
        super(BaseHandler, self).write(chunk)
