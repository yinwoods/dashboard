# coding: utf-8
import tornado.ioloop
import tornado.httpserver
from tornado.options import define, options
from tornado.httpclient import AsyncHTTPClient
from dashboard.application import Application
AsyncHTTPClient.configure('tornado.curl_httpclient.CurlAsyncHTTPClient')


define("port", default=8989, help="run on the given port", type=int)
define("method", default=1, help="1 multi process, 2 one process", type=int)
define("debug", default=1, help="1 true, 2 false", type=int)
if __name__ == '__main__':
    tornado.options.parse_command_line()
    debug = False
    if options.debug == 1:
        debug = True
    else:
        debug = False
    if options.method == 1:
        http_server = tornado.httpserver.HTTPServer(Application(debug))
        http_server.bind(options.port)
        http_server.start(0)
        tornado.ioloop.IOLoop.instance().start()
    else:
        http_server = tornado.httpserver.HTTPServer(Application(debug))

        http_server.listen(options.port)
        tornado.ioloop.IOLoop.instance().start()
