#!/usr/bin/env python3

import argparse
import json
import logging
import os

import tornado
from tornado.ioloop import IOLoop

from pkg_resources import resource_filename

from dbc_pyutils import BaseHandler
from dbc_pyutils import StatusHandler
from dbc_pyutils import build_info
from dbc_pyutils import Statistics
from dbc_pyutils import CoverUrls

from .solr.search import Searcher

STATS = {"search": Statistics(name="search")}

logger = logging.getLogger(__name__)

def setup_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("solr_url", metavar="solr-url")
    parser.add_argument("-p", "--port", default=5000)
    return parser.parse_args()


class CoverHandler(BaseHandler):

    def initialize(self):
        client_id = os.environ['OPEN_PLATFORM_CLIENT_ID']
        client_secret = os.environ['OPEN_PLATFORM_CLIENT_SECRET']
        self.cover_func = CoverUrls(client_id, client_secret)

    def get(self, pid):
        cover_info = self.cover_func([pid], fields=['coverUrlFull'])
        cover = cover_info.get(pid, {}).get('coverUrlFull', [''])[0]
        result = {'url': cover} if cover else {}
        self.write(result)


class ConfigHandler(BaseHandler):

    def get(self):
        config_filename = 'cfg/search_results_tester_config.json'
        config_path = resource_filename('simple_search', config_filename)
        queries = []
        with open(config_path, 'r') as f:
            config = json.load(f)
            queries = [query['q'] for query in config['queries']]
        self.write(json.dumps({'queries': queries}))
        
        
class SearchHandler(BaseHandler):
    def initialize(self, searcher):
        self.searcher = searcher

    def post(self):
        body = json.loads(self.request.body.decode("utf8"))
        query = body["q"]
        debug = body.get("debug", False)
        options = body.get("options", {})
        result = {"result": [doc for doc in self.searcher.search(query, debug, options)]}
        self.write(result)

    def get(self):
        query = self.get_argument('q')
        debug = self.get_argument('debug', 'False')
        debug = True if debug.lower() in {'true', '1'} else False
        result = {"result": [doc for doc in self.searcher.search(query, debug)]}
        self.write(result)


class DefaultHandler(BaseHandler):
    def get(self):
        path = resource_filename('simple_search', 'html/index.html')
        print('Hello')
        self.render(path)


class APIHandler(BaseHandler):
    def get(self):
        path = resource_filename('simple_search', 'html/help.html')
        self.render(path)


def main():
    args = setup_args()
    info = build_info.get_info("simple_search")
    searcher = Searcher(args.solr_url)
    tornado_app = tornado.web.Application([
        ("/", DefaultHandler, {}),
        ("/config", ConfigHandler),
        ("/api", APIHandler),
        ("/cover/(.*)", CoverHandler),
        ("/search", SearchHandler, {"searcher": searcher}),
        ("/status", StatusHandler, {"ab_id": 1, "info": info, "statistics": list(STATS.values())})
    ])
    tornado_app.listen(args.port)
    IOLoop.current().start()
