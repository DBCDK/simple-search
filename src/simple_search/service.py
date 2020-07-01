#!/usr/bin/env python3

import argparse
import json
import logging
import os

import tornado
from tornado.ioloop import IOLoop

from dbc_pyutils import BaseHandler
from dbc_pyutils import StatusHandler
from dbc_pyutils import build_info
from dbc_pyutils import Statistics

from . import solr

STATS = {"search": Statistics(name="search")}

logger = logging.getLogger(__name__)

def setup_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("solr_url", metavar="solr-url")
    parser.add_argument("-p", "--port", default=5000)
    return parser.parse_args()

class SearchHandler(BaseHandler):
    def post(self):
        self.write("Not implemented yet")

def main():
    args = setup_args()
    info = build_info.get_info("simple_search")
    tornado_app = tornado.web.Application([
        ("/search", SearchHandler),
        ("/status", StatusHandler, {"ab_id": 1, "info": info, "statistics": list(STATS.values())})
    ])
    tornado_app.listen(args.port)
    IOLoop.current().start()
