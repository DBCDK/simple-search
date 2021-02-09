#!/usr/bin/env python3

import argparse
import json
from dbc_pyutils.cursor import PostgresCursor
import os
import random

def cleanup(s):
    return s.replace('"', '')


def filter(s):
    # filter away ccl-queries
    if '=' in s:
        return True
    if s[0].isdigit():
        return True
    return False


def fetch_requests_for_performance_test_simple_search(num=1000, filename='simple_search.requests'):
    url = os.environ['BIBDKLOGDB_URL']
    with PostgresCursor(url) as cursor:
        sql = "SELECT data FROM event WHERE data->>'action'='query' AND timestamp >= '2020-01-01 00:00:00' LIMIT 100000"
        cursor.execute(sql)
        res = {}
        for row in cursor.fetchall():
            dct = row[0]
            data = dct['data']
            session_id = dct['session_id']
            query = data['queries'][0]
            if filter(query):
                continue
            query = cleanup(query)
            request = {'q': query}
            res[session_id] = request
        all_requests = [req for req in res.values()]
        requests = random.sample(all_requests, num)
        with open(filename, 'w') as f:
            for request in requests:
                # add smart-search:
                request['options'] = {'include-smartsearch': True}
                f.write(json.dumps(request) + '\n')


def cli():
    parser = argparse.ArgumentParser('Generates a file with requests for use with pytools.bench to make performance test of simple-search')
    parser.add_argument('-o', '--outfilename',
                        default='simple_search.requests',
                        help='filename to write requests into')
    parser.add_argument('-n', '--num', default=1000, type=int, help='Number of requests to generate')
    return parser.parse_args()


def run():
    args = cli()
    fetch_requests_for_performance_test_simple_search(args.num, args.outfilename)


if __name__ == '__main__':
    run()
