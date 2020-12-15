#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -*- mode: python -*-
"""
=================
Smartsearch Model
=================

Reads smartsearch data from file and collects number of workhits pr search, and writes them to file
"""
import json
from tqdm import tqdm
import pandas as pd
from mobus import lowell_mapping_functions as lmf
from collections import defaultdict
from dbc_pyutils import Time
import logging

logger = logging.getLogger(__name__)


def parse_file(smartsearch_file='Smartsearch1y.csv', outfile='search2works.json'):
    """
    Parses datafile and produces datafile to use in smartsearch feature
    """
    df = __read_and_clean_data(smartsearch_file)
    search2pids = {}
    pids = set()
    grouped = df.groupby('keyword')
    logger.info('Collecting hits for each search')
    for name, group in tqdm(grouped, ncols=150):
        if name.startswith('*') or name.startswith('(') or name.startswith(','):
            continue
        hits = list(group[['page', 'count']].itertuples(index=False, name=None))
        if hits:
            search2pids[name] = hits
            pids |= {pid for pid, _ in hits}
    pid2work = lmf.pid2work(list(pids))
    logger.info('mappings pids to works')
    search2works = {}
    for search, hits in tqdm(search2pids.items(), ncols=150):
        hits = __pid_mapper(hits, pid2work)
        if hits:
            search2works[search] = hits

    if outfile:
        logger.info(f'writing content to {outfile}')
        with open(outfile, 'w') as fp:
            json.dump(search2works, fp)
    return search2works


def __read_and_clean_data(path):
    logger.info('Reading data')
    df = pd.read_csv(path, skiprows=0, sep=';', error_bad_lines=False, encoding='ISO-8859-1')
    df.keyword = df.keyword.str.strip().str.strip('- [ ]')
    df.page = df.page.apply(lambda x: x.rsplit('.')[-1])
    return df


def __pid_mapper(hits, pid2work):
    work2count = defaultdict(int)
    for pid, count in hits:
        if pid in pid2work:
            work2count[pid2work[pid]] += count
    hits = sorted(work2count.items(), key=lambda x: x[1], reverse=True)
    hits = [list(e) for e in hits]
    return hits


class SmartSearch:
    """
    Returns result based on clickdata
    """
    def __init__(self, smartsearch_content, solr):
        self.data = smartsearch_content
        self.solr = solr

    @classmethod
    def load(cls, smartsearch_content_path, solr):
        logger.info('Loading smartsearch content')
        with Time('Loaded smartsearch in ', level='info'):
            with open(smartsearch_content_path) as fp:
                data = json.load(fp)
            return cls(data, solr)

    def get(self, query, max_n=3):
        return self.data.get(query, [])[:3]

    def search(self, query, max_n=3):
        with Time('smartsearch result took ', level='info'):
            result = self.get(query, max_n)
            if not result:
                return []
            query = ' OR '.join([f'workid:"{w}"' for w, _ in result])
            print(query)
            params = {
                      'fl': "pids,title,creator,contributor,workid,work_type,language,pid_to_type_map",
                      'rows': max_n}
            docs = [doc for doc in self.solr.search(query, **params)]
            return docs


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.DEBUG)
    parse_file()
