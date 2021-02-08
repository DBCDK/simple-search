#!/usr/bin/env python3

"""
:mod:`simple_search.solr.indexer` -- indexes documents into solr

=======
indexer
=======

Creates document collection.

"""
import requests
import grequests

import json

import argparse
import collections
from collections import defaultdict
import datetime
import gzip
import math
import os
import sys
import joblib
import logging
from tqdm import tqdm
import numpy as np
import pandas as pd
from psycopg2 import connect
import psycopg2.extras
import io
from mobus import lowell_mapping_functions as lmf
from simple_search.synonym_list import Synonyms
import dbc_pyutils.solr
import dbc_pyutils.cursor
from dbc_pyutils import Time


class ThreadedSolrIndexer():
    """
    Solr indexer.
    Indexer with batch functionality and parallel indexing from
    multiple threads
    """
    def __init__(self, url, num_threads=1, batch_size=1000):
        """
        Initializes indexer

        :param url:
            url of solr collection
        :param num_threads:
            Number of parallel indexer threads
        :param batch_size:
            Number of documents in each batch
        """
        self.url = url.rstrip('/')
        self.num_threads = num_threads
        self.batch_size = batch_size
        logger.info(f'Solr indexer initialized url={self.url}, num_threads={self.num_threads}, batch_size={self.batch_size}')

    def __call__(self, documents):
        return self.index(documents)

    def index(self, documents):
        """
        indexes docs into solr

        :param docs:
            list of docs to index
        """
        doc_chunks = (chunk for chunk in self.__chunks(documents, self.batch_size))
        rs = (grequests.post(self.url + '/update', data=json.dumps(docs), headers={'Content-Type': 'application/json'})
              for docs in doc_chunks)
        responses = grequests.map(rs, size=self.num_threads)
        for response in responses:
            if not response.ok:
                response.raise_for_status()

    def commit(self):
        """ commits changed to solr collection """
        try:
            resp = requests.get(self.url + '/update', params={'commit': 'true'})
        except Exception as e:
            print('Committing to solr failed!')
            raise
        if not resp.ok:
            resp.raise_for_status()
        return

    def __chunks(self, l, n):
        for i in range(0, len(l), n):
            yield l[i: i+n]

logger = logging.getLogger(__name__)

def create_collection(solr_url, docs, limit=None, batch_size=1000):
    """
    Harvest rows from work-presentation and creates and indexes solr documents
    """
    logger.info(f"Indexing into solr at {solr_url}")
    indexer = ThreadedSolrIndexer(solr_url, num_threads=10, batch_size=batch_size)
    with Time("Indexing into solr took: ", level="info"):
        indexer.index(docs)
    logger.info("Committing to solr...")
    indexer.commit()
    logger.info("Commit to solr done!")
    return

def setup_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", help="file with docs")
    parser.add_argument("solr", help="solr url")
    parser.add_argument("-l", "--limit", type=int, dest="limit", help="if set, limits the number of harvested loans")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", help="verbose output")
    return parser.parse_args()

def main():
    args = setup_args()

    level = logging.INFO
    if args.verbose:
        level = logging.DEBUG
    logging.basicConfig(format="%(asctime)s : %(levelname)s : %(message)s", level=level)
    with open(args.filename, "r") as fp:
        docs = json.load(fp)
        create_collection(args.solr, docs, args.limit)

if __name__ == "__main__":
    main()
