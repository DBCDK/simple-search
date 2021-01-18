#!/usr/bin/env python3

"""
:mod:`simple_search.solr.indexer` -- indexes documents into solr

=======
indexer
=======

Creates document collection.

"""

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

from mobus import lowell_mapping_functions as lmf
from simple_search.synonym_list import Synonyms
import dbc_pyutils.solr
import dbc_pyutils.cursor


logger = logging.getLogger(__name__)

def map_work_to_metadata(docs, pid2work):
    """
    Collects metadata from all pids in work, and returns
    dictionary with the collected information
    """
    work2metadata = defaultdict(list)
    for pid, work in tqdm(pid2work.items(), ncols=150):
        if pid in docs:
            work2metadata[work].append(docs[pid])
    logger.debug(work2metadata)
    work2metadata_union = {}
    logger.info("Fetching work metadata")
    for work, metadata_entries in tqdm(work2metadata.items(), ncols=150):
        metadata_union = defaultdict(set)
        for metadata in metadata_entries:
            if 'title' in metadata:
                metadata_union['title'] = metadata['title']
                metadata_union['title_alternative'] = metadata['title']
            metadata_union['workid'] = metadata['workId']
            pid_list = []
            work_types = set()
            pid2type = []
            if 'dbUnits' in metadata:
                dbUnit_pid_dicts = metadata['dbUnits']
                for unit in dbUnit_pid_dicts:
                    for d in dbUnit_pid_dicts[unit]:
                        if 'pid' in d:
                            pid_list.append(d['pid'])
                        if 'types' in d:
                            work_types |= set(d['types'])
                        if 'pid' in d and 'types' in d:
                            pid2type.append(d['pid'] + ':::' + d['types'][0])
                logger.debug('pids')
                logger.debug(",".join(pid_list))
            metadata_union['pids'] = pid_list
            metadata_union['work_type'] = list(work_types)
            metadata_union['pid2type'] = pid2type
            if len(metadata['creators']) > 0:
                contributors = []
                auts = []
                for d in metadata['creators']:
                    if d['type'] == 'aut':
                        auts.append(d['value'])
                    else:
                        contributors.append(d['value'])
                metadata_union['contributor'] = contributors
                metadata_union['contributor-phonetic'] = contributors
                metadata_union['aut'] = auts
            if 'subjects' in metadata:
                subjects = metadata['subjects']
                dbc_subjects = set()
                metadata_union['subject_dbc'] = []
                for key in ['DBCF', 'DBCM', 'DBCN', 'DBCO', 'DBCS']:
                    for d in subjects:
                        if d['type'] == key:
                            dbc_subjects.add(d['value'])
                metadata_union['subject_dbc'] = list(dbc_subjects)
            metadata_union['language'] = 'dk' # todo
            
        work2metadata_union[work] = dict(metadata_union)
    logger.debug(work2metadata_union)
    return work2metadata_union

def get_documents(sql, *args):
    with dbc_pyutils.cursor.PostgresCursor(os.environ["WORK_PRESENTATION_URL"]) as cursor:
        cursor.execute(sql, *args)
        yield from cursor

def pid2pwork(pids) -> dict:
    """ Creates pid2work dict by fetching all relevant works from relations table in work-presentation-db """
    logger.info("fetching workids for %d pids", len(pids))
    pids_tuple = tuple(pids)
    p2w = {}
    for row in get_documents(
        "SELECT wc.manifestationid pid, wo.persistentworkid persistentworkid FROM workobject wo, workcontains wc WHERE wo.corepoworkid = wc.corepoworkid AND wc.manifestationid IN %s", (pids_tuple,)
        ):
        p2w[row[0]] = row[1]
    return dict(p2w)

def pid2corepo_work(pids) -> dict:
    logger.info("fetching corepo-workids for %d pids", len(pids))
    pids_tuple = tuple(pids)
    p2cw = {}
    for row in get_documents(
        "SELECT manifestationid, corepoworkid FROM workcontains WHERE manifestationid IN %s", (pids_tuple,)
        ):
        p2cw[row[0]] = row[1]
    return dict(p2cw)


def pid_type_dict(pid_type_list : list) -> dict:
    res = {}
    for s in pid_type_list:
        l = s.split(':::')
        res[l[0]] = l[1]
    return res

def make_solr_documents(pid_list, work_to_holdings_map: dict, pop_map: dict, limit=None):
    """
    Creates solr documents based on rows from work presentation

    :param limit:
        limits number of retrieved rows
    """
    with open(pid_list) as fp:
        pids = [f.strip() for f in fp][:limit]
    pid2work = pid2pwork(pids)
    pid2cwork = pid2corepo_work(pids)
    logger.info("pid2work size %s", len(pid2work))
    docs = {r[0]: r[1] for r in get_documents(
        "SELECT wc.manifestationid pid, wo.content FROM workcontains wc JOIN workobject wo ON wo.corepoworkid = wc.corepoworkid WHERE wc.manifestationid IN %s", (tuple(pids),))}
    logger.info("size of docs %s", len(docs))

    work2metadata = map_work_to_metadata(docs, pid2work)
    logger.info("work2metadata size %s", len(work2metadata))

    work2pids = defaultdict(list)
    for pid, work in pid2work.items():
        work2pids[work].append(pid)
    logger.info("created work2pids")

    for work, pids in tqdm(work2pids.items()):
        # Solr doesn't have a field type which can be used as a tuple
        # natively and using nested documents for this solution will
        # introduce the overhead of then querying for child documents
        # and handling the combination logic afterwards.
        # Instead the mapping between a pid and which type it corresponds
        # to is encoding in a string with ::: separating the three elements,
        # pid, collection identifier, and type, and --- separating the individual
        # collection identifiers and types.

        # pid_to_types_map = {p: ("---".join(docs[p]["collection"]),
        #      "---".join(docs[p].get("type", []))) for p in pids}
        # pid_types_list = [f"{p}:::{pid_to_types_map[p][0]}:::{pid_to_types_map[p][1]}" for p in pids]


        n_pids = math.log(len(pids) if len(pids) <9 else 9)+1
        metadata = work2metadata[work]
        if 'pid2type' in metadata:
            work_pid_types = pid_type_dict(metadata['pid2type'])
            pid_types_list = []
            for p in pids:
                if p in work_pid_types:
                    s = p + ":::" + "870970-basis---870970-danbib---870970-bibdk" + ":::" + work_pid_types[p]
                    pid_types_list.append(s)
                else:
                    pid_types_list.append(p)
        else:
            pid_types_list = [f"{p}" for p in pids]
#        years_since_publication = get_years_since_publication(metadata["year"]) if "year" in metadata else 99
        years_since_publication = 99

        # Add one to holdings and popularity to avoid zeros since boosting is multiplicative
        holdings_sum = sum(int(work_to_holdings_map.get(pid2cwork.get(pid, 'hest'), 0)) for pid in pids)
        holdings = math.log(holdings_sum) + 1 if holdings_sum > 0 else 1
#        holdings = 1

        popularity_sum = sum(pop_map[pid] for pid in pids if pid in pop_map)
        popularity = math.log(popularity_sum) + 1 if popularity_sum > 0 else 1
#        popularity = 1
        document = { # "title": title,
                    "workid": work,
                    "pids": pids,
                    "pid_to_type_map": pid_types_list,
                    "n_pids": n_pids,
                    "holdings": holdings,
                    "popularity": popularity,
                    "years_since_publication": years_since_publication}
        logger.debug(document)
        document.update(add_keys(metadata, ["title", "title_alternative", "aut",
            "creator", "creator_sort", "contributor", "work_type",
            "language", "subject_dbc", "series"]))
        logger.debug(document)

        yield document

def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i: i+n]

def add_keys(metadata, keys, is_list=True):
    """ adds key to document"""
    document = {}
    for key in keys:
        if key in metadata:
            document[key] = metadata[key]
            if not is_list:
                document[key] = document[key][0]
    return document


def create_collection(solr_url, pid_list, work_to_holdings_map, pop_map, limit=None, batch_size=1000):
    """
    Harvest rows from work-presentation and creates and indexes solr documents
    """
    logger.info("Retrieving data from db")
    documents = [d for d in make_solr_documents(pid_list, work_to_holdings_map, pop_map, limit)]
    doc_chunks = [c for c in chunks(documents, batch_size)]
    logger.info(f"Created {len(doc_chunks)} document chunk (size={batch_size})")
    logger.info(f"Indexing into solr at {solr_url}")
    indexer = dbc_pyutils.solr.SolrIndexer(solr_url)
    for batch in tqdm(doc_chunks, ncols=150):
        indexer(batch)
    indexer.commit()
    return

def __read_popularity_counts(fp):
    logger.info("Loading popularity data")
    popularity_counts = []
    for line in fp:
        line = line.strip().decode("utf8")
        if " " not in line:
            continue
        parts = line.split(" ", maxsplit=1)
        popularity_counts.append(parts)
    popularity_map = {pid: int(count) for count, pid in popularity_counts}
    return popularity_map

def setup_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("pid_list", metavar="pid-list", help="List of pids to include")
    parser.add_argument("solr", help="solr url")
    parser.add_argument("work_to_holdings_map_path",
        metavar="work-to-holdings-map-path",
        help="Path to holdings file path, saved in joblib format")
    parser.add_argument("popularity_data", metavar="popularity-data",
        help="path to file containing data (hit counts)")
    parser.add_argument("-l", "--limit", type=int, dest="limit", help="if set, limits the number of harvested loans")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", help="verbose output")
    return parser.parse_args()

def main():
    args = setup_args()

    level = logging.INFO
    if args.verbose:
        level = logging.DEBUG
    logging.basicConfig(format="%(asctime)s : %(levelname)s : %(message)s", level=level)
    pop_file_opener = gzip.open if args.popularity_data[-3:] == ".gz" else open
    with open(args.work_to_holdings_map_path, "rb") as w2h_fp, pop_file_opener(args.popularity_data, "rb") as pop_fp:
        pop_map = __read_popularity_counts(pop_fp)
        work_to_holdings = joblib.load(w2h_fp)
        create_collection(args.solr, args.pid_list, work_to_holdings, pop_map, args.limit)

if __name__ == "__main__":
    main()
