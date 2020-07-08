#!/usr/bin/env python3

"""
:mod:`simple_search.solr.indexer` -- indexes documents into solr

=======
indexer
=======

Creates document collection.

"""

import argparse
from collections import defaultdict
import math
import os

from mobus import lowell_mapping_functions as lmf
import logging
from tqdm import tqdm

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
    work2metadata_union = {}
    logger.info("Fetching work metadata")
    for work, metadata_entries in tqdm(work2metadata.items(), ncols=150):
        metadata_union = defaultdict(set)
        for metadata in metadata_entries:
            for key, value in metadata.items():
                metadata_union[key] |= set(value)
        for key, value in metadata_union.items():
            metadata_union[key] = list(value)
        work2metadata_union[work] = dict(metadata_union)
    return work2metadata_union

def get_documents(sql, *args):
    with dbc_pyutils.cursor.PostgresCursor(os.environ["LOWELL_URL"]) as cursor:
        cursor.execute(sql, *args)
        yield from cursor

def add_keys(metadata, keys, is_list=True):
    """ adds key to document"""
    document = {}
    for key in keys:
        if key in metadata:
            document[key] = metadata[key]
            if not is_list:
                document[key] = document[key][0]
    return document

def make_solr_documents(pid_list, limit=None):
    """
    Creates solr documents based on rows from LOWELL

    :param limit:
        limits number of retrieved rows
    """
    with open(pid_list) as fp:
        pids = [f.strip() for f in fp][:limit]
    pid2work = lmf.pid2work(pids)
    logger.info("pid2work size %s", len(pid2work))
    docs = {r[0]: r[1] for r in get_documents(
        "SELECT pid, metadata FROM metadata WHERE pid IN %s", (tuple(pids),))}
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
        pid_to_types_map = {p: ("---".join(docs[p]["collection"]),
            "---".join(docs[p]["type"])) for p in pids}
        pid_types_list = [f"{p}:::{pid_to_types_map[p][0]}:::{pid_to_types_map[p][1]}" for p in pids]
        n_pids = math.log(len(pids) if len(pids) <9 else 9)+1
        document = {"workid": work,
                    "pids": pids,
                    "pid_to_type_map": pid_types_list,
                    "n_pids": n_pids}

        metadata = work2metadata[work]

        document.update(add_keys(metadata, ["title_alternative", "aut",
            "creator", "creator_sort", "contributor", "work_type", "language", "subject_dbc"]))
        document.update(add_keys(metadata, ["title"], is_list=False))

        yield document

def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i: i+n]

def create_collection(solr_url, pid_list, limit=None, batch_size=1000):
    """
    Harvest rows from LOWELL and creates and indexes solr documents
    """
    logger.info("Retrieving data from db")
    documents = [d for d in make_solr_documents(pid_list, limit)]
    doc_chunks = [c for c in chunks(documents, batch_size)]
    logger.info(f"Created {len(doc_chunks)} document chunk (size={batch_size})")
    logger.info(f"Indexing into solr at {solr_url}")
    indexer = dbc_pyutils.solr.SolrIndexer(solr_url)
    for batch in tqdm(doc_chunks, ncols=150):
        indexer(batch)
    indexer.commit()

def setup_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("pid_list", metavar="pid-list",
        help="List of pids to include")
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

    create_collection(args.solr, args.pid_list, args.limit)

if __name__ == "__main__":
    main()
