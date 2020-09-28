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

def get_work_holdings(holdings_path: str) -> dict:
    df = pd.read_json(holdings_path, lines=True,
        dtype={"bibliographicRecordId": str, "agencyId": str})
    occurrences = df.groupby(["agencyId", "bibliographicRecordId"]).size()\
        .to_frame(name="occurrences")\
        .reset_index()
    occurrences["potential-pid-1"] = "870970-basis:" + occurrences["bibliographicRecordId"]
    occurrences["potential-pid-2"] = occurrences["agencyId"] + "-katalog:" + occurrences["bibliographicRecordId"]
    all_pids = np.concatenate((occurrences["potential-pid-1"].unique(), occurrences["potential-pid-2"].unique()))
    all_works = lmf.pid2work(all_pids)
    occurrences["work1"] = occurrences["potential-pid-1"].apply(lambda p: all_works[p] if p in all_works else None)
    occurrences["work2"] = occurrences["potential-pid-2"].apply(lambda p: all_works[p] if p in all_works else None)
    occurrences["combined_work"] = np.where(occurrences["work1"].isna(), occurrences["work2"], occurrences["work1"])
    counts = occurrences.groupby("combined_work").sum("occurrences")
    return counts["occurrences"].to_dict()

def generate_work_to_holdings_map():
    parser = argparse.ArgumentParser()
    parser.add_argument("holdings_file_path", metavar="holdings-file-path",
        help="Path to holdings json file")
    parser.add_argument("output", help="Path to output file")
    args = parser.parse_args()

    work_to_holdings = get_work_holdings(args.holdings_file_path)
    with open(args.output, "wb") as fp:
        joblib.dump(work_to_holdings, fp)

def make_solr_documents(pid_list, work_to_holdings_map: dict, popularity_map: dict, synonym_container, limit=None):
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
            "---".join(docs[p].get("type", []))) for p in pids}
        pid_types_list = [f"{p}:::{pid_to_types_map[p][0]}:::{pid_to_types_map[p][1]}" for p in pids]
        n_pids = math.log(len(pids) if len(pids) <9 else 9)+1
        metadata = work2metadata[work]
        years_since_publication = get_years_since_publication(metadata["year"]) if "year" in metadata else 99
        holdings = math.log(int(work_to_holdings_map[work])) if work in work_to_holdings_map else 0
        popularity_sum = sum(popularity_map[pid] for pid in pids if pid in popularity_map)
        popularity = math.log(popularity_sum) if popularity_sum > 0 else 0
        document = {"workid": work,
                    "pids": pids,
                    "pid_to_type_map": pid_types_list,
                    "n_pids": n_pids,
                    "holdings": holdings,
                    "popularity": popularity,
                    "years_since_publication": years_since_publication}

        document.update(add_keys(metadata, ["title_alternative", "aut",
            "creator", "creator_sort", "contributor", "work_type",
            "language", "subject_dbc", "series"]))

        synonyms = __construct_synonym_list(document, synonym_container)
        if synonyms:
            document['subject_synonyms'] = synonyms
        document.update(add_keys(metadata, ["title"], is_list=False))
        yield document


def __construct_synonym_list(document, synonym_container):
    if 'subject_dbc' not in document:
        return []
    synonyms = []
    for subject in document['subject_dbc']:
        synonyms += synonym_container.get(subject, [])
    return synonyms


def get_years_since_publication(years):
    """
    Converts the year field to the number of years since publication,
    taking the newest published version in the work.
    """
    if not years:
        return 99
    try:
        year = max([int(y) for y in years])
        return datetime.datetime.now().year - year
    except ValueError as e:
        print(f"WARNING: failed converting element of {years} to an integer: {e}", file=sys.stderr)

def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i: i+n]

def create_collection(solr_url, pid_list, work_to_holdings_map, popularity_map: dict, synonym_file, limit=None, batch_size=1000):
    """
    Harvest rows from LOWELL and creates and indexes solr documents
    """
    logger.info('Reading subject synonyms')
    synonyms = Synonyms(synonym_file)
    logger.info("Retrieving data from db")
    documents = [d for d in make_solr_documents(pid_list, work_to_holdings_map, popularity_map, synonyms, limit)]
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
    parser.add_argument("work_to_holdings_map_path",
        metavar="work-to-holdings-map-path",
        help="Path to holdings file path, saved in joblib format")
    parser.add_argument("synonym_file", metavar="synonym-file", help="file with subject synonyms")
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

    popularity_file_opener = gzip.open if args.popularity_data[-3:] == ".gz" else open

    with open(args.work_to_holdings_map_path, "rb") as fp,\
            popularity_file_opener(args.popularity_data, "rb") as popularity_fp:
        popularity_map = __read_popularity_counts(fp)
        logger.info('Loading holdings-map')
        work_to_holdings = joblib.load(fp)
        create_collection(args.solr, args.pid_list, work_to_holdings,
                          popularity_map, args.synonym_file, args.limit)


def __read_popularity_counts(fp):
    logger.info("Loading popularity data")
    popularity_counts = []
    for line in popularity_fp:
        line = line.strip().decode("utf8")
        if " " not in line:
            continue
        parts = line.split(" ", maxsplit=1)
        popularity_counts.append(parts)
    popularity_map = {pid: int(count) for count, pid in popularity_counts}
    return popularity_map


if __name__ == "__main__":
    main()
