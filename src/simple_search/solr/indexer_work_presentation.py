#!/usr/bin/env python3

"""
:mod:`simple_search.solr.indexer_work_presentation` -- indexes documents into solr

=========================
indexer_work_presentation
=========================

Creates document collection.

"""
import requests
import grequests

import json

import argparse
from collections import defaultdict
import gzip
import math
import os
import joblib
import logging
from tqdm import tqdm
import io
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
            if response is None:
                raise Exception
            elif not response.ok:
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

def map_work_to_metadata(cworks):
    """
    Collects metadata from all pids in work, and returns
    dictionary with the collected information
    """
    ## work2metadata : maps persistentworkid -> json from workobject table 
    work2metadata = defaultdict(dict)
    workRows = get_docs(
        "SELECT wo.persistentworkid, wo.corepoworkid, wo.content FROM workobject wo WHERE wo.corepoworkid IN (SELECT cworkid FROM cworkids_tmp)",
        cworks
    )
    for row in workRows:
        if not row[0] in work2metadata: # check prolly no needed
            work2metadata[row[0]] = {'corepoworkid': row[1], 'content': row[2]}
    work2metadata_union = {}
    logger.info("Fetching work metadata")
    for work, obj in tqdm(work2metadata.items(), ncols=150):
        metadata = obj['content']
        corepo_workid = obj['corepoworkid']
        metadata_union = defaultdict(set)
        if 'title' in metadata:
            metadata_union['title'] = metadata['title']
            metadata_union['title_alternative'] = metadata['title']
        metadata_union['workid'] = metadata['workId']
        metadata_union['corepo_workid'] = corepo_workid
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
        metadata_union['pids'] = pid_list
        metadata_union['work_type'] = list(work_types)
        metadata_union['pid2type'] = pid2type
        if len(metadata['creators']) > 0:
            creators = []
            auts = []
            for d in metadata['creators']:
                if d['type'] == 'aut':
                    auts.append(d['value'])
                creators.append(d['value'])
            metadata_union['creator'] = creators
            metadata_union['creator-phonetic'] = creators
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
    return work2metadata_union

def get_docs(stmt, pids, args=None):
    args = args if args else {}
    with dbc_pyutils.cursor.PostgresCursor(os.environ['WORK_PRESENTATION_URL']) as cur:
        pid_fp = io.StringIO()
        for _id in pids:
            pid_fp.write(f"{_id}\n")
        pid_fp.seek(0)
        cur.execute("DROP TABLE IF EXISTS cworkids_tmp")
        cur.execute("CREATE TEMP TABLE cworkids_tmp(cworkid TEXT)")
        cur.copy_from(pid_fp, "cworkids_tmp", columns=["cworkid"])
        cur.execute(stmt, args)
        for row in cur:
            yield row

def pid_type_dict(pid_type_list : list) -> dict:
    res = {}
    for s in pid_type_list:
        l = s.split(':::')
        res[l[0]] = l[1]
    return res

def make_solr_documents(cwork_list, work_to_holdings_map: dict, pop_map: dict, limit=None):
    """
    Creates solr documents based on rows from work presentation

    :param limit:
        limits number of retrieved rows
    """
    with open(cwork_list) as fp:
        cworks = [f.strip() for f in fp][:limit]
    work2metadata = map_work_to_metadata(cworks)
    logger.info("work2metadata size %s", len(work2metadata))


    for work in tqdm(work2metadata.keys()):
        # Solr doesn't have a field type which can be used as a tuple
        # natively and using nested documents for this solution will
        # introduce the overhead of then querying for child documents
        # and handling the combination logic afterwards.
        # Instead the mapping between a pid and which type it corresponds
        # to is encoding in a string with ::: separating the three elements,
        # pid, collection identifier, and type, and --- separating the individual
        # collection identifiers and types.
        metadata = work2metadata[work]
        pids = []
        if 'pids' in metadata:
            pids = metadata['pids']
        n_pids = math.log(len(pids) if len(pids) <9 else 9)+1
        
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
        holdings_sum = int(work_to_holdings_map.get(metadata['corepo_workid'], 0))
        holdings = math.log(holdings_sum) + 1 if holdings_sum > 0 else 1

        popularity_sum = sum(pop_map[pid] for pid in pids if pid in pop_map)
        popularity = math.log(popularity_sum) + 1 if popularity_sum > 0 else 1
        document = { # "title": title,
                    "workid": work,
                    "pids": pids,
                    "pid_to_type_map": pid_types_list,
                    "n_pids": n_pids,
                    "holdings": holdings,
                    "popularity": popularity,
                    "years_since_publication": years_since_publication}
        document.update(add_keys(metadata, ["title", "title_alternative", "aut",
            "creator", "creator_sort", "contributor", "work_type",
            "language", "subject_dbc", "series"]))
        yield document

def add_keys(metadata, keys, is_list=True):
    """ adds key to document"""
    document = {}
    for key in keys:
        if key in metadata:
            document[key] = metadata[key]
            if not is_list:
                document[key] = document[key][0]
    return document


def create_collection(solr_url, cwork_list, work_to_holdings_map, pop_map, limit=None, batch_size=1000):
    """
    Harvest rows from work-presentation and creates and indexes solr documents
    """
    logger.info("Retrieving data from db")
    documents = [d for d in make_solr_documents(cwork_list, work_to_holdings_map, pop_map, limit)]
    logger.info(f"Indexing into solr at {solr_url}")
    indexer = ThreadedSolrIndexer(solr_url, num_threads=10, batch_size=batch_size)
    with Time("Indexing into solr took: ", level="info"):
        indexer.index(documents)
    logger.info("Committing to solr...")
    indexer.commit()
    logger.info("Commit to solr done!")
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
    parser.add_argument("cwork_list", metavar="cwork-list", help="List of corepo workids to include")
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
        create_collection(args.solr, args.cwork_list, work_to_holdings, pop_map, args.limit)

if __name__ == "__main__":
    main()
