#!/usr/bin/env python3

"""
Test database stuff
"""

import argparse
import os
import logging
import io
import dbc_pyutils.cursor

logger = logging.getLogger(__name__)

def get_docs(stmt, pids_fp, args=None):
    args = args if args else {}
    with dbc_pyutils.cursor.PostgresCursor(os.environ['WORK_PRESENTATION_URL']) as cur:
        # pid_fp = io.StringIO()
        # for _id in pids:
        #    pid_fp.write(f"{_id}\n")
        pids_fp.seek(0)
        cur.execute("CREATE TEMP TABLE pids_tmp(pid TEXT)")
        cur.copy_from(pids_fp, "pids_tmp", columns=["pid"])
        cur.execute(stmt, args)
        for row in cur:
            yield row

def pwork2pids(pids_fp) -> dict:
    """ Creates work -> (pids list) dict by fetching all relevant works from relations table in work-presentation-db """
    logger.info("fetching persistent workids for %d pids", len(pids))
    pw2p = {}
    ## debug
    counter = 0
    for r in get_docs("SELECT wc.manifestationid pid, wo.persistentworkid persistentworkid FROM workobjectv3 wo, workcontainsv3 wc WHERE wo.corepoworkid = wc.corepoworkid AND wc.manifestationid = ANY(SELECT pid FROM pids_tmp)", pids_fp):
        counter = counter + 1
    logger.info("counter is %d", counter)
    ## end debug
    for row in get_docs("SELECT wc.manifestationid pid, wo.persistentworkid persistentworkid FROM workobjectv3 wo, workcontainsv3 wc WHERE wo.corepoworkid = wc.corepoworkid AND wc.manifestationid = ANY(SELECT pid FROM pids_tmp)", pids_fp):
        if row[1] in pw2p:
            pw2p[row[1]].append(row[0])
        else:
            pw2p[row[1]] = [row[0]]
    res = dict(pw2p)
    logger.info("pwork2pids will return dict of size %d", len(res))
    return res

def create_collection(pid_list):
    """
    Harvest rows from work-presentation and creates and indexes solr documents
    """
    logger.info("Retrieving data from db")
    work2pid = pwork2pids(pid_list)
    logger.info("work2pids length is %d", len(work2pid))
    return

def setup_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("pid_list", metavar="pid-list", help="List of pids to include")
    return parser.parse_args()

def main():
    args = setup_args()

    level = logging.INFO
    logging.basicConfig(format="%(asctime)s : %(levelname)s : %(message)s", level=level)
    create_collection(args.pid_list)

if __name__ == "__main__":
    main()
