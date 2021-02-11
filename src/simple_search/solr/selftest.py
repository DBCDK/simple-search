#!/usr/bin/env python3

"""
Test database stuff
"""

import argparse
import os
import logging
import dbc_pyutils.cursor

logger = logging.getLogger(__name__)

def get_docs(stmt, pids_fn, args=None):
    args = args if args else {}
    with dbc_pyutils.cursor.PostgresCursor(os.environ['WORK_PRESENTATION_URL']) as cur:
        with open(pids_fn) as pids_fp:
            pids_fp.seek(0)
            cur.execute("CREATE TEMP TABLE pids_tmp(pid TEXT)")
            cur.copy_from(pids_fp, "pids_tmp", columns=["pid"])
            cur.execute(stmt, args)
            for row in cur:
                yield row

def create_collection(pids_fn):
    logger.info("Retrieving data from db")
    counter = 0
    for r in get_docs("SELECT wc.manifestationid pid, wo.persistentworkid persistentworkid FROM workobjectv3 wo, workcontainsv3 wc WHERE wo.corepoworkid = wc.corepoworkid AND wc.manifestationid = ANY(SELECT pid FROM pids_tmp)", pids_fn):
        counter = counter + 1
    logger.info("counter is %d", counter)
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
