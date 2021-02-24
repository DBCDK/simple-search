#!/usr/bin/env python3

import argparse
import os
import sys

from booklens.pid2work_mappings import bibdk_pid2work_map
from booklens.pid2work_mappings import work2pids_map
from dbc_pyutils.cursor import PostgresCursor


def get_randers_holdings_pids(holdingsfilename):
    randers_agencyid = '773000'
    work2pids, pids2agency = work2pids_map(holdingsfilename, randers_agencyid)
    pids = {pid for pid in pids2agency.keys()}
    return pids


def get_filmstriben_pids():
    lowell_url = os.environ['LOWELL_URL']
    with PostgresCursor(lowell_url) as lowell:
        stmt = """SELECT pid 
                  FROM metadata
                  WHERE metadata->'collection' ?| array ['150021-sofa', '150021-fjern']"""
        lowell.execute(stmt)
        pids = {row[0] for row in lowell.fetchall()}
    return pids


def get_ereolen_pids():
    lowell_url = os.environ['LOWELL_URL']
    with PostgresCursor(lowell_url) as lowell:
        stmt = """SELECT pid 
                  FROM metadata
                  WHERE metadata->'collection' ?| array ['150015-ereol', '150015-erelicchld', '150015-nlychld', '150015-nlylic', '150015-netlydbog', '150015-nlylicchld', '150015-erelic', '150015-ereolchld']"""
        lowell.execute(stmt)
        pids = {row[0] for row in lowell.fetchall()}
    return pids
            

def get_randers_pids(holdingsfilename):
    holdings_pids = get_randers_holdings_pids(holdingsfilename)
    filmstriben_pids = get_filmstriben_pids()
    ereolen_pids = get_ereolen_pids()
    return holdings_pids | filmstriben_pids | ereolen_pids


def cli():
    parser = argparse.ArgumentParser('')
    parser.add_argument("source", help="The source for which to get pids",
        choices=["bibdk", "randers", "corepo-workids"])
    parser.add_argument('-f', '--holdings-filename',
                        dest='holdingsfilename',
                        help='filename for the holdingsdata. May both be a json or a json-gzipped file.')
    parser.add_argument('-o', '--outfile',
                        dest='pid_file',
                        required=True,
                        help='File containing pids. One pid at each line')
    args = parser.parse_args()
    return args

def main():
    args = cli()
    pid_list = []
    if args.source == "bibdk":
        pid2work, _ = bibdk_pid2work_map()
        pid_list = {p for p in pid2work.keys()}
    elif args.source == "randers":
        if args.holdingsfilename is None:
            print(f"Holdings file is required if source is randers",
                file=sys.stderr)
            sys.exit(1)
        pid_list = get_randers_pids(args.holdingsfilename)
    elif args.source == "corepo-workids":
        pid2work, _ = bibdk_pid2work_map()
        work_list_duplicates = {p for p in pid2work.values()}
        work_set = set(work_list_duplicates)
        pid_list = list(work_set)
    else:
        print(f"Unknown source {args.source}", file=sys.stderr)
        sys.exit(1)
    with open(args.pid_file, 'w') as of:
        for pid in pid_list:
            of.write(pid + '\n')
            
if __name__ == '__main__':
    main()
