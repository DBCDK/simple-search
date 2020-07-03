#!/usr/bin/env python3

import argparse
import os
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
    parser.add_argument('-f', '--holdings-filename',
                        dest='holdingsfilename',
                        required=True,
                        help='filename for the holdingsdata. May both be a json or a json-gzipped file.')
    parser.add_argument('-o', '--outfile',
                        dest='pid_file',
                        required=True,
                        help='File containing pids. One pid at each line')
    args = parser.parse_args()
    return args


def main():
    args = cli()
    with open(args.pid_file, 'w') as of:
        for pid in get_randers_pids(args.holdingsfilename):
            of.write(pid + '\n')
            
    
if __name__ == '__main__':
    main()
