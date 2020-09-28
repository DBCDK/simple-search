#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# -*- mode: python -*-
"""
============
Synonym List
============

Creates synonym list from synonym records in corepo.

the synonyms are commaseparated list like this:

    bofællesskaber,boligfællesskaber
    effektmetre,powermetre
    maskulinum,hankøn
    processtyresystemer,pps
    tegnsprogstolke,døvetolke
    ...

"""
from collections import defaultdict
import os
import logging
from tqdm import tqdm
from dbc_pyutils import PostgresDictCursor
from lxml import etree


logger = logging.getLogger(__name__)

NSMAP = {'ting': 'http://www.dbc.dk/ting',
         'dkabm': 'http://biblstandard.dk/abm/namespace/dkabm/',
         'ac': 'http://biblstandard.dk/ac/namespace/',
         'dkdcplus': 'http://biblstandard.dk/abm/namespace/dkdcplus/',
         'oss': 'http://oss.dbc.dk/ns/osstypes',
         'dc': 'http://purl.org/dc/elements/1.1/',
         'dcterms': 'http://purl.org/dc/terms/',
         'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
         'docbook': 'http://docbook.org/ns/docbook',
         'marcx': 'info:lc/xmlns/marcxchange-v1'}


def fetch_subject_records(limit=None):
    """ Get all subject records from corepo """
    logger.info('Fetching subject records')

    with PostgresDictCursor(os.environ['LOWELL_URL']) as cur:
        stmt = """SELECT pid
                       FROM relations
                       WHERE pid LIKE '%-emne:%'"""
        if limit:
            stmt += f" LIMIT {limit}"
        cur.execute(stmt)
        pids = [row['pid'] for row in cur]
    with PostgresDictCursor(os.environ['COREPO_URL']) as cur:
        cur.execute("""SELECT pid, content
                       FROM streams
                       WHERE name='commonData'
                       AND pid in %(pids)s""", {'pids': tuple(pids)})
        for row in tqdm(cur, ncols=140, total=len(pids)):
            xml = etree.fromstring(row['content'])
            yield row['pid'], xml


def isolate_tags(pid, xml):
    """ isolate relevant tags """
    tags = defaultdict(list)
    tags['name'] = pid
    nodes = xml.xpath("/ting:container/marcx:collection/record/datafield", namespaces=NSMAP)
    for node in nodes:
        tag = node.attrib.get('tag', '')
        if tag.startswith('1') or tag.startswith('4') or tag.startswith('5'):
            for child in node:
                tags[tag].append(child.text)
                tags['tag'].append(tag[0])
    return dict(tags)


def make_synonyms(record_tags, outfile='synonyms.txt'):
    logger.info('Making synonym list')
    synonym_list = []
    for sub in record_tags:
        if 'tag' in sub and '4' in sub['tag'] and '410' not in sub and '100' not in sub:
            target = None
            source = None
            for key in sub.keys():
                if key.startswith('4'):
                    source = key
                elif key.startswith('1'):
                    target = key
            target = sub[target][0].lower()
            s = [s.lower() for s in sub[source] if target != s.lower() and s != 'brug']
            if s:
                synonym_list.append(",".join([target] + s))
    synonym_list = list(set(synonym_list))
    synonym_list.sort()
    with open(outfile, 'w') as fh:
        for line in synonym_list:
            fh.write(line + '\n')


def make_synonym_list(outfile='synonyms.txt'):
    """ Make synonym lists and write them to disc"""
    record_tags = [isolate_tags(pid, xml) for pid, xml in fetch_subject_records()]
    make_synonyms(record_tags, outfile)


class Synonyms:

    def __init__(self, synonym_file):

        self.synonyms = {}
        with open(synonym_file) as fh:
            for line in fh:
                # strip phrases og newlines and 'turtles'
                syns = [s.strip().replace('¤', '') for s in line.split(',')]
                syn_set = set(syns)
                for s in syns:
                    self.synonyms[s] = list(syn_set - set([s]))

    def __getitem__(self, word):
        return self.synonyms[word]

    def get(self, word, default):
        return self.synonyms.get(word, default)


def cli():
    """ Commandline interface """
    import argparse

    parser = argparse.ArgumentParser(description='Create synonym list')
    parser.add_argument('-o', '--ouput-file', dest='output_file',
                        help='output-file. default is synonyms.txt', default='synonyms.txt')
    args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.DEBUG)
    make_synonym_list(outfile=args.output_file)


if __name__ == '__main__':
    cli()
