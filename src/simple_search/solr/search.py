#!/usr/bin/env python3
from dataclasses import dataclass
from collections import namedtuple
import dbc_pyutils.solr
from simple_search.smartsearch import SmartSearch, CuratedSearch
import logging

logger = logging.getLogger(__name__)


SmartSearchData = namedtuple('SmartSearch', 'query bf')

@dataclass
class Option:
    phonetic_creator_contributor: str = ''
    smartsearch: int = 0
    curated_search: bool = False
    def __str__(self):
        return f"<Options - phonetic_creator_contributor='{self.phonetic_creator_contributor}', smartsearch={self.smartsearch}, curated_search={self.curated_search}>"


def parse_options(options_dict):

    def set_in_options(key):
        if key in options_dict and options_dict[key]:
            return True
        return False

    phonetic_creator_contributor = ""
    if set_in_options("include-phonetic-creator"):
        phonetic_creator_contributor = "creator_phonetic^10 contributor_phonetic"

    smartsearch = 0
    if set_in_options("include-smartsearch"):
        smartsearch = 3

    curated_search = False
    if set_in_options("include-curatedsearch"):
        curated_search = True

    return Option(phonetic_creator_contributor=phonetic_creator_contributor,
                  smartsearch=smartsearch,
                  curated_search=curated_search)


logger = logging.getLogger(__name__)


def create_bf(workids):
    """ Thew constructed bf string will boost the smartsearch results to the top """
    bval = 1000
    bf = '1'
    for w in workids[::-1]:
        bval += 1000
        bf = f'if(termfreq(workid,"{w}"),{bval},{bf})'
    return bf


class Searcher(object):
    def __init__(self, solr_url, smartsearch_model_file=None, curated_search_file=None):
        self.solr = dbc_pyutils.solr.Solr(solr_url)
        self.smartsearch = None
        if smartsearch_model_file:
            logger.info('Searcher initialized with smartsearch')
            self.smartsearch = SmartSearch.load(smartsearch_model_file, self.solr)
        self.curated_search = CuratedSearch({})
        if curated_search_file:
            logger.info('Searcher initialized with curated search')
            self.curated_search = CuratedSearch.load(curated_search_file)

    def search(self, phrase, debug=False, *, options: dict = {}, rows=10, start=0):
        logger.info(f'Searching for {phrase}')
        query = phrase.strip()
        options = parse_options(options)
        logger.info(f"search options {options}")

        smartsearch = None
        if options.smartsearch:
            smartsearch_workids = self.smartsearch.get(query, options.smartsearch)
            if smartsearch_workids:
                smartsearch = SmartSearchData("(" + " OR ".join([f'workid:"{w}"' for w in smartsearch_workids]) + ") OR ",
                                              create_bf(smartsearch_workids))
        params = {
            "defType": "edismax",
            "qf": f"creator_exact title_exact creator_and_title creator creator_sort title series contributor subject_dbc subject_synonyms {options.phonetic_creator_contributor}",
            "pf": "creator_exact^200 creator^100 creator_sort^100 creator_and_title^100 title_exact^100 title^100 series^75 contributor^50 subject_dbc subject_synonyms",
            "bq": [
                "years_since_publication:[0 TO 10]^5",
                "language:dan^5",
            ],
            "fl": "pids,title,creator,contributor,workid,work_type,language,pid_to_type_map,score",
            "sort": "score desc",
            # Submitting multiple values can be achived by specifying lists.
            # "boost": ["holdings", "popularity"] will result in &boost=holdings&boost=popularity
            "boost": ["holdings", "popularity"],
            "rows": rows,
            "start": start,
        }

        if smartsearch:
            params['bf'] = smartsearch.bf

        debug_fields = ["title_alternative", "creator", "workid", "contributor", "work_type"]
        include_fields = ["pids", "title", "language"]

        if options.curated_search:
            retain = {'defType': params['defType'], 'fl': params['fl'], 'sort': params['sort'], 'start': params['start'], 'rows': params['rows']}
            if 'bf' in params:
                retain['bf'] = params['bf']
            query, params = self.curated_search(query)

            params.update(retain)

        if smartsearch:
            query = smartsearch.query + query
        for doc in self.solr.search(query, **params):
            result_doc = {f: doc[f] for f in include_fields if f in doc}
            result_doc["pid_details"] = parse_pid_to_type_map(doc["pid_to_type_map"])
            if debug:
                debug_object = {f: doc[f] for f in debug_fields if f in doc}
                result_doc["debug"] = debug_object
            yield result_doc


def parse_pid_to_type_map(content):
    """
    Parses content of a solr_pid_to_type_map field into a desired response structure
    """
    def map_entry(entry):
        pid, collections, mattype = entry.split(":::")
        return {"pid": pid, "type": mattype}
    return [map_entry(entry) for entry in content]


def make_truncated_query(query, field):
    """
        Constructs querys like 'title:histori* AND title:om* AND title:e*'.
        The purpose of this is to find documents like 'Historien om en havn'
    """
    return " AND ".join([f"{field}:{s}*" for s in query.split()])
