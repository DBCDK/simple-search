#!/usr/bin/env python3
from dataclasses import dataclass
import dbc_pyutils.solr
from simple_search.smartsearch_model import SmartSearch
import logging

logger = logging.getLogger(__name__)


@dataclass
class Option:
    phonetic_creator_contributor: str = ''
    smartsearch: int = 0


def parse_options(options_dict):

    def set_in_options(key):
        if key in options_dict and options_dict[key]:
            return True
        return False

    phonetic_creator_contributor = ""
    if set_in_options("include-phonetic-creator"):
        phonetic_creator_contributor = "creator_phonetic^10 contributor_phonetic"

    smartsearch = 0
    if set_in_options("smart-search"):
        smartsearch = 3

    return Option(phonetic_creator_contributor=phonetic_creator_contributor,
                  smartsearch=smartsearch)


class Searcher(object):
    def __init__(self, solr_url, smartsearch_model_file):
        self.solr = dbc_pyutils.solr.Solr(solr_url)
        self.smartsearch = None
        if smartsearch_model_file:
            logger.info('Searcher initialized with smartsearch')
            self.smartsearch = SmartSearch.load(smartsearch_model_file, self.solr)

    def search(self, phrase, debug=False, *, options: dict = {}, rows=10, start=0):
        query = phrase.strip()

        options['smart-search'] = 3  # HARDCODED value - max number of smartsearch results
        options = parse_options(options)

        if options.smartsearch:
            smartsearch_docs = self.smartsearch.search(query, options.smartsearch)

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
        debug_fields = ["title_alternative", "creator", "workid", "contributor", "work_type"]
        include_fields = ["pids", "title", "language"]

        def doc_iter():
            for doc in smartsearch_docs:
                yield doc
            for doc in self.solr.search(query, **params):
                yield doc

        workids = set()
        for doc in doc_iter():
            if doc['workid'] in workids:
                continue
            workids.add(doc['workid'])
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
