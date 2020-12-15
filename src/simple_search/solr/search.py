#!/usr/bin/env python3
import logging
import dbc_pyutils.solr

logger = logging.getLogger(__name__)


class Searcher(object):
    def __init__(self, solr_url):
        self.solr = dbc_pyutils.solr.Solr(solr_url)

    def search(self, phrase, debug=False, *, options:dict={}, rows=10, start=0):
        logger.info(f'Searching for {phrase}')
        query = phrase.strip()
        phonetic_creator_contributor = ""
        if "include-phonetic-creator" in options and options["include-phonetic-creator"]:
            phonetic_creator_contributor = "creator_phonetic^10 contributor_phonetic"

        params = {
            "defType": "edismax",
            "qf": f"creator_exact title_exact creator_and_title creator creator_sort title series contributor subject_dbc subject_synonyms {phonetic_creator_contributor}",
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
