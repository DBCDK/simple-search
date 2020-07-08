#!/usr/bin/env python3

import dbc_pyutils.solr

class Searcher(object):
    def __init__(self, solr_url):
        self.solr = dbc_pyutils.solr.Solr(solr_url)

    def search(self, phrase, debug=False):
        phrase = phrase.strip()
#        combined_creator_title_query = f'(creator_and_title:"{phrase}"^250 OR creator_and_title:({phrase})^100)'
#        title_search_query = f'(meta_title:"{phrase}"^250 OR ({make_truncated_query(phrase, "title")})^100 OR meta_title:({phrase})^10 OR meta_title:({phrase}~1)^5)',
#        creator_search_query = f'(creator:"{phrase}"^250 OR ({make_truncated_query(phrase, "creator")})^100 OR creator:({phrase})^10 OR creator:({phrase}~1)^5 OR contributor:"{phrase}"^250 OR ({make_truncated_query(phrase, "contributor")})^100 OR contributor:({phrase})^10 OR contributor:({phrase}~1)^1)',
#        query = f"{combined_creator_title_query} OR {title_search_query} OR {creator_search_query}"
        query = phrase

        params = {
            "defType": "edismax",
            "qf": "creator_exact^200 title_exact^150 creator_and_title^100 creator^10 title^10 creator_phonetic^10 contributor contributor_phonetic subject_dbc",
            "pf": "creator^100 title^100 contributor^50 subject_dbc",
            "bq": "{!edismax qf=creator v=$q bq=}^10",
            "fl": "pids,title,creator,contributor,workid,work_type,language,pid_to_type_map,score",
            "sort": "score desc",
            "boost": "n_pids",
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
