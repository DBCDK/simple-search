#!/usr/bin/env python3

import dbc_pyutils.solr

class Searcher(object):
    def __init__(self, solr_url):
        self.solr = dbc_pyutils.solr.Solr(solr_url)

    def search(self, phrase, debug=False):
        phrase = phrase.strip()
        title_search_query = f'(meta_title:"{phrase}"^250 OR ({make_truncated_query(phrase, "title")})^100 OR meta_title:({phrase})^10 OR meta_title:({phrase}~1)^5)',
        creator_search_query = f'(creator:"{phrase}"^250 OR ({make_truncated_query(phrase, "creator")})^100 OR creator:({phrase})^10 OR creator:({phrase}~1)^5 OR contributor:"{phrase}"^250 OR ({make_truncated_query(phrase, "contributor")})^100 OR contributor:({phrase})^10 OR contributor:({phrase}~1)^1)',
        query = f"{title_search_query} OR {creator_search_query}"

        params = {
            "defType": "edismax",
            "bq": "{!edismax qf=creator v=$q bq=}^5",
            "bq": "{!edismax qf=title v=$q bq=}^10",
            "fl": "pids,title,creator,contributor,workid"
        }
        debug_fields = ["title_alternative", "creator", "workid", "contributor"]
        include_fields = ["pids", "title"]
        for doc in self.solr.search(query, **params):
            result_doc = {f: doc[f] for f in include_fields if f in doc}
            if debug:
                debug_object = {f: doc[f] for f in debug_fields if f in doc}
                result_doc["debug"] = debug_object
            yield result_doc

def make_truncated_query(query, field):
    """
        Constructs querys like 'title:histori* AND title:om* AND title:e*'.
        The purpose of this is to find documents like 'Historien om en havn'
    """
    return " AND ".join([f"{field}:{s}*" for s in query.split()])
