#!/usr/bin/env python3

import argparse
import dataclasses
import enum
import json
import re
import sys
import urllib.parse

import requests

def setup_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="Url for searcher to test")
    parser.add_argument("search_result_config", metavar="search-result-config")
    return parser.parse_args()

class TestResultMode(enum.Enum):
    SUCCESS = 1
    FAILURE = 2

@dataclasses.dataclass
class TestResult:
    query: str
    message: str
    mode: TestResultMode

class Searcher(object):
    def __init__(self, url, config):
        self.url = url
        self.config = config
        self.session = requests.Session()

    def test_searches(self):
        test_results = []
        for query_spec in self.config["queries"]:
            query = urllib.parse.quote_plus(query_spec["q"])
            response = self.session.post(f"{self.url}/search", data=json.dumps({"q": query, "debug": True}))
            response.raise_for_status()
            workid_results = {item["debug"]["workid"]: i+1 for i, item in enumerate(response.json()["result"])}
            for w, pos in query_spec["workids"].items():
                pos_range = re.search("(\d+)-?(\d*)", pos)
                start = int(pos_range.groups()[0])
                end_str = pos_range.groups()[1]
                if end_str == "":
                    end = start + 1
                else:
                    end = int(end_str) + 1
                if w in workid_results:
                    if workid_results[w] in range(start, end):
                        test_results.append(TestResult(query, "", TestResultMode.SUCCESS))
                    else:
                        test_results.append(TestResult(query,
                            f"Position {workid_results[w]} for work {w} not in the expected range ({range(start, end)})",
                            TestResultMode.FAILURE))
                else:
                    test_results.append(TestResult(query, f"Work {w} not found in the search response", TestResultMode.FAILURE))
        return test_results

def main():
    args = setup_args()
    with open(args.search_result_config) as fp:
        config = json.load(fp)
    searcher = Searcher(args.url, config)
    results = searcher.test_searches()
    failed_tests = [t for t in results if t.mode == TestResultMode.FAILURE]
    if any(failed_tests):
        for t in failed_tests:
            print(f"Search for {t.query} failed: {t.message}")
        sys.exit(1)

if __name__ == "__main__":
    main()
