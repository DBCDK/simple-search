#!/usr/bin/env bash

set -xe

simple-search-service --port 5001  --smart-search search2works.json --curated-search curated-searches.jsonl $SOLR_URL
