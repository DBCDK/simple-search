#!/usr/bin/env bash

set -xe

simple-search-service --port 5001  --smart-search search2works.json $SOLR_URL
