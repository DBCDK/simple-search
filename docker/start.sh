#!/usr/bin/env bash

set -xe

simple-search-service --verbose --ab-id 1 --port 5000 $SOLR_URL
