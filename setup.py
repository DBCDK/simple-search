#!/usr/bin/env python3

from setuptools import setup

setup(name="simple-search",
    version="0.1.0",
    package_dir={"": "src"},
    packages=["simple_search", "simple_search.solr"],
    description="",
    provides=["simple_search"],
    install_requires=["booklens", "dbc-pyutils", "mobus", "tornado", "tqdm"],
    entry_points=
        {"console_scripts": [
            "simple-search-service = simple_search.service:main",
            "solr-indexer = simple_search.solr.indexer:main",
            "pid-list-generator = simple_search.solr.pid_fetcher:main",
        ]}
    )
