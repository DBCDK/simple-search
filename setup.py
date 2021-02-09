#!/usr/bin/env python3

from setuptools import setup

setup(name="simple-search",
    version="0.1.0",
    package_dir={"": "src"},
    packages=["simple_search", "simple_search.solr"],
    description="",
    provides=["simple_search"],
    install_requires=["booklens", "dbc-pyutils", "joblib", "mobus", "numpy", "pandas", "tornado", "tqdm", "plotnine"],
    include_package_data=True,
    entry_points=
        {"console_scripts": [
            "simple-search-service = simple_search.service:main",
            "solr-indexer = simple_search.solr.indexer:main",
            "wp-solr-indexer = simple_search.solr.indexer_work_presentation:main",
            "pid-list-generator = simple_search.solr.pid_fetcher:main",
            "generate-work-to-holdings-map = simple_search.solr.indexer:generate_work_to_holdings_map",
            "generate-synonym-list = simple_search.synonym_list:cli",
            "evaluate-search = simple_search.evaluation:main",
        ]}
    )
