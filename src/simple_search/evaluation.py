#!/usr/bin/env python3

import json

import argparse
import pandas as pd
import plotnine as p9
import numpy as np
import os
import requests
from tqdm import tqdm
from search_relevance_eval.seca_2019 import get_all_query_and_rating_dataframes_from_url
from search_relevance_eval.seca_2019 import get_all_query_and_rating_dataframes_from_file
import search_relevance_eval.tools as tools
import search_relevance_eval.metrics as metrics
import search_relevance_eval.opensearch_query
import matplotlib.pyplot as plt

def setup_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="url for the search service to evaluate")
    parser.add_argument("--data-path",
        help="path for the directory containing evaluation data")
    parser.add_argument("output_dir", metavar="output-dir",
        help="directory to write resulting images to")
    return parser.parse_args()

def perform_search(query_fun, queries_and_dataframes):
    """ Perform searches for all queries in testset """
    print("Q", query_fun)
    results = []
    test_dfs = []

    performed_queries = set()
    for query, ground_truth_df in tqdm(queries_and_dataframes):
        # Avoid changing the original dataframe
        local_ground_truth_df = ground_truth_df.copy()
        if not isinstance(query, str):
            print(f"Ignoring invalid query {query}")
            continue
        query = query.lower()
        if query in performed_queries:
            continue
        performed_queries.add(query)

        search_result = query_fun(query)
        pid2work = tools.pid2work(set(search_result + local_ground_truth_df.pid.tolist()))
        search_result = [pid2work[p] for p in search_result]
        local_ground_truth_df.pid = local_ground_truth_df.pid.map(pid2work)
        test_df = tools.combine_search_result_and_ground_truth(search_result, local_ground_truth_df)
        if len(local_ground_truth_df) > 0 and len(test_df) > 0:
            result = {'query': query,
                      'precision': metrics.precision(local_ground_truth_df, test_df, k=5),
                      'recall': metrics.recall(local_ground_truth_df, test_df, k=5),
                      'f-measure': metrics.f_measure(local_ground_truth_df, test_df, k=5),
                      'nDCG': metrics.dcg(local_ground_truth_df, test_df, k=10, norm=True)}
        else:
            result = {'query': query,
                      'precision': 0.0,
                      'recall': 0.0,
                      'f-measure': 0.0,
                      'nDCG': 0.0}
            print(f"Query {query} had too few results: {len(local_ground_truth_df)} : {len(test_df)}")
        results.append(result)
        test_dfs.append(test_df)

    results = pd.DataFrame(results, columns=['query', 'precision', 'recall', 'f-measure', 'nDCG'])
    return results, test_dfs

def get_ratings(test_dfs, p_len=15):
    """ Retrieve ratings from test dataframes """
    ratings_ = [df['rating'].to_list() for df in test_dfs]

    def pad(lst, p_len=15, p_val=-1):
        lst = lst[:p_len]
        pad_ = [p_val] * (p_len - len(lst))
        return lst + pad_

    ratings_ = np.array([pad(lst, p_len=p_len) for lst in ratings_]).astype(float)
    ratings_ = ratings_.T
    return ratings_

def show_subset(ratings, results, n=15, cmap=plt.cm.Greens, size=20):
    """ Figure of the first n searches """
    plt.rcParams['figure.figsize'] = [size, size]
    fig, ax = plt.subplots()
    ax.matshow(ratings.T[:n], cmap=cmap, vmin=-1, vmax=2)

    for i in range(n):
        for j in range(ratings.T[:n].shape[0]):
            c = ratings.T[j,i]
            ax.text(i, j, str(c), va='center', ha='center')

    plt.yticks(range(n), results['query'][:n], rotation="horizontal")

def show_all(ratings, results, cmap=plt.cm.Greens, size=20):
    """ Figure of all searches """
    plt.rcParams['figure.figsize'] = [size, size]
    fig, ax = plt.subplots()
    ax.matshow(ratings.T, cmap=cmap, vmin=-1, vmax=2)
    plt.yticks(range(len(results)), results['query'], rotation="horizontal")

def simple_search(url, query, rows=10):
    r = requests.post(url, data=json.dumps({"q": query, "rows": rows, "options": {"include-smartsearch": True}}))
    r.raise_for_status()
    resp = r.json()
    pids = [d["pids"][0] for d in resp["result"]]
    return pids

def plot_result_stats(results, title):
    stats = results.describe().unstack().reset_index().rename(
        columns={"level_0": "metric", "level_1": "group", 0: "value"})
    stats = stats[~stats["group"].isin(["count", "min", "max"])]
    stats["value_presentation"] = round(stats["value"], 2)
    plot = (p9.ggplot(stats) +
        p9.aes("metric", "value", fill="group") +
        p9.geom_col(position="dodge") +
        p9.theme_bw() +
        p9.coord_cartesian(ylim=[0, 1.0]) +
        p9.ggtitle(title) +
        p9.geom_text(p9.aes(label="value_presentation"),
        position=p9.position_dodge(width=0.9), va="bottom")
    )
    return plot

def main():
    args = setup_args()
    os.makedirs(args.output_dir, exist_ok=True)
    if args.data_path is not None:
        data_generator = get_all_query_and_rating_dataframes_from_file(
            f"{args.data_path}/master.csv")
    else:
        data_generator = get_all_query_and_rating_dataframes_from_url()
    queries_and_dataframes = list(data_generator)
    search_results, search_test_dfs = perform_search(lambda q: simple_search(args.url, q, 15),
        queries_and_dataframes)
    search_ratings = get_ratings(search_test_dfs)

    img_save_args = {"width": 10, "height": 7.5, "dpi": 175}
    plot_simple_search_results = plot_result_stats(search_results, "Simple search")
    plot_simple_search_results.save(os.path.join(args.output_dir,
        "simple-search-result-stats.png"), **img_save_args)

    open_search = search_relevance_eval.opensearch_query.OpenSearch(
        "http://opensearch-5-2-ai-service.cisterne.svc.cloud.dbc.dk/b3.5_5.2/")
    open_search_cisterne_results, open_search_cisterne_test_dfs = perform_search(
        lambda q: [p for p in open_search(q)], queries_and_dataframes)
    open_search_cisterne_ratings = get_ratings(open_search_cisterne_test_dfs)
    plot_open_search_results = plot_result_stats(open_search_cisterne_results,
        "Open Search")
    plot_open_search_results.save(os.path.join(args.output_dir,
        "open-search-result-stats.png"), **img_save_args)

    save_fig = lambda name: plt.savefig(os.path.join(args.output_dir, name), bbox_inches="tight")

    show_subset(search_ratings, search_results, size=10)
    save_fig("subset.png")

    show_all(search_ratings, search_results, size=10)
    save_fig("all.png")

    show_subset(open_search_cisterne_ratings, open_search_cisterne_results, size=10)
    save_fig("opensearch-subset.png")

    show_all(open_search_cisterne_ratings, open_search_cisterne_results, size=10)
    save_fig("opensearch-all.png")

    print(f"Simple search:\n{search_results.describe()}\nOpen Search:\n{open_search_cisterne_results.describe()}")
