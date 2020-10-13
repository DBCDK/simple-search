#!/usr/bin/env python3

import json

import argparse
import pandas as pd
import plotnine as p9
import numpy as np
import os
import requests
from tqdm import tqdm
from search_relevance_eval.seca_2019 import get_all_query_and_rating_dataframes_from_file
import search_relevance_eval.tools as tools
import search_relevance_eval.metrics as metrics
import search_relevance_eval.opensearch_query
import matplotlib.pyplot as plt

def setup_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="url for the search service to evaluate")
    parser.add_argument("data_path", metavar="data-path",
        help="path for the directory containing evaluation data")
    parser.add_argument("output_dir", metavar="output-dir",
        help="directory to write resulting images to")
    return parser.parse_args()

def perform_search(data_path, query_fun):
    """ Perform searches for all queries in testset """
    print("Q", query_fun)
    results = []
    test_dfs = []

    for query, ground_truth_df in tqdm(get_all_query_and_rating_dataframes_from_file(f'{data_path}/master.csv')):
        search_result = query_fun(query)
        pid2work = tools.pid2work(set(search_result + ground_truth_df.pid.tolist()))
        search_result = [pid2work[p] for p in search_result]
        ground_truth_df.pid = ground_truth_df.pid.map(pid2work)
        test_df = tools.combine_search_result_and_ground_truth(search_result, ground_truth_df)
        if len(ground_truth_df) >= 5 and len(test_df) >= 5:
            result = {'query': query,
                      'precision': metrics.precision(ground_truth_df, test_df, k=5),
                      'recall': metrics.recall(ground_truth_df, test_df, k=5),
                      'f-measure': metrics.f_measure(ground_truth_df, test_df, k=5),
                      'nDCG': metrics.dcg(ground_truth_df, test_df, k=10, norm=True)}
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

def show_subset(ratings, results, n=15, cmap=plt.cm.Greens):
    """ Figure of the first n searches """
    plt.rcParams['figure.figsize'] = [20, 20]
    fig, ax = plt.subplots()
    ax.matshow(ratings[:,:n], cmap=cmap)

    for i in range(n):
        for j in range(ratings.shape[0]):
            c = ratings[j,i]
            ax.text(i, j, str(c), va='center', ha='center')

    plt.xticks(range(n), results['query'][:n], rotation='vertical')

def show_all(ratings, results, cmap=plt.cm.Greens):
    """ Figure of all searches """
    plt.rcParams['figure.figsize'] = [20, 20]
    fig, ax = plt.subplots()
    ax.matshow(ratings, cmap=cmap)
    plt.xticks(range(len(results)), results['query'], rotation='vertical')

def simple_search(url, query):
    r = requests.post(url, data=json.dumps({"q": query}))
    r.raise_for_status()
    resp = r.json()
    pids = [d["pids"][0] for d in resp["result"]]
    return pids

def plot_result_stats(results, title):
    stats = results.describe().unstack().reset_index().rename(
        columns={"level_0": "metric", "level_1": "group", 0: "value"})
    stats = stats[~stats["group"].isin(["count", "min", "max"])]
    plot = p9.ggplot(stats) + p9.aes("metric", "value",
        fill="group") + p9.geom_col(position="dodge") +\
        p9.theme_bw() + p9.ggtitle(title)
    return plot

def main():
    args = setup_args()
    os.makedirs(args.output_dir, exist_ok=True)
    search_results, search_test_dfs = perform_search(args.data_path, lambda q: simple_search(args.url, q))
    search_ratings = get_ratings(search_test_dfs)
    plot_simple_search_results = plot_result_stats(search_results, "Simple search")
    plot_simple_search_results.save(os.path.join(args.output_dir, "simple-search-result-stats.png"))

    open_search = search_relevance_eval.opensearch_query.OpenSearch(
        "http://opensearch-5-2-ai-service.cisterne.svc.cloud.dbc.dk/b3.5_5.2/")
    open_search_cisterne_results, open_search_cisterne_test_dfs = perform_search(args.data_path,
        lambda q: [p for p in open_search(q)])
    open_search_cisterne_ratings = get_ratings(open_search_cisterne_test_dfs)
    plot_open_search_results = plot_result_stats(search_results, "Open Search")
    plot_open_search_results.save(os.path.join(args.output_dir, "open-search-result-stats.png"))

    show_subset(search_ratings, search_results)
    plt.savefig(os.path.join(args.output_dir, "subset.png"))

    show_all(search_ratings, search_results)
    plt.savefig(os.path.join(args.output_dir, "all.png"))

    show_subset(open_search_cisterne_ratings, open_search_cisterne_results)
    plt.savefig(os.path.join(args.output_dir, "opensearch-subset.png"))

    show_all(open_search_cisterne_ratings, open_search_cisterne_results)
    plt.savefig(os.path.join(args.output_dir, "opensearch-all.png"))

    print(f"Simple search:\n{search_results.describe()}\nOpen Search:\n{open_search_cisterne_results.describe()}")
