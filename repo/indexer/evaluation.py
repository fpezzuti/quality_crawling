
from typing import List
import ir_measures
from ir_measures import nDCG, MRR, R
import pandas as pd

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.config import config
from indexer.dataset import MSMarcoWebSearch, ResearchyQuestions

from tqdm import tqdm

metric_map = {
    'ndcg@10': nDCG@10,
    'mrr@10': MRR@10,
    'r@100': R@100  
}

"""
    Parse the metric names from the configuration file to the corresponding IR measures suitable for the ir_measures package.
"""
def parse_ireval_metric_names(list_metrics: List) -> List:
    parsed_metrics = []
    for metric_name in list_metrics:
        parsed_metrics.append(metric_map[metric_name])
    print("parsed_metrics...", parsed_metrics)

    return parsed_metrics

METRICS = parse_ireval_metric_names(config.get('evaluation').get('metrics'))
BENCHMARKS = list(config.get('evaluation_benchmarks').keys())

"""
    Print aggregated IR evaluation metrics in a table format.
"""
def print_aggregated_metrics_table(results, experiment_name: str):
    df = pd.DataFrame.from_dict(results.items()).T
    df.columns =list(results.keys())
    df = df.iloc[1:]
    df.index =[experiment_name]
    return df

"""
    Compute the specified IR evaluation metrics for a given run in TREC format.
    - If aggregate is True, the function returns metrics aggregated for the benchmark specified
    - Otherwise, it returns the metrics for each query.
"""
def compute_metrics_from_qrels(path_to_run: str, benchmark_name: str, metrics: list, aggregate: bool = True):
    if benchmark_name == "msmarco-ws":
        qrels = MSMarcoWebSearch(benchmark=benchmark_name).get_qrels()
    elif benchmark_name == "rq":
        qrels = ResearchyQuestions(benchmark=benchmark_name).get_qrels()
    else:
        raise ValueError(f"Error, unsupported benchmark: {benchmark_name}")
    
    run = ir_measures.read_trec_run(path_to_run)
    
    if aggregate:
        return ir_measures.calc_aggregate(metrics, qrels, run)
    
    return ir_measures.iter_calc(metrics, qrels, run)


"""
    Calculate IR evaluation metrics of a given run and print them in a table format (aggregated), or return the dataframe of metrics (unaggregates)."""
def perform_evaluation(run_path: str, benchmark_name: str, experiment_name: str, aggregate = True):
    results = compute_metrics_from_qrels(run_path, benchmark_name, METRICS, aggregate=aggregate)
    if aggregate:
        df = print_aggregated_metrics_table(results, experiment_name)
    else:
        results_dict = {}
        for result in tqdm(results, desc=f"Computing IR metrics on {benchmark_name}"):
            if result.query_id not in results_dict:
                results_dict[result.query_id] = {f"{result.measure}": result.value}
            else:
                results_dict[result.query_id][f"{result.measure}"] = result.value

            df = pd.DataFrame.from_dict(results_dict).T
            df = df.reset_index()
            df = df.rename(columns={'index': 'qid'})
    return df

"""
    Evaluate multiple runs and return the aggregated metrics in a table format.
"""
def evaluate_multiple_runs(run_paths: list, run_names: list, benchmark_name: str): 
    df_concat = []

    for run_path, run_name in zip(run_paths, run_names):        
        try:
            df = perform_evaluation(run_path, benchmark_name, run_name)
        except Exception as e:
            print(f"Error: {e}")
            print(f"Error in {benchmark_name}")
            print(f"Error in {run_path}")
            print(f"Error in {run_name}")
            return None
        df["run"] = run_name
        df_concat.append(df)
        
    # concat all the dataframes for printing
    results = pd.concat(df_concat)
    results.columns =[str(col_name) for col_name in df_concat[0].columns]
    results = results[sorted(results.columns)]
    cols =[str(col_name) for col_name in results.columns]
    results = results[cols]

    return results