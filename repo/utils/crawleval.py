import pandas as pd
import math
from tqdm import tqdm
from utils.config import config
from indexer.dataset import MSMarcoWebSearch, ResearchyQuestions, Downloads

DOWNLOAD_PAGES_DIR = config.get('paths').get('downloaded_pages_dir')

def _maxndcg(num_rel: int) -> float:
    """
        Compute MaxNDCG for a given number of relevant documents.
    """
    return sum(1 / math.log2(i + 1) for i in range(1, num_rel+1))

def _calc_cmetric(target_metric: str, downloaded_relevant: int, downloaded: int, relevant: int) -> float:
    """
        Compute the crawling metric based on the target metric and return it as a float value.
        
        Args:
            target_metric: string identifier of the target metric
            downloaded_relevant: number of downloaded relevant documents
            downloaded: number of downloaded documents
            relevant: number of relevant documents
    """
    if target_metric == 'harvest':
        metric = (downloaded_relevant/downloaded)
    elif target_metric == 'recall':
        metric = (downloaded_relevant/relevant)
    elif target_metric == 'irr_ratio':
        metric = ((downloaded - downloaded_relevant) / downloaded)
    elif target_metric == 'nrel':
        metric = downloaded_relevant
    return metric

def _crawling_stats(downl_docids: list, relevant_set: set, target_metrics: list, tested_limits: list) -> dict:
    """
        Given a list of downloaded documents and the set of groundtruth relevant documents,
        compute the crawling metrics for each maximum limit (i.e., for the first lim downloaded documents).
    
        Args:
            downl_docids: list of downloaded document ids
            relevant_set: set of relevant document ids
            target_metrics: list of target metrics
            tested_limits: list of tested limits
    """
    start_idx = 0
    last_downloaded_rel = 0
    all_cmetrics = []
    print("Target metrics: ", target_metrics)
    print("Tested limits: ", tested_limits)
    print("Number of downloaded documents: ", len(downl_docids))
    print("Relevant documents: ", len(relevant_set))

    for limit in tqdm(tested_limits, total=len(tested_limits), desc=f"Processing limits", unit='limit'):
        newly_downloaded_set = set(downl_docids[start_idx:limit]) # new subset of downloaded documents

        downloaded_relevant = len(relevant_set.intersection(newly_downloaded_set)) + last_downloaded_rel            

        crawl_metrics = {}
        for metric_name in target_metrics:
            cmetric = _calc_cmetric(target_metric=metric_name, downloaded_relevant=downloaded_relevant, downloaded=limit, relevant=len(relevant_set))
            crawl_metrics[metric_name] = cmetric

        start_idx = limit
        last_downloaded_rel = downloaded_relevant
        all_cmetrics.append(crawl_metrics)
    return all_cmetrics

def _load_downloads(exp_name: str, max_limit: int, collection_name: str = "cw22b") -> list:
    """
        Load the downloaded document ids for a given experiment and collection.

        Args:
            exp_name: string identifier of the experiment
            max_limit: maximum limit of downloaded documents
            collection_name: string identifier of the collection
    """
    path_to_downloaded = f"{DOWNLOAD_PAGES_DIR}/{exp_name}/"
    downl_docids = Downloads(collection_name).load_downloads_docids(data_dir=path_to_downloaded, limit=max_limit)
    return downl_docids

def _load_qrels_in_df(query_sets: list, target_metrics: list) -> pd.DataFrame:
    qrels = []

    for benchmark in query_sets:
        if benchmark == "msmarco-ws":
            qrels_dataset = MSMarcoWebSearch(benchmark=benchmark)
          
        elif benchmark == "rq":
            qrels_dataset = ResearchyQuestions(benchmark=benchmark)
        else:
            raise ValueError(f"Unknown benchmark: {benchmark}")
        
        qrels_df = qrels_dataset.get_qrels()
        if "maxndcg" not in target_metrics:
            qrels_df.drop('query_id', axis=1, inplace=True)
        qrels.append(qrels_df)
  
    qrels_df = pd.concat(qrels)
    qrels_df.drop_duplicates(ignore_index=True, inplace=True)
    return qrels_df

def evaluate_crawling_metrics(query_sets: list, tested_exps: dict, tested_limits: list, collection_name: str = "cw22b", compute_ub: bool = False, target_metrics: list = ["harvest"], aggregate: bool =True) -> dict:
    """
        Evaluate crawling metrics for a set of experiments and query sets.
        
        Args:
            query_sets: list of string identifiers of query sets
            tested_exps: dictionary of tested experiments {experiment string id: printable name}
            tested_limits: list of int limits
            collection_name: string identifier of the collection
            compute_ub: boolean flag to compute the upper bound
            target_metrics: list of target metrics
            aggregate: boolean flag to aggregate the results for all the queries
        """
    print(f"Evaluating crawling metrics on query sets: {query_sets}")

    # load qrels in a dataframe
    qrels_df = _load_qrels_in_df(query_sets=query_sets, target_metrics=target_metrics)

    ubs_hr = []
    all_crawl_metrics = {}

    # get the set of docids of relevant documents
    relevant_set = set(qrels_df['doc_id'])

    print("Number of relevant documents in qrels: ", len(relevant_set))

    assert len(relevant_set) == len(qrels_df['doc_id'].unique()), "Error. Length mismatch for relevant documents."

    if compute_ub:
        n_relevant = len(qrels_df['doc_id'].unique())
        assert len(qrels_df) == n_relevant, "Error: qrels_df contains some duplicates."
    
    for exp_name in tested_exps.keys(): # iterate over the experiments (crawlers)
        all_crawl_metrics[exp_name] = []

        # load the list of downloads (max limit)
        downl_docids = _load_downloads(exp_name=exp_name, max_limit=max(tested_limits), collection_name=collection_name)

        if len(downl_docids) < max(tested_limits):
            tested_limits[tested_limits.index(max(tested_limits))] = len(downl_docids)


        if ("harvest" in target_metrics) or ("recall" in target_metrics) or ("irr_ratio" in target_metrics) or ("nrel" in target_metrics):
            all_crawl_metrics[exp_name] = _crawling_stats(downl_docids=downl_docids, relevant_set=relevant_set, target_metrics=target_metrics, tested_limits=tested_limits)

        if "maxndcg" in target_metrics:
            maxndcg_at_limits = []
            qids = qrels_df['query_id'].unique()
            for limit in tested_limits:
                downloaded_set = set(downl_docids[:limit])

                all_maxndcg = []
                for query_id in qids:
                    relevant = set(qrels_df[qrels_df['query_id'] == query_id]["doc_id"].unique().tolist())
                    downloaded_relevant = len(relevant.intersection(downloaded_set))
                    all_maxndcg.append(_maxndcg(downloaded_relevant))
                
                if aggregate:
                    maxndcg_at_limits.append(sum(all_maxndcg)/len(all_maxndcg))
                else:
                    maxndcg_at_limits.append(all_maxndcg)
            
            if len(all_crawl_metrics[exp_name]) == 0:
                all_crawl_metrics[exp_name] = [{"maxndcg": val} for val in maxndcg_at_limits]
            else:
                for id_limit, metric_at_lim in enumerate(maxndcg_at_limits):
                    all_crawl_metrics[exp_name][id_limit]["maxndcg"] = metric_at_lim

    if compute_ub:
        return all_crawl_metrics, ubs_hr

    return all_crawl_metrics
