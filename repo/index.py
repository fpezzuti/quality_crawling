
# coding=utf-8
import argparse
import time

from utils.config import config

from indexer.indexer import Indexer
from indexer.dataset import MSMarcoWebSearch, ResearchyQuestions


VERBOSE = config.get('indexer').get('verbose', True)
VERBOSITY = config.get('indexer').get('verbosity', 2)
COLLECTIONS = list(config.get('collections', None).keys())
TOPK = config.get('indexer').get('topk', 100)
DEFAULT_EXPERIMENT_NAME = config.get('indexer').get('experiment_name', 'experiment_0')

SUPPORTED_BENCHMARKS = list(config.get('evaluation_benchmarks', None).keys())

DEFAULT_PERIOD = config.get('indexer').get('period')

DOWNLOAD_PAGES_DIR = config.get('paths').get('downloaded_pages_dir')

EVAL_RES_DIR = "./results/"

BATCH_SIZE = config.get("indexer").get("batch_size")

def main():
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--collection", type=str, default="cw22b", choices=COLLECTIONS, help="Document collection to use.")
    parser.add_argument("--benchmark", type=str, default="all", choices=SUPPORTED_BENCHMARKS+["all"], help="Document collection to use.")
    parser.add_argument("--batch_size", type=int, default=BATCH_SIZE, help="Batch size used.")
    parser.add_argument("--topk", type=int, default=TOPK, help="Maximum number of results returned.")
    parser.add_argument("--verbose", type=bool, default=VERBOSE, help="Verbose mode.")
    parser.add_argument("--exp_name", type=str, required=True, help="Experiment name.")
    parser.add_argument("--limit", type=int, default=None, help="Limit the number of documents to index.")
    parser.add_argument("--evaluate", type=bool, default=False, help="Compute evaluation metrics for the generated run.")
    parser.add_argument("--periodic", type=bool, default=True, help="Every limit documents, build and index and compute a run.")
    parser.add_argument("--remove_all", type=bool, default=False, help="Remove all indexes after inferencec.")
    args = parser.parse_args()
    downloads_dir = DOWNLOAD_PAGES_DIR + args.exp_name
    print("************ START JOB ************")
                 
    if args.verbose:
        start_time = time.time()
    
    indexer = None

    if args.periodic:
        period = args.limit if args.limit is not None else DEFAULT_PERIOD

        print(f"Periodic mode activated, with period of {period}.")

        limit = period

        while True:
            print("Starting experiment with limit:", limit)
            sub_exp_name = f"{args.experiment_name}/limit_{limit}"

            indexer = Indexer(collection=args.collection, experiment_name=sub_exp_name, downloaded_pages_dir=downloads_dir, verbosity=VERBOSITY, num_docs_limit=limit, batch_size=args.batch_size)
            indexer.index()

            if args.evaluate:
                tested_benchamrks = SUPPORTED_BENCHMARKS if args.benchmark == "all" else [args.benchmark]

                for benchmark_name in tested_benchamrks:
                    if benchmark_name == "msmarco-ws":
                        queries_df = MSMarcoWebSearch(benchmark=benchmark_name).get_queries()
                    elif benchmark_name == "rq":
                        queries_df = ResearchyQuestions(benchmark=benchmark_name).get_queries()
                    else:
                        raise ValueError(f"Unknown benchmark: {benchmark_name}")
                    
                    results = indexer.search(queries_df, scorer="bm25", k=args.topk)
                    run_fpath = indexer.save_results(results, benchmark_name)
                    print("Saved run in file:", run_fpath)

            indexer.remove_index(subset=(not args.remove_all))

            limit += period

            if indexer.limit_reached() is True:
                print("Limit reached.")
                break

    if args.verbose:
        print("\nTime elapsed:", (time.time() - start_time)//60, "s.\n")
    print("************ JOB COMPLETED ************")
    

if __name__ == "__main__":
    main()
