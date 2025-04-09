import sys, os
import argparse
import pandas as pd

from datasets import load_dataset


sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.datasetIR import load_url2docids

from typing import Iterator
from utils.preprocecssor import Preprocessor
from utils.config import config



def yield_raw_queries(file_path: str) -> Iterator:
    """
        Load the raw queries from a file.

        Args:
            file_path: str, the path to the file containing the queries.
    """
    with open(file_path, 'r') as f:
        for line in f:
            qid, qtext, languages = line.strip().split('\t')
            yield qid, qtext

def yield_raw_qrels(file_path: str) -> Iterator:
    """
        Load the raw qrels from a file.

        Args:
            file_path: str, the path to the file containing the qrels.
    """
    with open(file_path, 'r') as f:
        for line in f:
            qid, docid = line.strip().split('\t')
            yield qid, docid

def write_msmarco_queries_to_file(queries: dict, file_path: str) -> None:
    """
        Write the preprocessed queries to a file.

        Args:
            queries: dict, a dictionary containing the preprocessed queries.
            file_path: str, the path to the file where the preprocessed queries will be written.
    """
    with open(file_path, 'w') as f:
        for qid, qtext in queries.items():
            f.write(f"{qid}\t{qtext}\n")

    print(f"Written {len(queries)} preprocessed queries to:", file_path)

def write_msmarco_qrels_to_file(qrels: dict, file_path: str) -> None:
    """
        Write the preprocessed qrels to a file.

        Args:
            qrels: dict, a dictionary containing the preprocessed qrels.
            file_path: str, the path to the file where the preprocessed qrels will be written.
    """
    with open(file_path, 'w') as f:
        for qid, docid in qrels.items():
            f.write(f"{qid}\t{docid}\n")

    print(f"Written {len(qrels)} preprocessed qrels to:", file_path)


def save_rq_queries_to_file(df: pd.DataFrame, output_fpath: str) -> pd.DataFrame:
    """
        Save queries to file.

        Args:
            df: DataFrame with queries
            output_fpath: path to the output file
    """
    queries_df = df.groupby('id').agg(question=('question', 'first')).reset_index()
    print(f"Number of queries: {len(queries_df)}")

    queries_df.to_csv(output_fpath, sep='\t', index=False, header=False)
    print("Saved queries to file=", output_fpath)
    return queries_df


def save_rq_qrels_to_file(qrels_df: pd.DataFrame, output_fpath: str, url2docids_fpath: str) -> pd.DataFrame:
    """
        Save qrels to file.

        Args:
            df: DataFrame with qrels
            output_fpath: path to the output file
            url2docids_fpath: path to the file with url2docids mapping
    """
    
    print("Number of qrels before filtering: ", len(qrels_df))
    url2docids = load_url2docids(url2docids_fpath)
    qrels_df['docid'] = qrels_df['url'].apply(lambda x: url2docids.get(x, None))
    qrels_df = qrels_df.dropna(subset=['docid'])
    qrels_df.drop(columns=['question', 'url'], inplace=True)
    qrels_df = qrels_df.rename(columns={'id': 'qid', 'docid': 'docno', 'click': 'rel'})
    qrels_df = qrels_df.reindex(columns=['qid', 'docno', 'rel'])
    print("Number of qrels after filtering: ", len(df))
    qrels_df = qrels_df.loc[qrels_df.groupby('qid')['rel'].idxmax()]
    qrels_df['rel'] = 1
    print("Number of qrels after filtering for max: ", len(qrels_df))
   
    queries_df = qrels_df.groupby('qid').agg(question=('qid', 'first')).reset_index()
    print("Number of queries after filtering:", len(queries_df))   
    del queries_df 

    qrels_df.to_csv(output_fpath, sep='\t', index=False, header=False)
    print("Saved qrels to file=", output_fpath)
    
    return qrels_df



def clean_rq_data(dataset_df: pd.DataFrame) -> pd.DataFrame:
    """
        Clean the Researchy Questions query set by removing unused fields and filtering english documents.
        
        Args:
            dataset_df: query set's DataFrame
    """
    df_exploded = dataset_df.explode('DocStream', ignore_index=True)

    df_exploded['url'] = df_exploded['DocStream'].apply(lambda x: x.get('Url'))
    df_exploded['click'] = df_exploded['DocStream'].apply(lambda x: x.get('Click_Cnt'))
    df_exploded['language'] = df_exploded['DocStream'].apply(lambda x: x.get('UrlLanguage'))
    df_exploded = df_exploded[df_exploded['language'] == 'en']

    df_exploded.drop(columns=['DocStream', 'intrinsic_scores', 'gpt4_decomposition', 'decompositional_score', 'nonfactoid_score', 'language'], inplace=True)

    print(df_exploded[:1])

    return df_exploded

def load_rq_data(split: str = "test") -> pd.DataFrame:
    """
        Load a split of the Researchy Questions dataset.

        Args: split: the split to load (train, validation, test)
    """
    dataset = load_dataset('corbyrosset/researchy_questions', split=split)
    dataset_df = pd.DataFrame(dataset)
    return dataset_df


def main():
    supported_querysets = ["msm-ws", "rq"]
    parser = argparse.ArgumentParser()
    parser.add_argument("--queryset", type=str, default="all", choices=supported_querysets+["all"], help="Query set to clean.")
    args = parser.parse_args()

    collection = "cw22b"
    COLLECTIONS = config.get('collections', None)
    URL2DOCIDS_FPATH = COLLECTIONS[collection]["url2docids_fpath"]

    QRELS_DIR = config.get("paths").get("qrels_dir")
    QUERIES_DIR = config.get("paths").get("queries_dir")

    BENCHMARKS = config.get('evaluation_benchmarks', None)

    querysets_to_clean = supported_querysets if args.queryset == "all" else [args.queryset]
   
    if "rq" in querysets_to_clean:
        benchmark_str = "rq"
        # load, clean, and save RQ queries and qrels
        CLEANED_QUERIES_FPATH = f"{QUERIES_DIR}{benchmark_str}/{BENCHMARKS[benchmark_str]['queries_file']}"
        CLEANED_QRELS_FPATH = f"{QRELS_DIR}{benchmark_str}/{BENCHMARKS[benchmark_str]['qrels_file']}"

        dataset_df = load_rq_data(split="test")
        dataset_df = clean_rq_data(dataset_df)


        queries_df = save_rq_queries_to_file(dataset_df, CLEANED_QUERIES_FPATH)
        print(queries_df.head())
    
        qrels_df = save_rq_qrels_to_file(dataset_df, CLEANED_QRELS_FPATH, URL2DOCIDS_FPATH)
        print(qrels_df.head())

    if "msm-ws" in querysets_to_clean:
        # load, clean, and save MSM-WS queries and qrels
        QUERIES_FPATH = f"{QUERIES_DIR}/{benchmark_str}/msmarco-ws-queries.tsv"
        QRELS_FPATH = f"{QUERIES_DIR}/{benchmark_str}/msmarco-ws-qrels.tsv"

        CLEANED_QUERIES_FPATH = f"{QUERIES_DIR}{benchmark_str}/{BENCHMARKS[benchmark_str]['queries_file']}"
        CLEANED_QRELS_FPATH = f"{QRELS_DIR}{benchmark_str}/{BENCHMARKS[benchmark_str]['qrels_file']}"

        preprocessed_queries = {}
        discarded = 0
        for qid, qtext in yield_raw_queries(QUERIES_FPATH):
            cleaned_text = Preprocessor.process_document(qtext)
            if cleaned_text is not None:
                preprocessed_queries[qid] = cleaned_text
            else:
                discarded += 1

        cleaned_qrels = {}
        for qid, docid in yield_raw_qrels(QRELS_FPATH):
            if qid in preprocessed_queries:
                cleaned_qrels[qid] = docid

        preprocessed_qids = set(preprocessed_queries.keys())

        for qid in preprocessed_qids:
            if qid not in cleaned_qrels:
                preprocessed_queries.pop(qid)
                discarded += 1

        print(f"Discarded {discarded} queries.")
        print(f"Total number of queries after preprocessing: {len(preprocessed_queries)}")
        write_msmarco_queries_to_file(preprocessed_queries, CLEANED_QUERIES_FPATH)
        write_msmarco_qrels_to_file(cleaned_qrels, CLEANED_QRELS_FPATH)

    print("Done preprocessing.")

if __name__ == "__main__":
    main()