import sys, os
import pandas as pd
import pyterrier as pt
from pandas import DataFrame

sys.path.append(os.path.dirname(os.path.dirname(__file__)))


from utils.datasetIR import load_collection_docnos_mappings, load_downloaded_list, load_queries, load_qrels
from utils.config import config
from utils.utils import navigate_to_id
from utils.preprocessor import Preprocessor
from typing import Iterator, Dict

COLLECTIONS = config.get('collections', None)

RANDOM_SEED = config.get('random_seed')

URL2DOCID_PATH = config.get('collections')

BENCHMARKS = config.get('evaluation_benchmarks', None)

QRELS_DIR = config.get('paths').get('qrels_dir')
QUERIES_DIR = config.get('paths').get('queries_dir')


def load_queries_from_dataset(benchmark_name: str):
    """
        Load the dataframe of the queries of the specified query set
    """
    if benchmark_name == "msmarco-ws":
        queries_df = MSMarcoWebSearch(benchmark=benchmark_name).get_queries()
    elif benchmark_name == "rq":
        queries_df = ResearchyQuestions(benchmark=benchmark_name).get_queries()
    else:
        raise ValueError(f"Error, unsupported benchmark: {benchmark_name}")
    return queries_df

class Downloads:
    """
        Class to handle the dataset of downloaded pages of a specific collection like ClueWeb22-B, by a specified crawler
    """

    def __init__(self, collection: str, verbose: bool = False) -> None:
        """
            Initialise the dataset of downloads related to a specific collection
        """
        self.verbose = verbose
        if self.verbose:
            print(f"Initialising dataset for {collection} collection.")

        self.url2docids_fpath = COLLECTIONS[collection]['url2docids_fpath']
        self.texts_dir = COLLECTIONS[collection]['texts_dir']
        self.collection = collection
        self.num_loaded = 0
        self.docno2urls, self.docno2docids = load_collection_docnos_mappings(self.url2docids_fpath)

    def set_numloaded(self, num_loaded) -> None:
        """
            Set the number of downloaded pages
        """
        self.num_loaded = num_loaded

    def get_numloaded(self) -> int:
        """
            Get the number of downaloded pages
        """
        return self.num_loaded
    
    def docno2url(self, docno: int) -> str:
        """
            Get url associated to a docno
        """
        return self.docno2urls.get(docno, None)

    def docno2docid(self, docno: int) -> int:
        """
            Get docid associated to a docno
        """
        return self.docno2docids.get(docno, None)


    def load_downloads(self, data_dir: str, preprocess: bool, limit: int = None) -> Iterator[Dict]:
        """
            Load downloaded docnos stored in data_dir eventually with their texts
        """
        if self.verbose:
            print(f"Loading {limit} data from dir = {data_dir}.")
        downloaded_docnos = load_downloaded_list(data_dir, limit=limit)
        if self.verbose:
            print(f"Downloaded {len(downloaded_docnos)} docnos.")
        self.set_numloaded(len(downloaded_docnos))

        text_field = COLLECTIONS[self.collection]['text_key']
        skipped_docs = 0
        for docno in downloaded_docnos:
            docid = self.docno2docid(docno)
            text = navigate_to_id(self.texts_dir, docid)[text_field]
            if preprocess:
                text = Preprocessor.process_document(text)
                if text is None:
                    skipped_docs +=1
                    continue
            yield {"docno": docid, "text": text} # format required by pyterrier_pisa 
        print(f"Skipped {skipped_docs} documents due to text=None.")

    
    def load_downloads_docnos(self, data_dir: str, limit: int = None) -> list:
        """
            Load downloaded docnos stored in data_dir eventually with their texts
        """
        if self.verbose:
            print(f"Loading {limit} data from dir = {data_dir}.")
        downloaded_docnos = load_downloaded_list(data_dir, limit=limit)
        if self.verbose:
            print(f"Downloaded {len(downloaded_docnos)} docnos.")
        self.set_numloaded(len(downloaded_docnos))
        return downloaded_docnos
    
    def load_downloads_docids(self, data_dir: str, limit: int = None) -> list:
        """
            Load downloaded docnos stored in data_dir eventually with their texts
        """
        if self.verbose:
            print(f"Loading {limit} data from dir = {data_dir}.")
        downloaded_docnos = load_downloaded_list(data_dir, limit=limit)
        if self.verbose:
            print(f"Downloaded {len(downloaded_docnos)} docnos.")
        self.set_numloaded(len(downloaded_docnos))
        return [self.docno2docid(docno) for docno in downloaded_docnos]
    
class ClueWeb22:
    """
        Class to handle dataset ClueWeb22-B
    """
    def __init__(self, collection: str, verbose: bool = False) -> None:
        """
            Initialise the dataset
        """
        self.verbose = verbose
        if self.verbose:
            print(f"Initialising dataset for {collection} collection.")

        self.url2docids_fpath = COLLECTIONS[collection]['url2docids_fpath']
        self.texts_dir = COLLECTIONS[collection]['texts_dir']
        self.collection = collection
        self.num_loaded = 0
        self.docno2urls, self.docno2docids = load_collection_docnos_mappings(self.url2docids_fpath)
        self.text_field = COLLECTIONS[self.collection]['text_key']
    
    def docno2url(self, docno: int) -> str:
        """
            Get url associated to a docno
        """
        return self.docno2urls.get(docno, None)

    def docno2docid(self, docno: int) -> int:
        """
            Get docid associated to a docno
        """
        return self.docno2docids.get(docno, None)
    
    def _get_text_from_docid(self, docid: str) -> str:
        """
            Get the text of a document given its docid
        """
        text = navigate_to_id(self.texts_dir, docid)[self.text_field]
        return text
    
    def _get_text_from_docno(self, docno: int) -> str:
        """
            Get the text of a document given its docno
        """
        docid = self.docno2docid(docno)
        assert docid is not None, f"Error: docid is None for docno={docno}."
        text = self._get_text_from_docid(docid)
        return text
    
    def get_document(self, docno: int | None = None, docid: str | None = None) -> str:
        """
            Get the text of a document given its docno or docid
        """
        if docno is None and docid is None:
            raise ValueError("Error: when trying to get a document, either docid or docno must be provided.")
        elif (docno is not None) and (docid is not None):
            raise ValueError("Error: when trying to get a document, only its docid or its docno should be provided.")
        else:
            doc_text = self._get_text_from_docno(docno) if docno else self._get_text_from_docid(docid)
        return doc_text

    
class RankingListDataframe:
    """
        Class to handle ranking lists stored in TREC format (run files)
    """
    def __init__(self, collection: str, run_fpath: str, topk: int, preprocess=True, verbose: bool = False) -> None:
        self.verbose = verbose
        if self.verbose:
            print(f"Initialising ranking list dataset for {collection} collection, with topk={topk}.")

        self.texts_dir = COLLECTIONS[collection]['texts_dir']
        self.collection = collection
        self.run_fpath = run_fpath
        self.topk = topk
        self.text_field = COLLECTIONS[self.collection]['text_key']
        self.preprocess = preprocess

    def get_rankings(self, benchmark: str, load_texts=True):
        """
            Load docids and texts of documents
        """
        if self.verbose:
            print(f"Loading {self.topk} from ranking lists stored at {self.run_fpath}.")
        results_df = pt.io.read_results(self.run_fpath, format="trec")
       
        results_df = results_df.sort_values(by=['qid', 'score'], ascending=[True, False])
        results_df = results_df.groupby('qid').head(self.topk)

        if load_texts:
            unique_docnos = results_df['docno'].unique().tolist()
            results_df = self._load_doc_texts(results_df, unique_docnos)

            results_df = self._load_query_texts(results_df, benchmark)
        
        results_df = results_df.reset_index(drop=True)

        return results_df

    def _load_doc_texts(self, df: DataFrame, docnos: list):
        """
            Load the texts of the documents given their docnos
        """
        doc_texts = [] 
        
        cleaned_docnos = []
        for docno in docnos:
            text = navigate_to_id(self.texts_dir, docno)[self.text_field]
            if self.preprocess:
                text = Preprocessor.process_document(text)
            if text is None:
                continue
            cleaned_docnos.append(docno)
            doc_texts.append(text)

        doc_texts_df = pd.DataFrame({'docno': cleaned_docnos, 'text': doc_texts})

        results_df = df.merge(doc_texts_df, on='docno', how='right') # TODO: change it to 'left

        assert results_df['text'].isna().sum() == 0, f"Error: number of NaN values in documents' text: {results_df['text'].isna().sum() == 0}"
        return results_df
    
    def _load_query_texts(self, df: DataFrame, benchmark_name: str):
        """
            Load the texts of the queries given their qids
        """
        queries_df = load_queries_from_dataset(benchmark_name)
        results_df = df.merge(queries_df, on='qid', how='inner') # TODO: change it to 'left' 
        assert results_df['query'].isna().sum() == 0, f"Error: number of NaN values in queries' text: {results_df['query'].isna().sum() == 0}"
        assert results_df['docno'].isna().sum() == 0, f"Error: number of NaN values in docnos: {results_df['docno'].isna().sum() == 0}"
        
        return results_df
    

class MSMarcoWebSearch:
    """
        Class to handle the query set MS MARCO Web Search
    """

    def __init__(self, benchmark: str = "msmarco-ws", verbose: bool = False, subset: int | None = None) -> None:
        """
            Initialise the dataset of MS MARCO Web Search
        """
        self.verbose = verbose
        if self.verbose:
            print(f"Initialising dataset for {benchmark} collection.")

        self.qrels_fpath = QRELS_DIR + f"{benchmark}/" + BENCHMARKS[benchmark]['qrels_file']
        self.queries_fpath = QUERIES_DIR + f"{benchmark}/" + BENCHMARKS[benchmark]['queries_file']       

        self.subset = subset

    def get_queries(self, judged: bool = True) -> DataFrame:
        """
            Load the queries of the dataset (or a subsample of them)
        """
        if self.verbose:
            print(f"Loading queries from file={self.queries_fpath}.")
        self.queries = load_queries(self.queries_fpath)
        df = pd.DataFrame(self.queries)
        if judged:
            qrels_rel = self.get_qrels()
            df = df[df.qid.isin(qrels_rel['query_id'])]
        if self.verbose:
            print(f"Loaded {len(self.queries)} queries.")
       
        if self.subset is not None:
            df = df.sample(n=self.subset, random_state=RANDOM_SEED)
            if self.verbose:
                print(f"Sampled {len(df)} queries.")
        return df

    def get_qrels(self) -> DataFrame:
        """
            Load the qrels of the query set
        """
        if self.verbose:
            print(f"Loading qrels from file={self.qrels_fpath}.")
        self.qrles = load_qrels(self.qrels_fpath)
        if self.verbose:
            print(f"Loaded {len(self.qrles)} qrels.")
        df = pd.DataFrame(self.qrles)
        
        return df

    def get_relevant(self) -> DataFrame:
        """
            Get the relevant documents of the query set
        """
        df = self.get_qrels()
        df = df[df["relevance"] == 1] # filter out irrelevant documents
        return df
    
    def get_irrelevant(self) -> DataFrame:
        """
            Get the irrelevant documents of the query set
        """
        df = self.get_qrels()
        df = df[df["relevance"] != 1] # filter out relevant documents
        return df
    

class ResearchyQuestions:
    """
        Class to handle the query set Researchy Questions
    """
    def __init__(self, benchmark: str = "rq", verbose: bool = False, subset: int | None = None) -> None:
        """
            Initialise the dataset of Researchy Questions
        """
        self.verbose = verbose
        if self.verbose:
            print(f"Initialising dataset for {benchmark} collection.")

        self.qrels_fpath = QRELS_DIR + f"{benchmark}/" + BENCHMARKS[benchmark]['qrels_file']
        self.queries_fpath = QUERIES_DIR + f"{benchmark}/" + BENCHMARKS[benchmark]['queries_file']  
        self.subset = subset     

    def get_queries(self, judged=True) -> DataFrame:
        """
            Load the queries of the dataset (or a subsample of them)
        """
        self.queries = load_queries(self.queries_fpath)
        df = pd.DataFrame(self.queries)
        if judged:
            qrels_rel = self.get_qrels()
            df = df[df.qid.isin(qrels_rel['query_id'])]

        if self.subset is not None:
            df = df.sample(n=self.subset, random_state=RANDOM_SEED)
            if self.verbose:
                print(f"Sampled {len(df)} queries.")
        return df

    def get_qrels(self) -> DataFrame:
        """
            Load the qrels of the query set
        """
        if self.verbose:
            print(f"Loading qrels from file={self.qrels_fpath}.")
        self.qrles = load_qrels(self.qrels_fpath)
        if self.verbose:
            print(f"Loaded {len(self.qrles)} qrels.")
        df = pd.DataFrame(self.qrles)
        return df
    
    def get_relevant(self) -> DataFrame:
        """
            Get the relevant documents of the query set
        """
        df = self.get_qrels()
        df = df[df["relevance"] == 1] # filter out irrelevant documents
        return df
    
    def get_irrelevant(self) -> DataFrame:
        """
            Get the irrelevant documents of the query set
        """
        df = self.get_qrels()
        df = df[df["relevance"] != 1] # filter out relevant documents
        return df