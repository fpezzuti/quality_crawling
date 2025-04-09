import sys, os
import datetime
from pandas import DataFrame
import pyterrier as pt
from pyterrier_pisa import PisaIndex, PisaRetrieve
from pyterrier.measures import *
from pyterrier import BatchRetrieve
import pyterrier_pisa

sys.path.append(os.path.dirname(os.path.dirname(__file__)))


from utils.config import config
from utils.component import Component
from indexer.dataset import Downloads

indexer_cfg = config.get("indexer")
orch_cfg = config.get("orchestrator")

VERBOSITY = indexer_cfg.get("verbosity")
DOWNLOADED_PAGES_DIR = config.get('paths').get("downloaded_pages_dir")

INDEX_DIR = config.get("paths").get("index_dir")
RUNS_DIR = config.get("paths").get("runs_dir")
BATCH_SIZE = indexer_cfg.get("batch_size", 100000)
NUM_THREADS = indexer_cfg.get("threads", 16)
TOPK = indexer_cfg.get("topk", 100)


"""
    Get the iterator of first {num_docs} for the list of crawled/downloaded pages given the collection name, and the downloaded pages directory.
"""
def get_downloads_iterator(collection: str, downloaded_pages_dir: str, num_docs_limit: int, preprocess: bool):
    is_lim_reached = False
    downloaded_dataset = Downloads(collection)
    docs_iterator = downloaded_dataset.load_downloads(downloaded_pages_dir, limit=num_docs_limit, preprocess=preprocess)

    num_loaded = downloaded_dataset.get_numloaded()

    if num_loaded < num_docs_limit:
        print(f"Limit of {num_docs_limit} documents reached. Returning {num_loaded} documents.")
        is_lim_reached = True
    return docs_iterator, num_loaded, is_lim_reached, downloaded_dataset.docno2docid
    

class Indexer(Component):
    def __init__(self, collection: str, experiment_name: str, downloads_dir: str = None, verbosity=VERBOSITY, num_docs_limit = None, batch_size = BATCH_SIZE, num_threads=NUM_THREADS) -> None:
        """
            Initialise the indexer component:
            - collection is the string identifier of the corpus of documents to be indexed
            - experiment_name is the name of the experiment (the crawler)
            - downloads_dir is the directory of the downloaded pages
            - verbosity is the integer level
            - num_docs_limit allows to specify the amount of pages to be indexed (assuming increasing chronological order by download time)
            - batch_size is the batch size to be used by the PisaIndex
            - num_threads is the number of threads to be used when indexing.
        """
        Component.__init__(self, verbose=True, verbosity=verbosity)

        self.component_name = "INDEXER"

        self.log(f"Initialising indexer to index at most {num_docs_limit} documents from {collection} collection.", 1)
       
        self.index_dir = f"{INDEX_DIR}/{experiment_name}"
        self.runs_dir = f"{RUNS_DIR}/{experiment_name}"
        self.experiment_name = experiment_name
        self.num_docs_limit = num_docs_limit
        self.flag_limit_reached = None
        self.batch_size = batch_size
        self.num_threads = num_threads
        self.mode = "downloads"

        if not os.path.exists(self.index_dir):
            os.makedirs(self.index_dir)

        self.log(f"The inverted index will be stored in directory={self.index_dir}.", 2)
       
        self._init_docs_iterator(collection, downloads_dir)

        self.log("Initialising PisaIndex.", 2)
        self.inverted_index = PisaIndex(self.index_dir, text_field=['text'], overwrite=False, threads=self.num_threads,  batch_size=self.batch_size)
        self.log("PisaIndex initialised.", 1)

    def _init_docs_iterator(self, collection: str, downloaded_pages_dir: str, preprocess: bool = False):
        """
            initialise the iterator for the documents to be indexed
        """
        if downloaded_pages_dir is not None:
            self.docs_iterator, self.num_loaded, self.flag_limit_reached, self.docno2docid = get_downloads_iterator(collection, downloaded_pages_dir, self.num_docs_limit, preprocess=preprocess)
        else:
            raise ValueError("downloaded_pages_dir must be provided.")
        return 

    def limit_reached(self):
        """
            return True if the maximum number of downloaded documents has been reached
        """
        return self.flag_limit_reached

    def index(self) -> None:
        """
            index with a PisaIndex the documents accessible via the iterator, skipping the indexing if the index already exists.
        """
        self.log("Starting indexing.", 1)
        if not os.listdir(self.index_dir):
            self.inverted_index.index(self.docs_iterator)
            self.log("Done indexing.", 1)
        else:
            self.log("Index already exists, skipping indexing.", 1)

    def remove_index(self) -> None:
        """
            remove the index from the directory
        """
        self.log("Removing index.", 1)
        if os.path.exists(self.index_dir):
            for filename in os.listdir(self.index_dir):
                file_path = os.path.join(self.index_dir, filename)
                os.remove(file_path)
        self.log(f"Index removed from directory={self.index_dir}.", 1)


    def search(self, queries: DataFrame, scorer: str = "bm25", k: int = TOPK) -> None:
        """
            search the top k documents for the given queries using the specified scorer
        """

        if scorer == "bm25":
            self.scorer = self.inverted_index.bm25(num_results=k, threads=self.num_threads)
            self.log("Done creating bm25 scorer.", 2)
        else:
            raise ValueError(f"Invalid scorer={scorer}. Please use 'bm25'.")
        
        self.log(f"Started {scorer} retrieval.", 1)

        results = self.scorer(queries)

        self.log(f"Done searching top {k} documents with {scorer} scorer.", 1)
        return results
    
    def save_results(self, results: DataFrame, benchmark_name: str) -> str:
        """
            transform pyterrier results into trec format and save them to csv
        """
        trec_results = self.pyterrier_to_trec(results, self.experiment_name)
        results['docno'] = results['docno'].apply(lambda x: self.docno2docid(x))
        run_fpath = self.__save_trec_results(trec_results, benchmark_name)
        return run_fpath
    
    def pyterrier_to_trec(self, results: DataFrame, run_name: str):
        """
            transform pyterrier results into trec format
        """
        trecres = results[['qid', 'docno', 'score', 'rank']]
        trecres["run"] = run_name
        trecres["Q0"] = "0"
        trecres = trecres[["qid", "Q0", "docno", "rank", "score", "run"]]
        return trecres
    
    def __save_trec_results(self, results: DataFrame, benchmark_name: str) -> str:
        """
            save results in trec format
        """
        runs_dir = f"{self.runs_dir}/{benchmark_name}"
        if not os.path.exists(runs_dir):
            os.makedirs(runs_dir)
        if self.mode == "downloads":
            fpath = runs_dir + f"/limit_{self.num_docs_limit}.tsv"
        else:
            raise ValueError("Error. Unsupported mode={self.mode}.")
        
        self.log(f"Saving results to file={fpath}.", 2)

        results.to_csv(fpath, index=False, sep='\t', header=False)

        self.log(f"Results saved in file={fpath}.", 1)
        return fpath