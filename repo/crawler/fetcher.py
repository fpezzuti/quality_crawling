
import numpy as np
import sys, os
import random


sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from utils.config import config
from utils.component import Component
from utils.datasetIR import load_collection_url2ids_mappings
from crawler.webpage import WebPage
from indexer.dataset import Downloads

COLLECTIONS = config.get('collections', None)

RANDOM_SEED = config.get('random_seed')

SAVE_EVERY_N_PAGES = config.get('orchestrator').get("save_every_n_pages")

# set random seed
random.seed(RANDOM_SEED)



class Fetcher(Component):
    """
        Class of the fetcher component.
        It simulates the download process of web pages.
    """
   
    def __init__(self, collection: str, downloaded_pages_fpath: str, verbose: bool = True) -> None:
        """
            Constructor of the fetcher

            Args:
                collection: string identifier of the collection
                downloaded_pages_fpath: path to the file where the downloaded pages will be stored
                verbose: boolean flag to print log messages
            """
        super().__init__(verbose)
        self.component_name = "FETCHER"

        self.log(f"Initialising fetcher for {collection} collection.")

        self.storage_fpath = downloaded_pages_fpath
        url2docids_fpath = COLLECTIONS[collection]["url2docids_fpath"]

        self.downloads_dataset = Downloads(collection=collection)

        if os.path.exists(self.storage_fpath):
            self.log("Output file already exists. Exiting")
            raise FileExistsError(f"Error: file={self.storage_fpath} already exists.")

        self.downloaded = []
        self.num_stored = 0

        self.log(f"Initialising url2docids mapping from file={url2docids_fpath}.")
        self.__url2docids, self.__url2docnos = load_collection_url2ids_mappings(url2docids_fpath)
    
        self.total_docs = len(self.__url2docids)

        self.log(f"Docnos of downloaded pages will be stored at={self.storage_fpath}.")

    def download(self, url: str) -> WebPage:
        """
            Download a document from a given url and returns its id

            Args: 
                url: string url of the document to be downloaded

            Returns:
                WebPage: object representing the downloaded document
        """
        self.log(f"Downloading document from url={url}.")

        id = self.url2id(url)
        if id is None:
           return None
        
        docno = self.url2docno(url)
        
        self.log(f"Document with id={id} downloaded.")
        return WebPage(id=id, url=url, docno=docno)

    def url2id(self, url: str):
        """
            Return the id of the document with a given url

            Args:
                url: URL of the document
        """
        id = (self.__url2docids).get(url, None)

        if id is None:
            self.log(f"Warning: url={url} not found in url2docid, returning None.")
        
        return id 
    
    def url2docno(self, url: str):
        """
            Return the docno of the document with a given url

            Args:
                url: URL of the document
        """
        try:
            return self.__url2docnos.get(url, None)
        except ValueError:
            self.log(f"Error: url={url} not found in url2docnos.")
            return None
 
    
    def store(self, url: str) -> None:
        """
            Store a document with a given docno

            Args:
                url: URL of the document to be stored
        """
        self.downloaded.append(self.url2docno(url))


    def write_downloads_to_file(self, last: bool = False) -> None:
        """
            Write dowloaded docs to file

            Args:
                last: boolean flag to indicate if this is the last batch of downloaded pages
        """
        print(f"Saving {len(self.downloaded)} downloaded docnos to file={self.storage_fpath}.")
        checkpoint_id = int((len(self.downloaded) + self.num_stored) / SAVE_EVERY_N_PAGES) + (1 if last else 0)
        fpath = f"{self.storage_fpath}_{checkpoint_id}"
        
        if not os.path.exists(os.path.dirname(fpath)):
            os.makedirs(os.path.dirname(fpath))
        np.save(fpath, self.downloaded)
        print(f"Saved {len(self.downloaded)} docnos to file={fpath}.")

    def close(self) -> None:
        """
            Close the fetcher
        """
        self.log("Applying exit functions.")

        if not os.path.exists(os.path.dirname(self.storage_fpath)):
            os.makedirs(os.path.dirname(self.storage_fpath))

        self.log(f"Saving {len(self.downloaded)} downloaded docnos to file={self.storage_fpath}.")
        np.save(self.storage_fpath, np.array(self.downloaded))
        
        self.log("Exited.")

    def get_seedURLs(self, n: int, strategy: str = "random") -> list:
        """
            Return n seed urls

            Args:
                n: number of seed urls to return
                strategy: strategy to select the seed urls

            Returns:
                list: seed WebPage objects related to the list of seed urls
        """
        urls = None
        if n > len(self.__url2docids):
            raise ValueError(f"Number of seed URLs requested exceeds {len(self.__url2docids)}.")

        if strategy == "random":
            urls = random.sample(list(self.__url2docids.keys()), n)
        else:
            raise ValueError(f"Error: strategy={strategy} not implemented.")

        seeds = [WebPage(id=self.url2id(url), url=url) for url in urls]
        return seeds

    def num_dowloaded(self) -> int:
        """
            Return the number of downloaded pages
        """
        return len(self.downloaded) + self.num_stored
    
    def get_total_docs(self) -> int:
        """
            Return the total number of documents to be crawled
        """
        return self.total_docs
    
    def checkpoint(self) -> None:
        """
            Save downloaded pages to file
        """
        self.log("Checkpointing downloaded pages.")
        self.write_downloads_to_file()
        self.num_stored += len(self.downloaded)
        self.downloaded = []
       
    def all_downloaded_docnos(self) -> list:
        """
            Return the list of docnos of the downloaded pages
        """
        downl_docnos = self.downloads_dataset.load_downloads_docnos(data_dir=os.path.dirname(self.storage_fpath), limit=None)
        return downl_docnos