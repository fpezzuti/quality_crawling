import sys, os
import random

from typing import Iterator

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from utils.config import config
from utils.component import Component
from utils.datasetIR import load_collection_url2ids_mappings, load_url2docids
from utils.utils import yield_initial_seeds


COLLECTIONS = config.get('collections', None)

RANDOM_SEED = config.get('random_seed')

SAVE_EVERY_N_PAGES = config.get('orchestrator').get("save_every_n_pages")

# set random seed
random.seed(RANDOM_SEED)


class RandomSeedsGenerator(Component):
    """
        Class of the random seeds generator.
        It generates random seeds from a collection of Web documents like ClueWeb22.
    """

    def __init__(self, collection: str, verbose: bool = True) -> None:
        """
            Constructor of the random seeds generator.

            Args:
                collection: string identifier of the collection
                verbose: boolean flag to print log messages
        """
        Component.__init__(self, verbose)
        self.component_name = "SEEDS_GENERATOR"

        self.log(f"Initialising seeds generator for {collection} collection.")       

        self.url2docids_fpath = COLLECTIONS[collection]["seeds_url2docids_fpath"]
        self.__url2docids, _ = load_collection_url2ids_mappings(self.url2docids_fpath)
        
        self.log(f"Initialising url2docids mapping from file={self.url2docids_fpath}.")
       
        self.total_docs = len(self.__url2docids)


    def url2id(self, url: str):
        """
            Return the id of the document with a given url

            Args:
                url: URL of the document
        """
        id = (self.__url2docids).get(url, None)

        if id is None:
            print(f"Warning: url={url} not found in url2docid, returning None.")
        
        return id
        
    def get_seedURLs(self, n: int) -> Iterator[str]:
        """
            yield n seed urls

            Args:
                n: number of seed urls to return
        """
        if n > self.total_docs:
            raise ValueError(f"Number of seed URLs requested exceeds {self.total_docs}.")
        
        sampled_urls = random.sample(list(self.__url2docids.keys()), n)

        for url in sampled_urls:
            yield url


class ListSeedsGenerator(Component):
    """
        Class of the list seeds generator.
        It generates seeds from a list of URLs.
    """

    def __init__(self, collection: str, verbose: bool = True) -> None:
        """
            Constructor of the list seeds generator.
            
            Args:
                collection: string identifier of the collection
                verbose: boolean flag to print log messages
        """
        Component.__init__(self, verbose)
        self.component_name = "SEEDS_GENERATOR"

        self.log(f"Initialising seeds generator for {collection} collection.")       

        self.url2docids_fpath = COLLECTIONS[collection]["seeds_url2docids_fpath"]
        self.log(f"Initialising url2docids mapping from file={self.url2docids_fpath}.")
        url2docid = load_url2docids(self.url2docids_fpath)

        self.__docid2url = {v: k for k, v in url2docid.items()}

        del url2docid
    
        self.total_docs = len(self.__docid2url)

        self.init_seeds_fpath = COLLECTIONS[collection]["init_seeds_fpath"]["best"]
        

    def get_seedURLs(self, n: int) -> list:
        """
            yield n seed urls

            Args:
                n: number of seed urls to return
        """
        if n > self.total_docs:
            raise ValueError(f"Number of seed URLs requested exceeds {self.total_docs}.")
        print(f"Trying to read {n} seeds from file={self.init_seeds_fpath}")

        return yield_initial_seeds(self.init_seeds_fpath, limit=n)



def init_seed_generator(generator_type: str, collection: str, verbose= True):
    """
        Initialises a seed generator of the specified type.

        Args:
            generator_type: string identifier of the generator type
            collection: string identifier of the collection
            verbose: boolean flag to print log messages
    """
    if generator_type == "random":
        return RandomSeedsGenerator(collection=collection, verbose=verbose)
    elif generator_type == "list":
        return ListSeedsGenerator(collection=collection, verbose=verbose)
    else:
        raise ValueError(f"Error: type={generator_type} not supported.")