import pickle, zlib
import os
import numpy as np
import sys
from collections import defaultdict
from tqdm import tqdm

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.config import config
DOWNLOADS_FNAME = config.get('paths').get('downloaded_pages_fprefix')
QSCORERS_CHECKPOINTS = config.get('qscorer').get('checkpoints')


def load_url2docids(url2docids_fpath: str) -> dict:
    """
        Load url2docids mapping from file, used to map a URL to a docid for a given Web document.
        Returns a dict that can be used to map URLs to docids.

        Args:
            url2docids_fpath: path to the file containing the dictionary mapping URLs to docids
    """
    try:
        # load the compressed data 
        with open(url2docids_fpath, 'rb') as f:
            compressed_url2docid = f.read()
    except Exception as e:
        raise RuntimeError(f"Error reading file={url2docids_fpath}: {e}")
    
    try:
        decompressed_url2docid = pickle.loads(zlib.decompress(compressed_url2docid))
    except Exception as e:
        raise RuntimeError(f"Error decompressing file={url2docids_fpath}: {e}")
    
    return decompressed_url2docid


def parse_url2docnos(url2docids: dict) -> dict:
    """
        Parse url2docnos mapping out of url2docids mapping.
        Given the dict that maps URLs to docids, returns a dict that can be used to map URLs to docnos.

        Args:
            url2docids: dict that maps URLs to docids
    """
    url2docnos = {url: index for index, url in enumerate(url2docids)}

    if len(url2docids) != len(url2docnos):
        raise ValueError("Error: url2docids and url2docnos have different lengths.")
    return url2docnos


def parse_docno2urls(url2docids: dict) -> tuple:
    """
        Parse docno2urls mapping out of url2docids mapping.
        Given the dict that maps URLs to docids, returns a dict that can be used to map docnos to URLs.

        Args:
            url2docids: dict that maps URLs to docids
    """
    docno2urls = {}
    docno2docids = {}

    for docno, (url, docid) in enumerate(url2docids.items()):
        docno2urls[docno] = url
        docno2docids[docno] = docid
    
    return docno2urls, docno2docids


def load_collection_url2ids_mappings(url2docids_fpath: str) -> tuple:
    """
        Load from file all the mappings that map URL to docids, and URL to docnos.

        Args:
            url2docids_fpath: path to the file containing the dictionary mapping URLs to docids
    """
    url2docids = load_url2docids(url2docids_fpath)
    url2docnos = parse_url2docnos(url2docids)
    return url2docids, url2docnos


def load_collection_docnos_mappings(url2docids_fpath: str) -> tuple:
    """
        Load from file all the mappings that map docno to URLs, and docno to docids.

        Args:
            url2docids_fpath: path to the file containing the dictionary mapping URLs to docids
    """
    url2docids = load_url2docids(url2docids_fpath)
    docno2urls, docno2docids = parse_docno2urls(url2docids)
    return docno2urls, docno2docids


def load_downloaded_list(downloaded_dir: str, limit: int | None = None) -> list:
    """
        Load the list docids of of downloaded documents stored in downloaded_dir by the fetcher

        Args:
            downloaded_dir: path to the directory containing the downloaded docids
            limit: maximum number of downloaded docids to load
    """
    downloaded_docnos = []

    chunk_idx = 1
    with tqdm(desc="Loading chunks of downloaded docids") as pbar:
        while True:
            downloaded_fpath = os.path.join(downloaded_dir, f"{DOWNLOADS_FNAME}_{chunk_idx}.npy")
        
            if not os.path.exists(downloaded_fpath):
                print(f"File={downloaded_fpath} does not exist.")
                return downloaded_docnos
            
            try:
                downloaded_docnos += np.load(downloaded_fpath, allow_pickle=True, mmap_mode='r').tolist()
            except:
                raise RuntimeError(f"Error reading file={downloaded_fpath}.")
            
            chunk_idx += 1
            pbar.update(1)
            if limit and len(downloaded_docnos) >= limit:
                downloaded_docnos = downloaded_docnos[:limit]
                break
        
    print(f"Loaded {len(downloaded_docnos)} docnos in dir={downloaded_dir}.")

    if len(downloaded_docnos) != limit:
        print("Warning: downloaded docnos and limit have different lengths.")
        print("Downloaded docnos:", len(downloaded_docnos))
        print("Limit:", limit)

    return downloaded_docnos


def load_qrels(qrels_fpath: str, with_click: bool = False) -> list:
    """
        Load the qrels from the file at qrels_fpath.
        If with_click is True, the relevance is the click value, otherwise it is 1.
        Returns a list of dicts, in which each dict has the keys: query_id, doc_id, relevance.
    
        Args:
            qrels_fpath: path to the file containing the qrels
            with_click: boolean flag to indicate whether the relevance is the click value
    """
    qrels = []
    with open(qrels_fpath, 'r') as file:
        for line in file:
            columns = line.strip().split('\t')
            qid, docid = columns[0], columns[1]
            relevance = columns[2] if with_click else 1
            qrels.append({"query_id": qid, "doc_id": docid, "relevance": relevance})
    return qrels


def load_queries(queries_fpath: str) -> list:
    """
        Load the queries from the file at queries_fpath.
        Returns a list of dicts, in which each dict has the keys: qid, query.

        Args:
            queries_fpath: path to the file containing the queries
    """
    queries = []
    with open(queries_fpath, 'r') as file:
        for line in file:
            qid, query = line.strip().split('\t')
            queries.append({"qid": qid, "query": query})
    return queries


def load_ranking_list(run_fpath: str, topk: int) -> dict:
    """
        Load the topk ranking list from the file at run_fpath.
        Returns a dict that maps query ids to lists of docids.

        Args:
            run_fpath: path to the file containing the ranking list
            topk: top-k value to consider
    """
    rankings = defaultdict(list)

    with open(run_fpath, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:  # skip empty lines
                continue
            cols = line.split()
    
            qid, _, docid, rank = cols[0], cols[1], cols[2], int(cols[3])
            
            if rank <= topk:
                rankings[qid].append(docid)

    print(f"Loaded {len(rankings)} rankings.")
    return rankings