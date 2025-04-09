import sys, os
import zlib, pickle
from datetime import datetime


sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.config import config
from utils.utils import read_json_gz
from utils.datasetIR import load_collection_url2ids_mappings

STARTING_INLINK_DIR = "./../cw22/inlink/en/en00/"
STARTING_OUTLINK_DIR = "./../cw22/outlink/en/en00/"
STARTING_TXT_DIR = "./../cw22/txt/en/en00/"
LOG_FILE_PATH = "./log_url2docids.txt"


def log(message: str, log_fpath: str = LOG_FILE_PATH) -> None:
    """
        Log a message to a file.

        Args:
            message: message to be logged.
            log_fpath: path to the log file.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_fpath, 'a') as file:
        file.write(f"[{timestamp}] {message}\n")


def clear_log(log_fpath: str = LOG_FILE_PATH) -> None:
    """
        Clear the log file.

        Args:
            log_fpath: path to the log file.
    """
    with open(log_fpath, 'w') as file:
        file.write("")


def process_links_dir(subdir_path: str, from_txt: bool) -> list:
    """
        Process a directory containing .json.gz files and return a list of parsed objects.

        Args:
            subdir_path: str, path to the directory containing the .json.gz files.
            from_txt: bool, whether the links are from the txt files or not.
    """
    list_dir = []
    for filename in os.listdir(subdir_path):
        if filename.endswith('.json.gz'):
            file_path = os.path.join(subdir_path, filename)
            list_dir.append(process_links_file(file_path, from_txt))
    log(f"Processed dir: {subdir_path}.")
    return list_dir


def process_links_file(file_path: str, from_txt: bool) -> dict:
    """
        Process a single .json.gz file and return its parsed links

        Args:
            file_path: str, path to the .json.gz file.
            from_txt: bool, whether the links are from the txt files or not.
    """
    local_url2docid = {}
    url_key = "URL" if from_txt else "url"
    for doc in read_json_gz(file_path):     
        local_url2docid[doc[url_key]] = doc["ClueWeb22-ID"]
    log(f"Processed file: {file_path}.")
    return local_url2docid
    

def build_url2docid_mapping(start_dir: str, from_txt: bool) -> dict:
    """
        Read the URLs from the .json.gz files in the given directory and build a url2docid mapping.

        Args:
            start_dir: str, path to the directory containing the .json.gz files.
            from_txt: bool, whether the links are from the txt files or not.
    """
    url2docids_dict = {}
    completed_tasks = 0
    
    for dirname in os.listdir(start_dir):
        subdir_path = os.path.join(start_dir, dirname)
        if not os.path.isdir(subdir_path):
            continue

        log(f"Processing {subdir_path}.")
        
        local_dicts = process_links_dir(subdir_path, from_txt)
        counter = 0
        for local_url2docid in local_dicts:
            url2docids_dict.update(local_url2docid)
            counter += 1
            log(f"Merged {counter} dicts")

        completed_tasks += 1
        log(f"Completed {completed_tasks} tasks.")
    return url2docids_dict

def clean_url(url: str) -> str:
    """
        Clean the URL and return a new URL.
        
        Args:
            url: the URL to be cleaned.
    """
    new_url = url.replace("\n", "")
    return new_url

def clean_url2docids(url2docids: dict) -> dict:
    """
        Clean the URLs in the url2docids dictionary and return a new url2docids mapping

        Args:
            url2docids: dict, a dictionary containing the URL to docid mapping.
        """
    new_url2docids = {}
    for url, docid in url2docids.items():
        new_url = clean_url(url)
        new_url2docids[new_url] = docid
    return new_url2docids


def save_url2docids(url2docids: dict, fpath: str) -> None:
    # compress data and write to file
    compressed_data = zlib.compress(pickle.dumps(url2docids))
    log("Done compressing url2docids.")

    with open(fpath, 'wb') as f:  # save the compressed data to a file
        f.write(compressed_data)

    log(f"Done saving compressed data to file={fpath}.")


def main():
    collection = "cw22b"
    COLLECTIONS = config.get('collections', None)
    CLEANED_URL2DOCID_FPATH = COLLECTIONS[collection]["url2docids_fpath"]

    STARTING_DIR = STARTING_TXT_DIR
    RAW_URL2DOCID_FPATH = CLEANED_URL2DOCID_FPATH.replace(".dat", "_raw.dat")
    
    os.makedirs(os.path.dirname(RAW_URL2DOCID_FPATH), exist_ok=True)

    clear_log()

    log(f"Starting extraction of URL to docid mapping from directory ={STARTING_DIR}.")

    urls = build_url2docid_mapping(STARTING_DIR, from_txt=True)

    log(f"Loaded {len(urls)} urls.")

    save_url2docids(urls, RAW_URL2DOCID_FPATH)

    log(f"Done saving compressed data to {RAW_URL2DOCID_FPATH}.")

    log("Finished extraction of URL to docid mapping.")

    log("Starting cleaning URLS extreacted")

    url2docids, _ = load_collection_url2ids_mappings(RAW_URL2DOCID_FPATH)
        
    total_docs = len(url2docids)
    log(f"Total number of documents: {total_docs}")

    # clean urls and update the url2docids dict
    url2docids = clean_url2docids(url2docids)

    save_url2docids(url2docids, fname=CLEANED_URL2DOCID_FPATH)
    
    if os.path.exists(RAW_URL2DOCID_FPATH):
        os.remove(RAW_URL2DOCID_FPATH)

if __name__ == "__main__":
    main()