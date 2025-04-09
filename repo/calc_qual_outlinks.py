from crawler.parser import Parser
from crawler.webpage import WebPage
from utils.datasetIR import load_collection_url2ids_mappings

from typing import Iterator
from tqdm import tqdm

from utils.config import config

def chunk_url2docids(url2docids: dict, batch_size: int) -> Iterator[list]:
    """
        Chunks the url2docids dictionary into batches of size batch_size and returns an iterator over them.

        Args:
            url2docids: dict of url to docid mappings.
            batch_size: size of the batches.
    """
    batch = []
    for url, docid in url2docids.items():
        batch.append((url, docid))
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:  
        yield batch
    
def compute_mean_link_quality(outlinks: list, parser: Parser, url2docids: dict) -> float:
    """
        Computes the mean quality of the outlinks of a page.
        
        Args:
            outlinks: list of outlinked URLS of a page.
            parser: parser object used to compute link quality.
            url2docids: dict of url to docid mappings.
    """
    mean_qual = 0
    num_pages = 0
    none_links = 0

    for link_url in outlinks: # iterate over outlinked URLs 
        linked_docid = url2docids.get(link_url, None) # get docid
        if linked_docid is None:
            none_links += 1
            continue

        linked_page = WebPage(url=link_url, id=linked_docid)

        qual = parser.parse_qscore(linked_page) # parse score
        if qual is not None:
            mean_qual += qual
            num_pages += 1

    mean_qual = mean_qual / num_pages if num_pages > 0 else None # compute mean quality
    return mean_qual, none_links

def get_outlinks_qual(url2docids: dict, parser: Parser, batch_size: int = 5_000_000) -> tuple:
    """
        Computes the mean quality of the outlinks of the pages in the collection.
        Returns a tuple of lists containing the page quality and the mean quality of the outlinked pages in the Web collection.

        Args:
            url2docids: dict of url to docid mappings.
            parser: parser object used to compute page quality.
            batch_size: size of the batches.
    """
    page_qual, links_qual = [], []

    none_links = 0
    with tqdm(total=len(url2docids), desc="Processing URLs", unit="url", dynamic_ncols=True) as pbar: # process pages in the collection
        for idx, batch in enumerate(chunk_url2docids(url2docids, batch_size)):
            for url, docid in batch:
                page = WebPage(url=url, id=docid)
                outlinks = parser.parse_outlinks(page) # parse outlinks
                qual = parser.parse_qscore(page) # parse page quality

                if outlinks is not None:
                    mean_qual, new_none = compute_mean_link_quality(outlinks=outlinks, parser=parser, url2docids=url2docids)
                    if mean_qual is not None:
                        page_qual.append(qual)
                        links_qual.append(mean_qual)        

                pbar.update(1)
                none_links += new_none

            print("None links up to now:", none_links)
         
    print(f"Processed {len(url2docids)} URLs.")
    print(f"Mean page quality: {sum(page_qual) / len(page_qual)}")
    print(f"Mean link quality: {sum(links_qual) / len(links_qual)}")
    return page_qual, links_qual

def main():
    collection = "cw22b"
    COLLECTIONS = config.get('collections', None)
    URL_DOCIDS_PATH = COLLECTIONS[collection]["url2docids_fpath"]
    PLOTS_DIR = config.get("paths").get("plots_dir")
    OUTPUT_FPATH = PLOTS_DIR + "neighbours_quality.tsv"

    # load collection url to docid mappings
    url2docids, _ = load_collection_url2ids_mappings(URL_DOCIDS_PATH)

    print(f"Loaded {len(url2docids)} url2docids mappings.")

    parser = Parser("cw22b", to_parse=["qscores"], verbose=False)
    
    # get lists of mean quality of pages in the collection and mean quality of outlinked pages
    page_qual, links_qual = get_outlinks_qual(url2docids, parser, batch_size=1_000_000)

    # write to file
    with open(OUTPUT_FPATH, "w") as f:
        for p, l in zip(page_qual, links_qual):
            f.write(f"{p}\t{l}\n")

if __name__ == "__main__":
    main()