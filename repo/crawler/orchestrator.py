import sys, os
from tqdm import tqdm

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from crawler.fetcher import Fetcher
from crawler.parser import Parser
from crawler.frontier import init_frontier_manager
from crawler.seen import init_url_seen_tester
from crawler.seedsgenerator import init_seed_generator
from crawler.webpage import WebPage
from utils.config import config
from utils.component import Component

orch_cfg = config.get("orchestrator")

SEEN_URLS_TYPE = orch_cfg.get("seen_urls_type")
SEEDS_STRATEGY = orch_cfg.get("seeds_strategy")
FRONTIER_TYPE = orch_cfg.get("frontier_type")
NUM_SEED_URLS = orch_cfg.get("num_seed_urls")
MAX_PAGES = orch_cfg.get("max_pages")
VERBOSITY = orch_cfg.get("verbosity")
DOWNLOADED_PAGES_FPATH = config.get('paths').get("downloaded_pages_dir")
DOWNLOADED_FILE = config.get('paths').get("downloaded_pages_fprefix")

SAVE_EVERY_N_PAGES = orch_cfg.get("save_every_n_pages")

UPDATES_ENABLED = orch_cfg.get("updates_enabed")

EXPERIMENT_NAME = orch_cfg.get("experiment_name", "exp_0")

PAGERANK_PERIOD = config.get("pagerank").get("period")

def save_seeds(seeds, fpath):
    print(f"Writing {len(seeds)} seeds to {fpath}")
    with open(fpath, "w") as f:
        for seed in seeds:
            f.write(seed + "\n")

class Orchestrator(Component):
    """
        Class of the orchestrator component.
        It coordinates the fetcher, parser, frontier manager and seen url tester.
    """
   
    def __init__(self,
                 collection: str,
                 seen_urls_type: str = SEEN_URLS_TYPE,
                 num_seed_urls: int = NUM_SEED_URLS,
                 max_pages: int = MAX_PAGES,
                 seeds_strategy: str = SEEDS_STRATEGY,
                 frontier_type: str = FRONTIER_TYPE,
                 verbosity=VERBOSITY,
                 save_every_n_pages=SAVE_EVERY_N_PAGES,
                 updates_enabled=UPDATES_ENABLED,
                 experiment_name=EXPERIMENT_NAME) -> None:
        """
            Constructor of the orchestrator
            
            Args:
                collection: string identifier of the collection
                seen_urls_type: string identifier of the type of seen urls tester
                num_seed_urls: number of seed urls to start the crawling
                max_pages: maximum number of pages to crawl
                seeds_strategy: string identifier of the seeds generator strategy
                frontier_type: string identifier of the frontier manager type
                verbosity: integer value to set the verbosity level
                save_every_n_pages: number of pages to save the downloaded pages
                updates_enabled: boolean flag to enable updates in the frontier
                experiment_name: string identifier of the experiment    
        """
        
        super().__init__(verbose=True, verbosity=verbosity)
        self.component_name = "ORCHESTRATOR"

        self.log(f"Initialising orchestrator for crawling {collection} collection.", 1)
        self.max_pages = max_pages
        self.save_every_n_pages = save_every_n_pages
        self.updates_enabled = updates_enabled 
        self.notfound_seedurls = 0
        self.num_noutlinks = 0
        self.pagerank_period = None
        self.oracle = False
        
        to_parse = []

        subcomponents_verbose = True if verbosity >= 2 else False

        downloaded_pages_fpath = DOWNLOADED_PAGES_FPATH + frontier_type + "_"+f"{experiment_name}/" + DOWNLOADED_FILE

        SeedsGenerator = init_seed_generator(generator_type=seeds_strategy, collection=collection, verbose=subcomponents_verbose)
        self.log(f"Generating seed URLs.", 1)
        self.seed_urls = SeedsGenerator.get_seedURLs(n=num_seed_urls)

        del SeedsGenerator

        self.Fetcher = Fetcher(collection=collection, downloaded_pages_fpath=downloaded_pages_fpath, verbose=False)

        self.log(f"Initialising frontier manager for type={frontier_type}", 1)

        if frontier_type in ["quality", "oracle-quality"]:
            to_parse = to_parse + ["qscores"]
            if frontier_type == "oracle-quality":
                self.oracle = True
        elif frontier_type in ["bfs", "dfs"]:
            pass
        else:
            self.log(f"Error: Unknown frontier type={frontier_type}.", 1)
            return


        self.log(f"Initialising parser for parsing {to_parse}", 1)
            
        self.Parser = Parser(collection=collection, verbose=subcomponents_verbose, to_parse=to_parse)

        seen_urls_capacity = (self.Fetcher.get_total_docs() + 10) if seen_urls_type != "set" else None

        self.SeenURLTester = init_url_seen_tester(tester_type=seen_urls_type, capacity=seen_urls_capacity)
        self.FrontierManager = init_frontier_manager(frontier_type=frontier_type, updates=self.updates_enabled, verbose=True)
       
        self.log(f"Orchestrator initialised.", 1)

        if self.oracle:
            self.log(f"Oracle mode enabled.", 1)
        sys.stdout.flush()
        
    def process_page(self, page: WebPage) -> None:
        """
            Process a webpage after it has been downloaded. It stores the page, extracts metadata, process outlinks.

            Args:
                page: WebPage object to be processed
        """
        self.Fetcher.store(page.get_url())

        page = self.Parser.parse_metadata(page) # extract metadata

        outlinks = page.get_metadata("outlinks") # get outlinks

        if outlinks is None:
            self.log(f"Warning: page={page.get_url()} has no outlinks.", 2)
            self.num_noutlinks += 1
            outlinks = []

        for linked_url in outlinks: # process each outlink
            linked_docid = self.Fetcher.url2docno(linked_url)
            
            if linked_docid is None:
                self.wrong_linked_docid += 1
                continue

            # create a new page object for the linked page
            linked_page = WebPage(url=linked_url, docno=linked_docid, id=self.Fetcher.url2id(linked_url))

            if self.oracle:
                # get metadata of the linked page
                to_parse = self.Parser.get_to_parse()
                metadata = {}
                if "qscores" in to_parse:
                    qscore = self.Parser.parse_qscore(linked_page)
                    metadata["qscore"] = qscore
                linked_page.set_metadata(metadata)
               
            if self.SeenURLTester.is_seen(linked_docid): # check if the linked page has been seen
                if not self.updates_enabled:
                    continue
                # update the linked page in the frontier
                res = self.FrontierManager.update(page=linked_page, father=page)
                continue
            else:
                # mark the linked page as seen and add it to the frontier
                self.SeenURLTester.mark_seen(linked_docid)
                self.FrontierManager.add(page=linked_page, father=page)


    def populate_frontier(self) -> None:
        """
            Populate the frontier with the initialised seed urls
        """
        
        to_write = []

        self.log(f"Inserting seeds into the frontier.", 1)

        for seed_url in self.seed_urls: # process each seed url
            docid = self.Fetcher.url2docno(seed_url) # fetch the docid of the seed url
            if docid is None:
                self.notfound_seedurls += 1
                continue # skip
            
            if not self.oracle: # add the seed url to the frontier with maximum priority
                self.FrontierManager.add_with_max_priority(seed_url, docid)
            else:
                # parse metadata and insert the seed url to the frontier with its oracle priority
                page = WebPage(url=seed_url, docno=docid, id=self.Fetcher.url2id(seed_url))

                to_parse = self.Parser.get_to_parse()
                metadata = {}
                if "qscores" in to_parse:
                    qscore = self.Parser.parse_qscore(page)
                    if qscore is not None:
                        metadata["qscore"] = qscore
                    else:
                        self.log(f"Warning: qscore not found for seed url with docid={docid}.", 1)

                page.set_metadata(metadata)
                self.FrontierManager.add(page, None)

            to_write.append(seed_url) # add see url to the list of urls to write to file.
            self.SeenURLTester.mark_seen(docid) # mark the seed url as seen

        save_seeds(to_write, "seeds.txt") # save seed urls to file
        self.seed_urls = None


    def crawl(self) -> None:
        """
            Start the crawling process.
        """
        self.failed_dowloads = 0
        self.wrong_linked_docid = 0

        self.populate_frontier() # populate the frontier with the seed urls

        self.log(f"Starting to crawl.", 1)
        self.log(f"Frontier contains: {self.FrontierManager.enqueued()} pages.", 1)


        # initialise progress bar
        total_pages = self.max_pages if self.max_pages > 0 else self.Fetcher.get_total_docs()
        progress_bar = tqdm(total=total_pages, desc="Crawling", unit="page")
        sys.stdout.flush()

        while self.FrontierManager.enqueued() > 0: # process the frontier until it becomes empty
            processed_pages = self.Fetcher.num_dowloaded()

            if (self.max_pages>0) and (processed_pages >= self.max_pages): # check if the maximum number of pages has been reached
                self.log(f"Max pages limit of {self.max_pages} crawled pages reached.", 1)
                break

            url = self.FrontierManager.pop() # get next url to be processed from the frontier
            page = self.Fetcher.download(url) # download the page

            if page is None: # check if the download has failed
                self.failed_dowloads += 1
                continue
            
            self.process_page(page) # process the page by extracting metadata and outlinks

            progress_bar.update(1)

            if (processed_pages % self.save_every_n_pages) == 0 and processed_pages > 1:
                self.log(f"Up to now:\n\t{processed_pages} downloaded pages.", 1)
                self.log(f"\t{self.failed_dowloads} failed downloads.", 1)
                self.log(f"\t{self.wrong_linked_docid} failed wrong linked docid.", 1)
                self.log(f"\t{self.notfound_seedurls} not found seed urls", 1)
                self.log(f"\t{self.num_noutlinks} pages with no outlinks.", 1)
                self.log(f"Storing {processed_pages} downloaded pages to file.", 1)
                self.Fetcher.checkpoint()
            sys.stdout.flush()

        # save the downloaded pages to file

        progress_bar.close()
        if self.FrontierManager.enqueued() == 0:
            self.log(f"Frontier is empty.", 1)

        if self.FrontierManager.enqueued() < 0:
            self.log(f"Error: Frontier has negative number of pages.", 1)

        self.log(f"Crawling completed.", 1)
        self.log(f"Failed downloads={self.failed_dowloads}.", 1)
        self.log(f"Failed wrong linked docid ={self.wrong_linked_docid}.", 1)
        self.log(f"Total of {self.notfound_seedurls} not found seed urls", 1)
        self.log(f"Total of {self.num_noutlinks} pages with no outlinks.", 1)
        num_dowloaded = self.Fetcher.num_dowloaded()
        self.log(f"Saving {num_dowloaded} downloaded pages to file.", 1)

        self.Fetcher.write_downloads_to_file(last=True)
        self.log("Done.", 1)