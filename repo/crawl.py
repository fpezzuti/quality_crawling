
# coding=utf-8
import argparse
import time

from crawler.orchestrator import Orchestrator

from utils.config import config

COLLECTIONS = list(config.get('collections', None).keys())
FRONTIER_TYPES = list(config.get('frontiers', None).keys())
MAX_PAGES_DEFAULT = config.get('orchestrator', None).get('max_pages', 5)
DEFAULT_FRONTIER_TYPE = config.get('orchestrator').get('frontier_type', 'random')
VERBOSITY = config.get('orchestrator').get('verbosity', 2)

EXPERIMENT_NAME = config.get('orchestrator').get('experiment_name')

def main():
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--collection", type=str, default="cw22b", choices=COLLECTIONS, help="Document collection to use.")
    parser.add_argument("--frontier_type", type=str, default=DEFAULT_FRONTIER_TYPE, help="Type of frontier to use.")
    parser.add_argument("--max_pages", type=int, default=MAX_PAGES_DEFAULT, help="Maximum number of pages to be crawled.")
    parser.add_argument("--verbosity", type=int, default=VERBOSITY, help="Level of verbosity.")
    parser.add_argument("--exp_name", type=str, default=EXPERIMENT_NAME, help="Name of the experiment")
    parser.add_argument("--updates_enabled", type=bool, default=False, help="Enable priority updatesin the frontier")
    args = parser.parse_args()
    
    print("************ START JOB ************")
                 
    if args.verbosity > 0:
        start_time = time.time()
    
    orchestrator = Orchestrator(collection=args.collection, frontier_type=args.frontier_type, max_pages=args.max_pages, verbosity=args.verbosity, experiment_name=args.exp_name, updates_enabled=args.updates_enabled)                            

    if args.verbosity > 0:
        print("\nTime elapsed:", time.time() - start_time, "s.\n")
        start_time = time.time()

    orchestrator.crawl()

    print("\nTime elapsed:", time.time() - start_time, "s.\n")
    print("************ JOB COMPLETED ************")
    

if __name__ == "__main__":
    main()
    