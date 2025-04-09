import sys, os
import random
from abc import ABC, abstractmethod

from collections import deque

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from crawler.webpage import WebPage
from utils.config import config
from utils.component import Component
from utils.priorityqueue import PQueuePriorityQueue, PQueueHeap

RANDOM_SEED = config.get('random_seed', None)
random.seed(RANDOM_SEED)


class FrontierManager(ABC, Component):
    """
        Abstract class of the frontier manager component.
    """
    name = "FrontierManager"

    def __init__(self, verbose: bool = True) -> None:
        """
            Constructor of the frontier manager

            Args:
                verbose: boolean flag to print log messages
        """
        super().__init__(verbose)
        self.component_name = "FRONTIER_MANAGER"
        
    @abstractmethod
    def pop(self):
        """
            get the maximum priority page from the frontier
        """
        pass

    @abstractmethod
    def add(self, page: WebPage, father: WebPage = None):
        """
            add a page to the frontier exploiting the father's metadata if available
        """
        pass

    @abstractmethod
    def add_with_max_priority(self, url: str, docid: int):
        """
            add a page to the frontier with max priority
        """
        pass

    @abstractmethod
    def update(self, page: WebPage, key, value):
        """
            update an element of the frontier
        """
        pass

    @abstractmethod
    def enqueued(self) -> int:
        """
            return the number of enqueued pages
        """
        pass
        
    def log(self, msg: str) -> None:
        """
            print a log message
        """
        if self.verbose:
            print(f"[{self.component_name}]: {msg}")

    def __repr__(self):
        """
            return the name of the frontier manager
        """
        return f"{self.name}"
    
    def __str__(self):
        """
            return the string representation of the frontier manager
        """  
        return self.__repr__()


class RandomFrontierManager(FrontierManager):
    """
        Random frontier manager.

        This frontier manager selects pages randomly from the frontier and never updates priorities.
    """
    name = "RandomFrontierManager"

    def __init__(self, verbose: bool = True) -> None:
        """
            Constructor of the random frontier manager

            Args: 
                verbose: boolean flag to print log messages
        """
        FrontierManager.__init__(self, verbose)
        self.log(f"Initialising RandomFrontierManager.")
        self.queue = []

    def pop(self) -> WebPage:
        """
            Pop a random page from the frontier
        """
        if len(self.queue) == 0:
            raise IndexError("Error: trying to pop from empty queue")
        random_index = random.randint(0, len(self.queue) - 1)
        # swap the random element with the last element
        self.queue[random_index], self.queue[-1] = self.queue[-1], self.queue[random_index]
        return self.queue.pop() # pop last

    def add(self, page: WebPage, father: WebPage = None) -> None:
        """
            Add a page to the frontier

            Args:
                page: WebPage object to be added to the frontier
                father: WebPage object representing the father of the page
        """
        url = page.get_url()
        self.queue.append(url)

    def add_with_max_priority(self, url: str, docid: int) -> None:
        """
            Add a page to the frontier with max priority

            Args:
                url: string url of the page
                docid: integer identifier of the page
        """
        self.queue.append(url)

    def update(self, page: WebPage, father: WebPage = None) -> None:
        """
            Update an element of the frontier

            Args:
                page: WebPage object to be updated
                father: WebPage object representing the father of the page
        """
        pass

    def enqueued(self) -> int:
        """
            Return the number of enqueued pages
        """
        return len(self.queue)
    
class QualityFrontierManager(FrontierManager):
    """
        Quality frontier manager.
        This frontier manager assigns priorities to pages based on their quality scores.
    """
    name = "QualityFrontierManager"
    MAX_PRIORITY = 1
    MIN_PRIORITY = -50
    no_priority_pages = 0
    priority_pages = 0
    no_father_pages = 0

    def __init__(self, updates = False, verbose: bool = True, oracle: bool = False) -> None:
        """
            Constructor of the quality frontier manager
            
            Args:
                updates: boolean flag to enable updates of the priorities
                verbose: boolean flag to print log messages
                oracle: boolean flag to use an oracle to assign priorities
        """
        FrontierManager.__init__(self, verbose)
        self.log(f"Initialising QualityFrontierManager.")
        
        self.queue = PQueuePriorityQueue() if updates else PQueueHeap()

        self.log(f"Using a queue of type={type(self.queue)}.")

        self.oracle = oracle
        if self.oracle:
            self.log("QualityFrontierManager will use an oracle to assign priorities.")
        else:
            raise NotImplementedError("Error, QualityFrontierManager not oracle-based not implemented yet.")
        
    def pop(self) -> str:
        """
            Get the maximum priority page from the frontier
        """
        if self.queue.enqueued() == 0:
            raise IndexError("Error: trying to pop from empty queue")
        url, priority = self.queue.get()
        return url # pop max priority page

    def add(self, page: WebPage, father: WebPage) -> None:
        """
            Add a page to the frontier

            Args:
                page: WebPage object to be added to the frontier
                father: WebPage object representing the father of the page
        """
        url = page.get_url()

        if father is None: # check if page is a seed
            self.add_with_max_priority(url, docid=-1)
            self.no_father_pages += 1
            self.priority_pages +=1
            return

        if self.oracle:
            priority = page.get_metadata(key="qscore")
        else:
            raise ValueError("Error, non-oracle frontier prioritisation strategies not implemented yet.")

        if priority is None:
            priority = self.MIN_PRIORITY
            self.log(f"Warning: page={url} has no priority, setting it to {priority}.")
            self.no_priority_pages +=1
        else:
            self.priority_pages +=1 
        self.queue.put(item=url, priority=priority)

    def add_with_max_priority(self, url: str, docid: int) -> None:
        """
            Add a page to the frontier with max priority

            Args:
                url: string url of the page
                docid: internal unique identifier of the page
        """
        self.queue.put(item=url, priority=self.MAX_PRIORITY)
    

    def update(self, page: WebPage, father: WebPage) -> bool:
        """
            Update an element of the frontier given its new father

            Args:
                page: WebPage object to be updated
                father: WebPage object representing the father of the page
        """
        url = page.get_url()

        if self.oracle:
            priority = page.get_metadata(key="qscore")
        else:
            priority = father.get_metadata(key="qscore")
        
        if priority is None:
            priority = self.MIN_PRIORITY
            self.log(f"Warning: page={url} has no priority, setting it to {priority}.")
    
        return self.queue.update(item=url, priority=priority)

    def enqueued(self) -> int:
        """
            Return the number of enqueued pages
        """
        return self.queue.enqueued()
    
    def remove(self, url: str) -> None:
        """
            Remove a page from the frontier

            Args:
                url: URL of the page to be removed
        """
        num_enqueued = self.enqueued()
        
        if num_enqueued == 0:
            raise IndexError("Error: trying to pop from an empty frontier.")
       
        self.queue.remove(url)

        if self.enqueued() == num_enqueued:
            raise ValueError(f"Error: the removal was not effective (num_enqueued still equal to {num_enqueued}).")
        
    def get_stats(self) -> tuple:
        """
            Return the number of pages with no priority and the number of pages with priority
        """
        print(f"Number of pages with no father for the QualityFrontierManager: {self.no_father_pages}")
        return self.no_priority_pages, self.priority_pages

    def get_all_urls(self) -> list:
        """
            Return all the urls in the frontier
        """
        return self.queue.get_all_items()
        
    
class BreadthFirstSearchFrontierManager(FrontierManager):
    """
        BFS frontier manager.
        This frontier manager selects pages based on the order of their insertion and never updates priorities.
    """
    name = "BreadthFirstSearchFrontierManager"

    def __init__(self, verbose: bool = True) -> None:
        """
            Constructor of the BFS frontier manager
            
            Args:
                verbose: boolean flag to print log messages
        """
        FrontierManager.__init__(self, verbose)
        self.log(f"Initialising BreadthFirstSearchFrontierManager.")
        self.queue = deque() # double-ended queue

    def pop(self) -> str:
        """
            Pop a random url from the frontier
        """
        if len(self.queue) == 0:
            raise IndexError("Error: trying to pop from empty queue")

        return self.queue.popleft() # pop first url from the frontier

    def add(self, page: WebPage, father: WebPage = None) -> None:
        """
            Add a page to the frontier

            Args:
                page: WebPage object to be added to the frontier
                father: WebPage object representing the father of the pages (ignored)
        """
        url = page.get_url()
        self.queue.append(url)

    def add_with_max_priority(self, url: str, docid: int) -> None:
        """
            Add a page to the frontier with max priority

            Args:
                url: string url of the page
                docid: internal unique identifier of the page (ignored)
        """
        self.queue.append(url)

    def update(self, page: WebPage, father: WebPage = None) -> None:
        """
            update an element of the frontier (ignored)
        """
        pass

    def enqueued(self) -> int:
        """
            Return the number of enqueued pages
        """
        return len(self.queue)
    
class DepthFirstSearchFrontierManager(FrontierManager):
    """
        DFS frontier manager.
        This frontier manager selects pages based on the order of their insertion and never updates priorities.
    """
    name = "LIFOFrontierManager"

    def __init__(self, verbose: bool = True) -> None:
        """
            Constructor of the DFS frontier manager
            
            Args:
                verbose: boolean flag to print log messages
        """
        FrontierManager.__init__(self, verbose)
        self.log(f"Initialising DepthFirstSearchFrontierManager.")
        self.queue = deque() # double-ended queue

    def pop(self) -> str:
        """
            Pop the next page from the frontier
        """
        if len(self.queue) == 0:
            raise IndexError("Error: trying to pop from empty queue")

        return self.queue.pop() # pop last url from the frontier

    def add(self, page: WebPage, father: WebPage = None) -> None:
        """
            Add a page to the frontier

            Args:
                page: WebPage object to be added to the frontier
                father: WebPage object representing the father of the pages (ignored)
        """
        url = page.get_url()
        self.queue.append(url)

    def add_with_max_priority(self, url: str, docid: int) -> None:
        """
            Add a page to the frontier with max priority

            Args:
                url: string url of the page
                docid: internal unique identifier of the page (ignored)
        """
        self.queue.append(url)

    def update(self, page: WebPage, father: WebPage = None) -> bool:
        """
            Update an element of the frontier

            Args:
                page: WebPage object to be updated
                father: WebPage object representing the father of the page (ignored)
        """
        pass

    def enqueued(self) -> int:
        """
            Return the number of enqueued pages
        """
        return len(self.queue)

def init_frontier_manager(frontier_type: str, updates: bool, verbose= True):
    """
        Initialise a frontier manager of a given type
        
        Args:
            frontier_type: string identifier of the frontier manager
            updates: boolean flag to enable updates of the priorities
            verbose: boolean flag to print log messages
    """
    if frontier_type == "random":
        return RandomFrontierManager(verbose=verbose)
    elif frontier_type == "oracle-quality":
        return QualityFrontierManager(updates=updates, verbose=verbose, oracle=True)
    elif frontier_type == "bfs":
        return BreadthFirstSearchFrontierManager(verbose=verbose)
    elif frontier_type == "dfs":
        return DepthFirstSearchFrontierManager(verbose=verbose)
    else:
        raise ValueError(f"Error: type={frontier_type} not supported.")