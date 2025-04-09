import sys, os
from abc import ABC, abstractmethod

from bitarray import bitarray

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

class SeenURLTester(ABC):
    """
        Abstract class for seen url testers.
    """
    component_name = "SEEN_URLS_TESTER"
        
    @abstractmethod
    def is_seen(self, url: int) -> bool:
       """
            Check if a url is already seen

            Args:
                url: URL to test
        """
       pass
        
    @abstractmethod
    def mark_seen(self, url: int) -> None:
        """
            Mark a url as seen
            
            Args:
                url: URL to mark as seen
        """
        pass

    @abstractmethod
    def seen_count(self) -> int:
        """
            Return the number of seen urls
        """
        pass

    def __repr__(self):
        """
            Return the name of the class
        """
        return f"{self.name}"
    
    def __str__(self):  
        """
            Return the string representation of the class
        """
        return self.__repr__()


class SetSeenURLTester(SeenURLTester):
    """
        Class to test if a url is already seen using a set
    """
    name = "SetSeenURLTester"

    def __init__(self, capacity=None) -> None:
        """
            Constructor of the class
            Args:
                capacity: capacity of the set
        """
        super().__init__()
        self.seen_urls = set()


    def is_seen(self, docid: int) -> bool:
        """
            Check if a docid is already seen

            Args:
                docid: docid to test
        """
        return docid in self.seen_urls
        
    def mark_seen(self, docid: int) -> None:
        """
            Mark a docid as seen

            Args:
                docid: docid to mark as seen

        """
        self.seen_urls.add(docid)

    def seen_count(self) -> int:    
        """
            Return the number of seen urls
        """
        return len(self.seen_urls)
    

class BitArraySeenURLTester(SeenURLTester):
    """
        Class to test if a url is already seen using a bitarray
    """
    name = "BitArraySeenURLTester"

    default_capacity = 1_000_000

    def __init__(self, capacity: int = default_capacity) -> None:
        """
            Constructor of the class
            
            Args:
                capacity: capacity of the bitarray
        """
        super().__init__()
        self.capacity = capacity
        self.seen_urls = bitarray(self.capacity)
        self.seen_urls.setall(0)


    def is_seen(self, docid: int) -> bool:
        """
            Check if a url is already seen

            Args:
                docid: docid to test
        """
        if docid >= self.capacity or docid < 0:
            raise IndexError(f"Error: docid={docid} out of range (capacity={self.capacity}).")
        return self.seen_urls[docid]
        
    def mark_seen(self, docid: int) -> None:
        """
            Mark a docid as seen

            Args:
                docid: docid to mark as seen
        """
        if docid >= self.capacity or docid < 0:
            raise IndexError(f"Error: docid={docid} out of range (capacity={self.capacity}).")
        self.seen_urls[docid] = 1

    def seen_count(self) -> int:    
        """
            Return the number of seen urls
        """
        return self.seen_urls.count(1)
    

def init_url_seen_tester(tester_type: str, capacity: int = None):
    """
        Initialises a seen url tester of the specified type.

        Args:
            tester_type: string identifier of the tester type
            capacity: capacity of the tester
    """
    if tester_type == "set":
        return SetSeenURLTester(capacity=capacity)
    elif tester_type == "bitarray":
        return BitArraySeenURLTester(capacity=capacity)
    else:
        raise ValueError(f"Error: type={tester_type} not supported.")