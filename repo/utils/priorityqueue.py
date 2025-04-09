#from queue import PriorityQueue
import heapq
from queue import PriorityQueue
from abc import ABC, abstractmethod

class PQueue(ABC):
    """
        Abstract class for a max-heap priority queue.
    """
    def __init__(self):
        """
            Constructor of the priority queue.
        """
        self.num_enqueued = 0

    def _internal_priority(self, priority: float) -> float: # for max-heap
        """
            Convert external priority to internal priority.
        """
        return -priority
    
    def _external_priority(self, priority: float) -> float:
        """
            Convert internal priority to external priority.
        """
        return -priority
    
    @abstractmethod
    def put(self, item: object, priority: float) -> None:
        """
            Put an item in the priority queue with a specific priority

            Args:
                item: the object to be enqueued
                priority: the priority of the item
        """
        pass

    @abstractmethod
    def update(self, item: object, priority: float) -> None:
       """
            Update the priority of an item in the priority queue
            
            Args:
                item: the object to be updated
                priority: the new priority of the item
        """
       pass

    @abstractmethod
    def get(self) -> object:
        """
            Get the item with the highest priority from the priority queue
        """
        pass
    
    @abstractmethod
    def enqueued(self) -> int:
        """
            Get the number of items in the priority queue
        """
        pass

class PQueueHeap(PQueue):
    """
        Class for a max-heap priority queue using heapq.
    """

    def __init__(self):
        """
            Constructor of the priority queue.
        """
        super().__init__()
        self.queue = [] # heapq queue
    
    def put(self, item: object, priority: float) -> None:
      """
            Put an item in the priority queue with a specific priority

            Args:
                item: the object to be enqueued
                priority: the priority of the item
        """
      heapq.heappush(self.queue, (self._internal_priority(priority), item))
    
    def update(self, item: object, priority: float) -> None:
        """
            Insert item in set of removed items and re-heapify the queue.

            Args:
                item: the object to be updated
                priority: the new priority of the item
        """
        old_priority = None
        for i in range(len(self.queue)): # process all items in the queue
            if self.queue[i][1] == item: # search the item in the queue
                old_priority = self.queue[i][0] # get current priority
                if self._external_priority(old_priority) < priority: # update priority if necesasry
                    self.queue[i] = (self._internal_priority(priority), item)
                    heapq.heapify(self.queue) # re-heapify the queue
                break

    def get(self) -> tuple:
        """
            Pop the item with the highest priority and return both the item and its priority.
        """
        try:
            item =  heapq.heappop(self.queue)  
            return item[1], item[0] 
        except IndexError:
            raise KeyError('Trying to pop from an empty priority queue')
    
    def enqueued(self) -> int:
        """
            Get the number of items in the priority queue
        """
        return len(self.queue)
    
    def get_all_items(self) -> list:
        """
            Get the list of all items in the priority queue
        """
        num_enqueued = self.enqueued()
        items = [item for priority,  item in self.queue]
        assert num_enqueued == len(items), f"Error, num_enqueued={num_enqueued} != len(items)={len(items)}"
        return items

class PQueuePriorityQueue(PQueue):
    """
        Class for a max-heap priority queue using PriorityQueue.
    """
    MAX_DELETED_THRESHOLD = 10_000_000

    def __init__(self):
        """"
            Constructor of the priority queue.
        """
        self.queue = PriorityQueue()
        self.deleted = {} # lazy deletion set
        self.num_enqueued = 0
        self.num_deleted = 0
        if self.enqueued() != (self.queue.qsize() - self.num_deleted):
            raise ValueError(f"Error: enqueued()={self.enqueued()} != queue.qsize() - num_deleted = {self.queue.qsize()} - {self.num_deleted}")
    
    def put(self, item: object, priority: float) -> None:
        """
            Put an item in the priority queue with a specific priority
        """
        if item in self.deleted:
            assert False, "Error, trying to put an item that is already in the deleted"
        internal_priority = self._internal_priority(priority)
        self.queue.put((internal_priority, item))
    
        self.deleted[item] = {"priority": internal_priority, "count": 0}
        self.num_enqueued += 1

    def remove(self, item: object) -> None:
        """
            Remove an item from the priority queue

            Args:
                item: the object to be removed
        """
        self._mark_deleted(item, float('-inf'))
        self.num_enqueued -= 1

    def update(self, item: object, priority: float) -> bool:
        """
            Update the priority of an item in the priority queue.
            Returns True if the item was updated, False otherwise.

            Args:
                item: the object to be updated
                priority: the new priority of the item
        """
        if item not in self.deleted:  
            return False

        # find the current priority of the item
        old_priority = self.deleted[item]["priority"]

        # check if new priority is higher than the current one
        if self._external_priority(old_priority) > priority:
            self._mark_deleted(item, priority) # update priority
            self.queue.put((self._internal_priority(priority), item)) # add new item to the queue
            
        # if the number of deleted items exceeds the threshold, clean up the deleted list
        if self.num_deleted > self.MAX_DELETED_THRESHOLD:
            self._cleanup_deleted()

        return True
    
    def get(self) -> tuple:
        """
            Pop the item with the highest priority and return both the item and its priority.
        """
        while not self.queue.empty(): # process the queue until it is empty
            priority, item = self.queue.get() # get next item from the queue
            priority = self._external_priority(priority) # convert internal priority to external priority

            # get the minimum priority of the item and the number of times it was deleted
            min_del_priority, del_count = self.deleted.get(item).values()             
            min_del_priority = self._external_priority(min_del_priority) # convert internal priority to external priority

            del_count -= 1
            if del_count < 0:
                del self.deleted[item]
            else:
                self.deleted[item] = {"priority": self._internal_priority(min_del_priority), "count": del_count}

            if priority > min_del_priority: # item is marked as deleted
                self.num_deleted -= 1
                continue
            else: # item is not marked as deleted
                self.num_enqueued -= 1
                return item, priority

        raise KeyError('Trying to pop from an empty priority queue')
    
    def enqueued(self) -> int:
        """
            Return the number of items in the queue.
        """
        return self.num_enqueued

    def _mark_deleted(self, item: object, priority: float) -> None:
        """
            Mark an item as deleted without actually removing it.
            
            Args:
                item: the object to be marked as deleted
                priority: the priority of the item
        """
        old_priority, old_count = self.deleted.get(item).values()
   
        if priority >= self._external_priority(old_priority):
            return
        
        self.deleted[item] = {"priority": self._internal_priority(priority), "count": old_count +1}
        self.num_deleted += 1 
        
    def _cleanup_deleted(self) -> None:
        """
            Remove all items from the deleted set.
        """
        old_num_enqueued = self.enqueued()
        new_queue = PriorityQueue()
        new_deleted = {}
        
        while not self.queue.empty():
            item, priority = self.get()
            new_queue.put((self._internal_priority(priority), item))
        
            new_deleted[item] = {"priority": self._internal_priority(priority), "count": 0}

        self.queue = new_queue
        self.num_deleted = 0
        self.num_enqueued = old_num_enqueued
        self.deleted = new_deleted
        self.num_deleted = 0

    def get_all_items(self) -> list:
        """
            Get the list of all items in the priority queue
        """
        num_enqueued = self.enqueued()
        items = []
        while self.enqueued() > 0:
            item, _ = self.get()
            items.append(item)

        assert num_enqueued == len(items), f"Error, num_enqueued={num_enqueued} != len(items)={len(items)}"

        return items