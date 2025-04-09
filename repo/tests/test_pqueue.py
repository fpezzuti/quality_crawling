import pytest

import sys, os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.priorityqueue import PQueue

@pytest.fixture
def pq():
    """Fixture to create a new PQueue instance for each test."""
    return PQueue()

def test_put_get(pq):
    """Test get."""
    pq.put("https://example.com/page1", 5)
    pq.put("https://example.com/page2", 1)
    pq.put("https://example.com/page3", 10)

    item, priority = pq.get()
    assert item == "https://example.com/page3"
    item, priority = pq.get()
    assert item == "https://example.com/page1"
    item, priority = pq.get()
    assert item == "https://example.com/page2"

    with pytest.raises(KeyError):
        pq.get()

def test_update_priority(pq):
    """Test update."""
    pq.put("https://example.com/page1", 5)
    pq.put("https://example.com/page2", 1)
    pq.put("https://example.com/page3", 10)

    pq.update("https://example.com/page1", 15)

    item, _ = pq.get()
    assert item == "https://example.com/page1"
    item, _ = pq.get()
    assert item == "https://example.com/page3"
    item, _ = pq.get()
    assert item == "https://example.com/page2"

"""
def test_update_non_existing_item(pq):
    pq.put("https://example.com/page1", 5)
    pq.put("https://example.com/page2", 1)

    pq.update("https://example.com/page3", 10)  # "page3" doesn't exist

    item = pq.get()
    assert item[1] == "https://example.com/page1"
    item = pq.get()
    assert item[1] == "https://example.com/page2"
"""
def test_empty_queue(pq):
    """Test behavior of queue when empty."""
    with pytest.raises(KeyError):
        pq.get()  # should raise error because the queue is empty

def test_multiple_updates(pq):
    """Test multiple updates on the same URL."""
    pq.put("https://example.com/page1", 5)
    pq.put("https://example.com/page2", 1)
    pq.put("https://example.com/page3", 10)

    pq.update("https://example.com/page1", 20)
    pq.update("https://example.com/page1", 30)

    item, _ = pq.get()
    assert item == "https://example.com/page1"
    item, _ = pq.get()
    assert item == "https://example.com/page3"
    item, _ = pq.get()
    assert item == "https://example.com/page2"

def test_enqueued_count(pq):
    """Test the enqueued count with URLs."""
    assert pq.enqueued() == 0

    pq.put("https://example.com/page1", 5)
    pq.put("https://example.com/page2", 1)
    assert pq.enqueued() == 2

    pq.get()
    assert pq.enqueued() == 1

    pq.get()
    assert pq.enqueued() == 0

def test_cleanup_deleted(pq):
    pq.put("item1", 5)
    pq.put("item2", 3)
    pq.put("item3", 8)
    pq.put("item4", 2)
    pq.put("item5", 4)
    assert pq.enqueued() == 5
    
    pq.update("item1", 3) # skipped
    pq.update("item2", 2) # skipped
    assert pq.num_deleted == 0
    
    
    pq._cleanup_deleted()
    
    assert pq.num_deleted == 0   # num of deleted items should be 0
    assert pq.enqueued() == 5    # num of enqueued items should be the same

    item, _ = pq.get()
    assert item == "item3"  # the first item should be the first

    assert pq.enqueued() == 4  # number of enqueued items

def test_cleanup_deleted2(pq):
    pq.put("item1", 5)
    pq.put("item2", 3)
    pq.put("item3", 8)
    pq.put("item4", 2)
    pq.put("item5", 4)
    assert pq.enqueued() == 5
    
    pq.update("item1", 7) # inserted
    pq.update("item2", 2) # skipped
    assert pq.num_deleted == 1
    
    
    pq._cleanup_deleted()
    
    assert pq.num_deleted == 0   # num of deleted items should be 0
    assert pq.enqueued() == 5    # num of enqueued items should be the same

    item, _ = pq.get()
    assert item == "item3"  # the first item should be the first

    assert pq.enqueued() == 4  # number of enqueued items

def test_many_cleanup_deleted(pq):
    pq.put("item1", 5)
    pq.put("item2", 3)
    pq.put("item3", 8)
    pq.put("item4", 2)
    pq.put("item5", 4)
    assert pq.enqueued() == 5
    
    assert pq.enqueued() == (pq.queue.qsize() - pq.num_deleted)
    pq.update("item1", 3)
    pq.update("item2", 3)
    assert pq.num_deleted == 0
    
    
    pq._cleanup_deleted()
    
    assert pq.num_deleted == 0   # num of deleted items should be 0
    assert pq.enqueued() == 5    # num of enqueued items should be the same

    item, _ = pq.get()
    assert item == "item3"  # the first item should be the first

    assert pq.enqueued() == 4  # number of enqueued items
    assert pq.enqueued() == (pq.queue.qsize() - pq.num_deleted)  # number of enqueued items should be the same as the queue size

def test_many_cleanup_deleted2(pq):
    pq.put("item1", 5)
    pq.put("item2", 3)
    pq.put("item3", 8)
    pq.put("item4", 2)
    pq.put("item5", 4)
    assert pq.enqueued() == 5
    
    assert pq.enqueued() == (pq.queue.qsize() - pq.num_deleted)
    pq.update("item1", 3) # skipped
    pq.update("item1", 12)
    pq.update("item1", 11) # skipped
    pq.update("item2", 3) # skipped
    assert pq.num_deleted == 1

    item, priority = pq.get()
    assert item == "item1"  # the first item should be the first
    assert priority == 12
    
    pq._cleanup_deleted()
    
    assert pq.num_deleted == 0   # num of deleted items should be 0
    assert pq.enqueued() == 4    # num of enqueued items should be the same

    item, _ = pq.get()
    assert item == "item3"  # the first item should be the first

    assert pq.enqueued() == 3  # number of enqueued items
    assert pq.enqueued() == (pq.queue.qsize() - pq.num_deleted)  # number of enqueued items should be the same as the queue size