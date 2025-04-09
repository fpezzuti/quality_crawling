import pytest

from crawler.frontier import BreadthFirstSearchFrontierManager
from crawler.webpage import WebPage

@pytest.fixture
def frontier_manager():
    return BreadthFirstSearchFrontierManager(verbose=True)


def test_add_page(frontier_manager):
    """Test adding a page to the frontier."""
    page = WebPage(id=1, url="http://example.com")
    frontier_manager.add(page)
    
    # Check that the URL was added to the queue
    assert frontier_manager.enqueued() == 1

    popped_url = frontier_manager.pop()
    assert popped_url == "http://example.com"
    assert frontier_manager.enqueued() == 0


def test_add_page_with_max_priority(frontier_manager):
    """Test adding a page to the frontier with max priority."""
    page = WebPage(id=1, url="http://example.com")
    frontier_manager.add_with_max_priority(page)
    
    # Check that the URL was added to the queue
    assert frontier_manager.enqueued() == 1
    popped_url = frontier_manager.pop()
    assert popped_url == "http://example.com"
    assert frontier_manager.enqueued() == 0

def test_pop_from_empty_queue(frontier_manager):
    """Test popping from an empty queue raises IndexError."""
    with pytest.raises(IndexError, match="Error: trying to pop from empty queue"):
        frontier_manager.pop()

def test_enqueued(frontier_manager):
    """Test the enqueued method."""
    page1 = WebPage(id=1, url="http://example.com")
    assert frontier_manager.enqueued() == 0
    
    frontier_manager.add(page1)
    assert frontier_manager.enqueued() == 1

    page2 = WebPage(id=9, url="http://example2.com")

    frontier_manager.add(page2)
    assert frontier_manager.enqueued() == 2


    page3 = WebPage(id=17, url="http://example3.com")
    frontier_manager.add(page3)
    assert frontier_manager.enqueued() == 3

    popped_url = frontier_manager.pop()
    assert frontier_manager.enqueued() == 2
    assert popped_url == "http://example.com"

    popped_url = frontier_manager.pop()
    assert frontier_manager.enqueued() == 1
    assert popped_url == "http://example2.com"

    popped_url = frontier_manager.pop()
    assert frontier_manager.enqueued() == 0
    assert popped_url == "http://example3.com"