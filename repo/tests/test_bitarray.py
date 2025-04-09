import pytest

import sys, os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from crawler.seen import BitArraySeenURLTester

@pytest.fixture
def tester():
    return BitArraySeenURLTester(verbose=False, capacity=10)

def test_initial_seen_count(tester):
    assert tester.seen_count() == 0, "Initial seen count should be 0."

def test_mark_and_check_seen(tester):
    tester.mark_seen(3)
    assert tester.is_seen(3), "URL with docid 3 should be marked as seen."
    assert tester.seen_count() == 1, "Seen count should be 1 after marking one URL."

def test_is_seen_when_not_marked(tester):
    assert not tester.is_seen(5), "URL with docid 5 should not be seen initially."

def test_mark_multiple_and_check_seen(tester):
    tester.mark_seen(1)
    tester.mark_seen(7)
    assert tester.is_seen(1), "URL with docid 1 should be marked as seen."
    assert tester.is_seen(7), "URL with docid 7 should be marked as seen."
    assert tester.seen_count() == 2, "Seen count should be 2 after marking two URLs."

def test_mark_seen_multiple_times(tester):
    tester.mark_seen(2)
    tester.mark_seen(2)  # Marking again
    assert tester.is_seen(2), "URL with docid 2 should be marked as seen."
    assert tester.seen_count() == 1, "Seen count should be 1 after marking the same URL twice."

def test_capacity_limit(tester):
    tester.mark_seen(9)
    assert tester.is_seen(9), "URL with docid 9 should be marked as seen within capacity."

def test_over_capacity(tester):
    with pytest.raises(IndexError):
        tester.mark_seen(11)