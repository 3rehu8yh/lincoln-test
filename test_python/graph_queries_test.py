import json
import os
from .graph_queries import most_diverse_journal


def test_most_diverse_journal() -> None:
    with open(os.path.join(os.path.dirname(__file__),
                           'expected_graph.json')) as f:
        graph = json.load(f)

    assert most_diverse_journal(graph) == 'Psychopharmacology'
