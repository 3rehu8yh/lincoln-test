"""
Ad hoc queries on the `Graph` object.  These queries lie outside of the
data pipeline.
"""
from typing import Dict

from .graph import Graph


def most_diverse_journal(graph: Graph) -> str:
    """
    Returns the name of the most diverse journal.
    Note that, if there is a tie, an arbitrary journal, amongst the
    journals that share the top spot, is returned.
    """
    drug_count_per_journal: Dict[str, int] = {}
    for drug in graph['drugs']:
        journals_for_drug = [
            ref['journal_name'] for ref in drug['journal_references']
        ]
        for journal in set(journals_for_drug):
            drug_count_per_journal[journal] = \
                drug_count_per_journal.get(journal, 0) + 1

    most_diverse_journal = None
    max_drug_count = None
    for journal, count in drug_count_per_journal.items():
        if most_diverse_journal is None or max_drug_count < count:
            most_diverse_journal = journal
            max_drug_count = count

    assert most_diverse_journal is not None
    return most_diverse_journal
