import json
import os
import dask
from .pipeline import extract_drug_references

dask.config.set(scheduler='threads')


def test_extract_drug_references() -> None:
    from .data_catalogue import test_data_catalogue

    catalogue_path = os.path.join(
        os.path.dirname(__file__), '../problem_statement')
    catalogue = test_data_catalogue(catalogue_path)
    graph = extract_drug_references(
        clinical_trials=catalogue.clinical_trials,
        drugs=catalogue.drugs,
        articles=catalogue.articles,
    )

    with open(os.path.join(os.path.dirname(__file__),
                           'expected_graph.json')) as f:
        expected_graph = json.load(f)

    assert graph == expected_graph
