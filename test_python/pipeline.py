"""
The data pipeline.

The function `extract_drug_references` is the entry point for
the data pipeline.  We use Dask to abstract away the logic around data
extraction: the input dataframes can come from local storage, partitioned
parquet files on AWS (which is a good solution for TB of data), the
function does not care.  The transformation is written in a map-reduce
fashion and can be executed on a distributed Dask cluster.
"""
import re
from typing import Sequence, List, Any
import datetime
import pandas as pd
import dask.dataframe as ddf
import dask.bag as dbg
from dask import delayed

from .pipeline_util import parse_date, format_date
from .graph import Graph, Drug, JournalReference, ArticleReference, ClinicalTrialReference


def extract_drug_references(
    clinical_trials: ddf.DataFrame,
    drugs: ddf.DataFrame,
    articles: ddf.DataFrame,
) -> Graph:
    """
    Extracts all the drug references from a series of clinical trials and
    articles.

    The function uses Dask's architecture to split the input dataframes in
    chunks that can be loaded in memory and to combine them into the
    final result.  This is akin to a map-reduce.  By design, this function
    is scalable and can benefit from distributed computing.  It can also
    run locally with no distributed infrastructure.

    The function produces a `Graph` which can be dumped in a JSON file.
    """

    # Bags are distributed collections of Python objects.  In our case,
    # the objects have type `graph.Drug`.  For each Dask partition, we
    # schedule a delayed call to processing functions that return bags.
    # This is the "map" part.  Note that the processing is lazy: the
    # delayed calls return immediately.
    drug_reference_bags: List[dbg.Bag] = []
    for drug_partition in drugs.partitions:
        for clinical_trial_partition in clinical_trials.partitions:
            drug_reference_bags.append(
                _process_clinical_trials(
                    drugs=drug_partition,
                    clinical_trials=clinical_trial_partition,
                ))
        for article_partition in articles.partitions:
            drug_reference_bags.append(
                _process_articles(
                    drugs=drug_partition,
                    articles=article_partition,
                ))

    # We concatenate these bags into one single bag and we execute our
    # "reduce" step on it.  This reduce step is a "group by drug ID"
    # followed by a reduction of all the objects that deal with the same
    # drug into a single object.
    drug_references = dbg.concat(drug_reference_bags) \
        .foldby(
            key=lambda x: x['id'],
            binop=_combine_references,
        ).compute()

    return {
        'drugs': [ref for drug_id, ref in drug_references],
    }


@delayed
def _process_clinical_trials(
    drugs: pd.DataFrame,
    clinical_trials: pd.DataFrame,
) -> dbg.Bag:
    """
    Processes clinical trials.  The result is a bag of `graph.Drug` objects
    for which `article_references` is always an empty list.
    """
    drug_references: List[Drug] = []
    for i, drug in drugs.iterrows():
        drug_re = _drug_re(drug['drug'])

        journal_references: List[JournalReference] = []

        clinical_trial_references: List[ClinicalTrialReference] = []
        clinical_trials_for_drug = clinical_trials[
            clinical_trials['scientific_title'].str.contains(
                drug_re, case=False, regex=True)]
        for j, clinical_trial in clinical_trials_for_drug.iterrows():
            date = format_date(parse_date(clinical_trial['date']))
            clinical_trial_references.append({
                'date': date,
                'clinical_trial_name': clinical_trial['scientific_title'],
            })
            journal_references.append({
                'date': date,
                'journal_name': clinical_trial['journal'],
                'ref_count': 1,
            })

        drug_references.append({
            'id': drug['atccode'],
            'name': drug['drug'],
            'clinical_trial_references': clinical_trial_references,
            'article_references': [],
            'journal_references': journal_references,
        })
    return dbg.from_sequence(drug_references)


@delayed
def _process_articles(
    drugs: pd.DataFrame,
    articles: pd.DataFrame,
) -> dbg.Bag:
    """
    Processes articles.  The result is a bag of `graph.Drug` objects
    for which `clinical_trial_references` is always an empty list.
    """
    drug_references: List[Drug] = []
    for i, drug in drugs.iterrows():
        drug_re = _drug_re(drug['drug'])

        journal_references: List[JournalReference] = []

        article_references: List[ArticleReference] = []
        articles_for_drug = articles[articles['title'].str.contains(
            drug_re, case=False, regex=True)]
        for j, article in articles_for_drug.iterrows():
            date = format_date(parse_date(article['date']))
            article_references.append({
                'date': date,
                'article_name': article['title'],
            })
            journal_references.append({
                'date': date,
                'journal_name': article['journal'],
                'ref_count': 1,
            })

        drug_references.append({
            'id': drug['atccode'],
            'name': drug['drug'],
            'clinical_trial_references': [],
            'article_references': article_references,
            'journal_references': journal_references,
        })
    return dbg.from_sequence(drug_references)


def _drug_re(drug_name: str) -> str:
    """
    Gets a regular expression to search for a specific drug (we simply
    put word boundaries around the drug name).
    """
    assert re.match(r'^[A-Z0-9-]+$', drug_name), \
        f'invalid drug name "{drug_name}"'
    return r'\b' + drug_name + r'\b'


def _deduplicate_references(references: Sequence[Any],
                            id_name: str) -> Sequence[Any]:
    """
    Deduplicate the references so that there is only one reference
    per date and ID.  The `ref_count` numbers are summed in each of the
    `(date, ID)` buckets.
    """
    if len(references) == 0:
        return []
    df = pd.DataFrame.from_records(references)
    df_grouped = df.groupby([id_name, 'date']).agg('sum').reset_index()
    deduplicated = []
    for i, row in df_grouped.iterrows():
        deduplicated.append({
            id_name: row[id_name],
            'date': row['date'],
            'ref_count': row['ref_count'],
        })
    return deduplicated


def _combine_references(a: Drug, b: Drug) -> Drug:
    """
    Combines drug references.
    """
    assert a['id'] == b['id']
    assert a['name'] == b['name']
    return {
        'id':
            a['id'],
        'name':
            a['name'],
        'clinical_trial_references':
            list(a['clinical_trial_references']) +
            list(b['clinical_trial_references']),
        'article_references':
            list(a['article_references']) + list(b['article_references']),
        'journal_references':
            _deduplicate_references(
                list(a['journal_references']) + list(b['journal_references']),
                id_name='journal_name',
            )
    }


if __name__ == '__main__':
    # Entrypoint for testing.
    import json
    import os
    import dask
    from .data_catalogue import test_data_catalogue

    # Don't worry with distributed processing when testing the pipeline.
    dask.config.set(scheduler='threads')

    # Loads the catalogue (metadata only).
    catalogue_path = os.path.join(
        os.path.dirname(__file__), '../problem_statement')
    catalogue = test_data_catalogue(catalogue_path)

    # Extracts the graph.
    graph = extract_drug_references(
        clinical_trials=catalogue.clinical_trials,
        drugs=catalogue.drugs,
        articles=catalogue.articles,
    )

    # Dumps the graph to stdout.
    print(json.dumps(graph, sort_keys=True, indent=4))
