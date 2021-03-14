"""
Data catalogue.

The data catalogue exposes all the datasets relevant to this project.

By using Dask, we ensure that, even if we manipulate dataframes, the
computations are made lazily and the dataframes are not loaded in 
memory when the catalogue is created (only the metadata is read by Dask
when the catalogue is created).
"""
import pandas as pd
import dask.dataframe as ddf
import attr
import os


@attr.s(frozen=True, auto_attribs=True)
class DataCatalogue:
    clinical_trials: ddf.DataFrame
    drugs: ddf.DataFrame
    articles: ddf.DataFrame


def test_data_catalogue(path: str) -> DataCatalogue:
    """
    Returns a test catalogue made of local data.
    """
    return DataCatalogue(
        clinical_trials=_read_test_csv(
            os.path.join(path, 'clinical_trials.csv')),
        drugs=_read_test_csv(os.path.join(path, 'drugs.csv')),
        articles=_read_test_csv(os.path.join(path, 'pubmed.csv')),
    )


def _read_test_csv(path: str) -> ddf.DataFrame:
    """
    Reads a CSV file for testing purposes.  To test distributed processing,
    we force the number of partitions to 2.
    """
    df = pd.read_csv(path)
    return ddf.from_pandas(df, npartitions=2)
