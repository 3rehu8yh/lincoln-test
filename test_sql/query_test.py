"""
Tests the SQL requests.
"""

import sqlite3
import os
import pytest
import pandas as pd
import sqlalchemy


@pytest.fixture
def db_conn():
    catalogue_path = os.path.join(
        os.path.dirname(__file__), '../problem_statement')

    db = sqlalchemy.create_engine('sqlite://', echo=True)
    pd.read_csv(f'{catalogue_path}/transactions.csv', parse_dates=['date']) \
        .to_sql('transactions', db)
    pd.read_csv(f'{catalogue_path}/product_nomenclature.csv') \
        .to_sql('product_nomenclature', db)
    return db


def test_query_1(db_conn):
    with open(os.path.join(os.path.dirname(__file__), 'query_1.sql')) as f:
        query_1 = f.read()
    df = pd.read_sql_query(query_1, db_conn)

    expected_df = pd.read_csv(
        os.path.join(os.path.dirname(__file__), 'expected_query_1_result.csv'))
    pd.testing.assert_frame_equal(expected_df, df)


def test_query_2(db_conn):
    with open(os.path.join(os.path.dirname(__file__), 'query_2.sql')) as f:
        query_2 = f.read()
    df = pd.read_sql_query(query_2, db_conn)

    expected_df = pd.read_csv(
        os.path.join(os.path.dirname(__file__), 'expected_query_2_result.csv'))
    pd.testing.assert_frame_equal(expected_df, df)
