"""Integration tests that hit the real VCPI API.

Skipped unless TVC_TOKEN is available in the environment or keyring.
Run with:  pytest -m integration
"""

from __future__ import annotations

import os

import keyring
import polars as pl
import pytest

import vcpi
from vcpi.data import EMPTY_CHEM_DF

_token = os.environ.get("TVC_TOKEN") or keyring.get_password("vcpi-client", "TVC_TOKEN")
if _token and not os.environ.get("TVC_TOKEN"):
    os.environ["TVC_TOKEN"] = _token

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not _token, reason="No TVC_TOKEN in env or keyring"),
]


@pytest.fixture(scope="module")
def first_job_id():
    datasets = vcpi.list_datasets()
    assert datasets.shape[0] > 0, "No datasets available"
    return datasets["job_id"][0]


class TestListDatasets:
    def test_returns_dataframe_with_job_id(self):
        df = vcpi.list_datasets()
        assert isinstance(df, pl.DataFrame)
        assert df.shape[0] > 0
        assert "job_id" in df.columns


class TestQuery:
    def test_metadata_limit(self, first_job_id):
        df = vcpi.query(
            job_id=first_job_id,
            sql="SELECT * FROM metadata LIMIT 5",
        )
        assert isinstance(df, pl.DataFrame)
        assert df.shape[0] <= 5
        assert "sequenced_id" in df.columns

    def test_chemistry_limit(self, first_job_id):
        df = vcpi.query(
            job_id=first_job_id,
            sql="SELECT * FROM chemistry LIMIT 5",
        )
        assert isinstance(df, pl.DataFrame)
        assert "compound" in df.columns

    def test_join_works(self, first_job_id):
        df = vcpi.query(
            job_id=first_job_id,
            sql="""
                SELECT m.sequenced_id, c.smiles
                FROM metadata m
                JOIN chemistry c ON c.compound = m.compound
                LIMIT 5
            """,
        )
        assert isinstance(df, pl.DataFrame)
        assert "smiles" in df.columns

    def test_count_returns_single_row(self, first_job_id):
        df = vcpi.query(
            job_id=first_job_id,
            sql="SELECT COUNT(*) AS n FROM metadata",
        )
        assert df.shape[0] == 1
        assert df["n"][0] > 0

    def test_invalid_job_id_raises(self):
        with pytest.raises(ValueError, match="Dataset not found"):
            vcpi.query(job_id="__nonexistent_job__")


class TestDescribe:
    def test_returns_schemas(self, first_job_id):
        schemas = vcpi.describe(job_id=first_job_id)
        assert "metadata" in schemas
        assert "chemistry" in schemas
        assert "column_name" in schemas["metadata"].columns
        assert schemas["metadata"].shape[0] > 0


class TestLoadMetadata:
    def test_returns_dataframe(self, first_job_id):
        df = vcpi.load_metadata(first_job_id)
        assert isinstance(df, pl.DataFrame)
        assert df.shape[0] > 0
        assert "sequenced_id" in df.columns
        assert "percent_mitochondrial" in df.columns


class TestLoadChem:
    def test_returns_dataframe(self, first_job_id):
        df = vcpi.load_chem(first_job_id)
        assert isinstance(df, pl.DataFrame)
        expected_cols = {"compound", "user_compound_id", "smiles", "log_p", "tpsa"}
        assert expected_cols.issubset(set(df.columns))

    def test_nonexistent_returns_empty(self):
        df = vcpi.load_chem("__nonexistent_job__")
        assert df.shape[0] == 0
        assert df.columns == EMPTY_CHEM_DF.columns
