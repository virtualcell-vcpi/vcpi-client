from __future__ import annotations

import os
from unittest.mock import patch

import httpx
import polars as pl
import pytest
import respx

from vcpi.data import (
    EMPTY_CHEM_DF,
    SUPABASE_FUNCTIONS_URL,
    SUPABASE_KEY,
    _clear_token_cache,
    _get_token,
    _headers,
    _safe_load_chem,
    describe,
    list_datasets,
    load_chem,
    load_metadata,
    query,
    resolve_dataset_url,
)

from .conftest import (
    FAKE_TOKEN,
    SAMPLE_COMPOUNDS_JSON,
    SAMPLE_DATASETS_JSON,
    SAMPLE_GET_DATASET_JSON,
    SAMPLE_LIST_AUTHORIZED_URLS_JSON,
    SAMPLE_METADATA_CSV,
)


# ---------------------------------------------------------------------------
# _get_token / _clear_token_cache / _headers
# ---------------------------------------------------------------------------
class TestGetToken:
    def test_returns_env_var(self, set_token_env):
        assert _get_token() == FAKE_TOKEN

    def test_falls_back_to_keyring(self, mock_keyring):
        mock_keyring["vcpi-client:TVC_TOKEN"] = "keyring-token"
        assert _get_token() == "keyring-token"

    def test_raises_when_missing(self, mock_keyring):
        with pytest.raises(PermissionError, match="TVC_TOKEN not found"):
            _get_token()

    def test_caches_across_calls(self, set_token_env):
        t1 = _get_token()
        os.environ.pop("TVC_TOKEN")
        t2 = _get_token()
        assert t1 == t2 == FAKE_TOKEN

    def test_clear_resets_cache(self, set_token_env):
        _get_token()
        _clear_token_cache()
        os.environ.pop("TVC_TOKEN")
        with patch("vcpi.data.keyring.get_password", return_value=None):
            with pytest.raises(PermissionError):
                _get_token()


class TestHeaders:
    def test_keys_present(self, set_token_env):
        h = _headers()
        assert h["apikey"] == SUPABASE_KEY
        assert h["Authorization"] == f"Bearer {FAKE_TOKEN}"
        assert h["Content-Type"] == "application/json"


# ---------------------------------------------------------------------------
# list_datasets
# ---------------------------------------------------------------------------
class TestListDatasets:
    @respx.mock
    def test_parses_json(self, set_token_env):
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/list-datasets").mock(
            return_value=httpx.Response(200, json=SAMPLE_DATASETS_JSON)
        )
        df = list_datasets()
        assert isinstance(df, pl.DataFrame)
        assert df.shape[0] == 1
        assert "job_id" in df.columns


# ---------------------------------------------------------------------------
# resolve_dataset_url
# ---------------------------------------------------------------------------
class TestResolveDatasetUrl:
    @respx.mock
    def test_returns_url(self, set_token_env):
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/get-dataset").mock(
            return_value=httpx.Response(200, json=SAMPLE_GET_DATASET_JSON)
        )
        url = resolve_dataset_url("tvc-test-001")
        assert url == "https://example.com/data.parquet"

    @respx.mock
    def test_raises_when_no_url(self, set_token_env):
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/get-dataset").mock(
            return_value=httpx.Response(200, json={"job_id": "x"})
        )
        with pytest.raises(ValueError, match="No parquet URL"):
            resolve_dataset_url("x")


# ---------------------------------------------------------------------------
# load_metadata
# ---------------------------------------------------------------------------
class TestLoadMetadata:
    @respx.mock
    def test_parses_csv(self, set_token_env):
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/download-dataset-metadata").mock(
            return_value=httpx.Response(200, content=SAMPLE_METADATA_CSV)
        )
        df = load_metadata("tvc-test-001")
        assert isinstance(df, pl.DataFrame)
        assert df.shape[0] == 2
        assert "sequenced_id" in df.columns
        assert "percent_mitochondrial" in df.columns


# ---------------------------------------------------------------------------
# load_chem
# ---------------------------------------------------------------------------
class TestLoadChem:
    @respx.mock
    def test_parses_compounds(self, set_token_env):
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/get-dataset-compounds").mock(
            return_value=httpx.Response(200, json=SAMPLE_COMPOUNDS_JSON)
        )
        df = load_chem("tvc-test-001")
        assert isinstance(df, pl.DataFrame)
        assert df.shape[0] == 1
        assert "smiles" in df.columns

    @respx.mock
    def test_returns_empty_on_404(self, set_token_env):
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/get-dataset-compounds").mock(
            return_value=httpx.Response(404)
        )
        df = load_chem("nonexistent")
        assert df.shape[0] == 0
        assert df.columns == EMPTY_CHEM_DF.columns

    @respx.mock
    def test_returns_empty_on_500(self, set_token_env):
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/get-dataset-compounds").mock(
            return_value=httpx.Response(500)
        )
        df = load_chem("bad")
        assert df.shape[0] == 0

    @respx.mock
    def test_returns_empty_when_no_compounds(self, set_token_env):
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/get-dataset-compounds").mock(
            return_value=httpx.Response(200, json={"compounds": []})
        )
        df = load_chem("empty")
        assert df.shape[0] == 0
        assert df.columns == EMPTY_CHEM_DF.columns


class TestSafeLoadChem:
    @respx.mock
    def test_returns_empty_on_exception(self, set_token_env):
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/get-dataset-compounds").mock(
            side_effect=httpx.ConnectError("boom")
        )
        df = _safe_load_chem("fail")
        assert df.shape[0] == 0
        assert df.columns == EMPTY_CHEM_DF.columns


# ---------------------------------------------------------------------------
# query
# ---------------------------------------------------------------------------
class TestQuery:
    @respx.mock
    def test_404_raises_valueerror(self, set_token_env):
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/get-dataset").mock(
            return_value=httpx.Response(404)
        )
        with pytest.raises(ValueError, match="Dataset not found"):
            query(job_id="bad-id")

    @respx.mock
    def test_single_job_query(self, set_token_env):
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/get-dataset").mock(
            return_value=httpx.Response(200, json=SAMPLE_GET_DATASET_JSON)
        )
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/download-dataset-metadata").mock(
            return_value=httpx.Response(200, content=SAMPLE_METADATA_CSV)
        )
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/get-dataset-compounds").mock(
            return_value=httpx.Response(200, json=SAMPLE_COMPOUNDS_JSON)
        )
        df = query(
            job_id="tvc-test-001",
            sql="SELECT sequenced_id, cell_line FROM metadata LIMIT 1",
        )
        assert isinstance(df, pl.DataFrame)
        assert df.shape[0] == 1
        assert "sequenced_id" in df.columns

    @respx.mock
    def test_collective_query(self, set_token_env):
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/list-authorized-urls").mock(
            return_value=httpx.Response(
                200, json=SAMPLE_LIST_AUTHORIZED_URLS_JSON
            )
        )
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/download-dataset-metadata").mock(
            return_value=httpx.Response(200, content=SAMPLE_METADATA_CSV)
        )
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/get-dataset-compounds").mock(
            return_value=httpx.Response(200, json=SAMPLE_COMPOUNDS_JSON)
        )
        df = query(sql="SELECT COUNT(*) AS n FROM metadata")
        assert df["n"][0] == 2

    @respx.mock
    def test_join_metadata_chemistry(self, set_token_env):
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/get-dataset").mock(
            return_value=httpx.Response(200, json=SAMPLE_GET_DATASET_JSON)
        )
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/download-dataset-metadata").mock(
            return_value=httpx.Response(200, content=SAMPLE_METADATA_CSV)
        )
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/get-dataset-compounds").mock(
            return_value=httpx.Response(200, json=SAMPLE_COMPOUNDS_JSON)
        )
        df = query(
            job_id="tvc-test-001",
            sql="""
                SELECT m.sequenced_id, c.smiles
                FROM metadata m
                JOIN chemistry c ON c.compound = m.compound
            """,
        )
        assert isinstance(df, pl.DataFrame)
        assert "smiles" in df.columns

    @respx.mock
    def test_empty_manifest_returns_empty(self, set_token_env):
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/list-authorized-urls").mock(
            return_value=httpx.Response(200, json={"urls": []})
        )
        df = query(sql="SELECT 1")
        assert df.is_empty()


# ---------------------------------------------------------------------------
# describe
# ---------------------------------------------------------------------------
class TestDescribe:
    @respx.mock
    def test_returns_both_schemas(self, set_token_env):
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/get-dataset").mock(
            return_value=httpx.Response(200, json=SAMPLE_GET_DATASET_JSON)
        )
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/download-dataset-metadata").mock(
            return_value=httpx.Response(200, content=SAMPLE_METADATA_CSV)
        )
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/get-dataset-compounds").mock(
            return_value=httpx.Response(200, json=SAMPLE_COMPOUNDS_JSON)
        )
        result = describe(job_id="tvc-test-001")
        assert "metadata" in result
        assert "chemistry" in result
        assert "column_name" in result["metadata"].columns
        assert "column_name" in result["chemistry"].columns
