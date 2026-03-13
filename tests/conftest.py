from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from vcpi.data import _clear_token_cache

FAKE_TOKEN = "test-token-abc123"

SAMPLE_DATASETS_JSON = {
    "datasets": [
        {
            "job_id": "tvc-test-001",
            "imported_at": "2026-01-01T00:00:00+00:00",
            "is_public": True,
            "parquet_file_size_bytes": 100,
            "dataset_name": "Test Dataset",
            "datapoint_count": 50,
            "cell_lines": ["THP-1"],
            "timepoints": ["24h"],
            "metadata_row_count": 50,
        }
    ]
}

SAMPLE_GET_DATASET_JSON = {
    "job_id": "tvc-test-001",
    "parquet_url": "https://example.com/data.parquet",
}

SAMPLE_METADATA_CSV = (
    b"sequenced_id,job_id,compound,user_compound_id,cell_line,timepoint,"
    b"is_control,percent_mitochondrial,total_sequenced_reads\n"
    b"1001,tvc-test-001,cmpd-uuid,Bortezomib,THP-1,24h,false,10.5,2000000\n"
    b"1002,tvc-test-001,cmpd-uuid,Furosemide,THP-1,24h,false,8.3,1500000\n"
)

SAMPLE_COMPOUNDS_JSON = {
    "compounds": [
        {
            "compound": "cmpd-uuid",
            "user_compound_id": "Bortezomib",
            "smiles": "CC(C)CC",
            "purity_pct": 99.0,
            "molecular_weight": 384.24,
            "log_p": 0.36,
            "tpsa": 124.44,
            "inchi_key": "GXJABQQUPOEQRB-UHFFFAOYSA-N",
            "num_rotatable_bonds": 10,
            "num_h_acceptors": 5,
            "num_h_donors": 4,
            "num_atoms": 27,
            "num_bonds": 28,
        }
    ]
}

SAMPLE_LIST_AUTHORIZED_URLS_JSON = {
    "urls": [
        {
            "job_id": "tvc-test-001",
            "parquet_url": "https://example.com/data.parquet",
        }
    ]
}


@pytest.fixture(autouse=True)
def _clean_token_state(request):
    """Reset token cache and remove TVC_TOKEN env var around every unit test.

    Integration tests are left alone so they can use the real token.
    """
    if "integration" in {m.name for m in request.node.iter_markers()}:
        yield
        return

    _clear_token_cache()
    old = os.environ.pop("TVC_TOKEN", None)
    yield
    _clear_token_cache()
    if old is not None:
        os.environ["TVC_TOKEN"] = old
    else:
        os.environ.pop("TVC_TOKEN", None)


@pytest.fixture()
def set_token_env():
    """Set a fake TVC_TOKEN in the environment for the duration of the test."""
    os.environ["TVC_TOKEN"] = FAKE_TOKEN
    yield FAKE_TOKEN
    os.environ.pop("TVC_TOKEN", None)


@pytest.fixture()
def mock_keyring():
    """Patch keyring get/set so tests never touch the real keychain."""
    store: dict[str, str] = {}

    def _get(service, key):
        return store.get(f"{service}:{key}")

    def _set(service, key, value):
        store[f"{service}:{key}"] = value

    with (
        patch("vcpi.data.keyring.get_password", side_effect=_get),
        patch("vcpi.auth.keyring.get_password", side_effect=_get),
        patch("vcpi.auth.keyring.set_password", side_effect=_set),
    ):
        yield store
