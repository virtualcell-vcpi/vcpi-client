from __future__ import annotations

import os
from unittest.mock import patch

import httpx
import pytest
import respx

from vcpi.auth import _validate_token, login
from vcpi.data import SUPABASE_FUNCTIONS_URL

from .conftest import FAKE_TOKEN


class TestValidateToken:
    @respx.mock
    def test_valid_token_returns_true(self):
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/list-datasets").mock(
            return_value=httpx.Response(200, json={"datasets": []})
        )
        assert _validate_token("good-token") is True

    @respx.mock
    def test_unauthorized_returns_false(self):
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/list-datasets").mock(
            return_value=httpx.Response(401)
        )
        assert _validate_token("bad-token") is False

    @respx.mock
    def test_forbidden_returns_false(self):
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/list-datasets").mock(
            return_value=httpx.Response(403)
        )
        assert _validate_token("bad-token") is False

    @respx.mock
    def test_network_error_returns_false(self):
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/list-datasets").mock(
            side_effect=httpx.ConnectError("connection refused")
        )
        assert _validate_token("any-token") is False


class TestLogin:
    @respx.mock
    def test_already_logged_in(self, mock_keyring, capsys):
        mock_keyring["vcpi-client:TVC_TOKEN"] = FAKE_TOKEN
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/list-datasets").mock(
            return_value=httpx.Response(200, json={"datasets": []})
        )
        login()
        assert "Already logged in." in capsys.readouterr().out
        assert os.environ.get("TVC_TOKEN") == FAKE_TOKEN

    @respx.mock
    def test_explicit_token_success(self, mock_keyring, capsys):
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/list-datasets").mock(
            return_value=httpx.Response(200, json={"datasets": []})
        )
        login(token="new-valid-token")
        out = capsys.readouterr().out
        assert "LOGIN SUCCESSFUL" in out
        assert mock_keyring["vcpi-client:TVC_TOKEN"] == "new-valid-token"
        assert os.environ.get("TVC_TOKEN") == "new-valid-token"

    @respx.mock
    def test_explicit_token_invalid(self, mock_keyring, capsys):
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/list-datasets").mock(
            return_value=httpx.Response(401)
        )
        login(token="revoked-token")
        out = capsys.readouterr().out
        assert "LOGIN FAILED" in out
        assert "vcpi-client:TVC_TOKEN" not in mock_keyring

    @respx.mock
    def test_prompts_when_no_keyring_token(self, mock_keyring, capsys):
        route = respx.get(f"{SUPABASE_FUNCTIONS_URL}/list-datasets").mock(
            return_value=httpx.Response(200, json={"datasets": []})
        )
        with patch("vcpi.auth.input", return_value="prompted-token"):
            login()
        out = capsys.readouterr().out
        assert "Authentication required." in out
        assert "LOGIN SUCCESSFUL" in out
        assert mock_keyring["vcpi-client:TVC_TOKEN"] == "prompted-token"

    @respx.mock
    def test_empty_prompt_shows_error(self, mock_keyring, capsys):
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/list-datasets").mock(
            return_value=httpx.Response(401)
        )
        with patch("vcpi.auth.input", return_value=""):
            login()
        out = capsys.readouterr().out
        assert "No token provided." in out

    @respx.mock
    def test_stale_keyring_token_prompts(self, mock_keyring, capsys):
        """When keyring has a token but it's revoked, prompt for a new one."""
        mock_keyring["vcpi-client:TVC_TOKEN"] = "stale-token"
        call_count = 0

        def _mock_validate(request):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return httpx.Response(401)
            return httpx.Response(200, json={"datasets": []})

        respx.get(f"{SUPABASE_FUNCTIONS_URL}/list-datasets").mock(
            side_effect=_mock_validate
        )
        with patch("vcpi.auth.input", return_value="fresh-token"):
            login()
        out = capsys.readouterr().out
        assert "Authentication required." in out
        assert "LOGIN SUCCESSFUL" in out
        assert mock_keyring["vcpi-client:TVC_TOKEN"] == "fresh-token"

    @respx.mock
    def test_login_syncs_env_var_over_stale(self, mock_keyring, capsys):
        """login() should override a stale TVC_TOKEN env var."""
        os.environ["TVC_TOKEN"] = "stale-env-token"
        respx.get(f"{SUPABASE_FUNCTIONS_URL}/list-datasets").mock(
            return_value=httpx.Response(200, json={"datasets": []})
        )
        login(token="new-token")
        assert os.environ["TVC_TOKEN"] == "new-token"
