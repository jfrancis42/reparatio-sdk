"""Tests for reparatio.client.Reparatio — all methods mocked at the httpx level."""
from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from reparatio import Reparatio
from reparatio.exceptions import (
    APIError,
    AuthenticationError,
    FileTooLargeError,
    InsufficientPlanError,
    ParseError,
)
from reparatio.models import ConvertResult, FormatsResult, InspectResult, MeResult

from .conftest import make_response


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FORMATS_JSON = {"input": ["csv", "parquet", "xlsx"], "output": ["csv", "parquet"]}

_INSPECT_JSON = {
    "filename": "data.csv",
    "detected_encoding": "utf-8",
    "rows": 500,
    "columns": [
        {"name": "id", "dtype": "Int64", "null_count": 0, "unique_count": 500},
        {"name": "name", "dtype": "Utf8", "null_count": 1, "unique_count": 499},
    ],
    "preview": [{"id": "1", "name": "Alice"}],
    "sheets": [],
}

_ME_JSON = {
    "email": "user@example.com",
    "plan": "pro",
    "active": True,
    "api_access": True,
    "expires_at": None,
}

CSV_BYTES = b"id,name\n1,Alice\n2,Bob\n"
PARQUET_BYTES = b"PAR1" + b"\x00" * 20  # fake parquet magic bytes


# ---------------------------------------------------------------------------
# Context manager + close
# ---------------------------------------------------------------------------

class TestClientLifecycle:
    def test_context_manager_returns_self(self):
        with Reparatio(api_key="rp_test") as c:
            assert isinstance(c, Reparatio)

    def test_close_does_not_raise(self):
        c = Reparatio(api_key="rp_test")
        c.close()  # should not raise

    def test_env_var_key_used_when_no_explicit_key(self, monkeypatch):
        monkeypatch.setenv("REPARATIO_API_KEY", "rp_env_key")
        c = Reparatio()
        assert c._client.headers.get("x-api-key") == "rp_env_key"

    def test_explicit_key_takes_precedence_over_env(self, monkeypatch):
        monkeypatch.setenv("REPARATIO_API_KEY", "rp_env_key")
        c = Reparatio(api_key="rp_explicit")
        assert c._client.headers.get("x-api-key") == "rp_explicit"

    def test_no_key_no_auth_header(self, monkeypatch):
        monkeypatch.delenv("REPARATIO_API_KEY", raising=False)
        c = Reparatio()
        assert "x-api-key" not in c._client.headers


# ---------------------------------------------------------------------------
# formats()
# ---------------------------------------------------------------------------

class TestFormats:
    def test_success(self, client):
        resp = make_response(200, json_data=_FORMATS_JSON)
        with patch.object(client._client, "get", return_value=resp):
            result = client.formats()
        assert isinstance(result, FormatsResult)
        assert "csv" in result.input
        assert "parquet" in result.output

    def test_401_raises_authentication_error(self, client):
        resp = make_response(401, json_data={"detail": "Invalid key"})
        with patch.object(client._client, "get", return_value=resp):
            with pytest.raises(AuthenticationError):
                client.formats()

    def test_500_raises_api_error(self, client):
        resp = make_response(500, json_data={"detail": "Internal error"})
        with patch.object(client._client, "get", return_value=resp):
            with pytest.raises(APIError) as exc_info:
                client.formats()
        assert exc_info.value.status_code == 500


# ---------------------------------------------------------------------------
# me()
# ---------------------------------------------------------------------------

class TestMe:
    def test_success(self, client):
        resp = make_response(200, json_data=_ME_JSON)
        with patch.object(client._client, "get", return_value=resp):
            result = client.me()
        assert isinstance(result, MeResult)
        assert result.email == "user@example.com"
        assert result.plan == "pro"
        assert result.active is True
        assert result.api_access is True

    def test_401_raises_authentication_error(self, client):
        resp = make_response(401, json_data={"detail": "Unauthorized"})
        with patch.object(client._client, "get", return_value=resp):
            with pytest.raises(AuthenticationError):
                client.me()

    def test_403_raises_authentication_error(self, client):
        resp = make_response(403, json_data={"detail": "Forbidden"})
        with patch.object(client._client, "get", return_value=resp):
            with pytest.raises(AuthenticationError):
                client.me()


# ---------------------------------------------------------------------------
# inspect()
# ---------------------------------------------------------------------------

class TestInspect:
    def test_success_with_path(self, client, tmp_path):
        f = tmp_path / "data.csv"
        f.write_bytes(CSV_BYTES)
        resp = make_response(200, json_data=_INSPECT_JSON)
        with patch.object(client._client, "post", return_value=resp) as mock_post:
            result = client.inspect(f)
        assert isinstance(result, InspectResult)
        assert result.filename == "data.csv"
        assert result.rows == 500
        assert len(result.columns) == 2
        # Verify filename was derived from path
        call_files = mock_post.call_args.kwargs.get("files") or mock_post.call_args[1].get("files")
        assert call_files["file"][0] == "data.csv"

    def test_success_with_bytes(self, client):
        resp = make_response(200, json_data=_INSPECT_JSON)
        with patch.object(client._client, "post", return_value=resp) as mock_post:
            result = client.inspect(CSV_BYTES, filename="upload.csv")
        assert isinstance(result, InspectResult)

    def test_no_header_flag_sent_as_true(self, client, tmp_path):
        f = tmp_path / "data.csv"
        f.write_bytes(CSV_BYTES)
        resp = make_response(200, json_data=_INSPECT_JSON)
        with patch.object(client._client, "post", return_value=resp) as mock_post:
            client.inspect(f, no_header=True)
        sent_data = mock_post.call_args.kwargs.get("data") or mock_post.call_args[1].get("data")
        assert sent_data["no_header"] == "true"

    def test_fix_encoding_false_sent(self, client, tmp_path):
        f = tmp_path / "data.csv"
        f.write_bytes(CSV_BYTES)
        resp = make_response(200, json_data=_INSPECT_JSON)
        with patch.object(client._client, "post", return_value=resp) as mock_post:
            client.inspect(f, fix_encoding=False)
        sent_data = mock_post.call_args.kwargs.get("data") or mock_post.call_args[1].get("data")
        assert sent_data["fix_encoding"] == "false"

    def test_preview_rows_sent(self, client, tmp_path):
        f = tmp_path / "data.csv"
        f.write_bytes(CSV_BYTES)
        resp = make_response(200, json_data=_INSPECT_JSON)
        with patch.object(client._client, "post", return_value=resp) as mock_post:
            client.inspect(f, preview_rows=20)
        sent_data = mock_post.call_args.kwargs.get("data") or mock_post.call_args[1].get("data")
        assert sent_data["preview_rows"] == "20"

    def test_422_raises_parse_error(self, client, tmp_path):
        f = tmp_path / "data.bin"
        f.write_bytes(b"\x00\x01\x02")
        resp = make_response(422, json_data={"detail": "Cannot parse file"})
        with patch.object(client._client, "post", return_value=resp):
            with pytest.raises(ParseError):
                client.inspect(f)

    def test_string_path_accepted(self, client, tmp_path):
        f = tmp_path / "data.csv"
        f.write_bytes(CSV_BYTES)
        resp = make_response(200, json_data=_INSPECT_JSON)
        with patch.object(client._client, "post", return_value=resp):
            result = client.inspect(str(f))
        assert isinstance(result, InspectResult)


# ---------------------------------------------------------------------------
# convert()
# ---------------------------------------------------------------------------

class TestConvert:
    def test_success_returns_convert_result(self, client, tmp_path):
        f = tmp_path / "data.csv"
        f.write_bytes(CSV_BYTES)
        resp = make_response(200, content=PARQUET_BYTES,
                             headers={"content-disposition": 'attachment; filename="data.parquet"'})
        with patch.object(client._client, "post", return_value=resp):
            result = client.convert(f, "parquet")
        assert isinstance(result, ConvertResult)
        assert result.content == PARQUET_BYTES
        assert result.filename == "data.parquet"
        assert result.warning is None

    def test_warning_header_propagated(self, client, tmp_path):
        f = tmp_path / "data.csv"
        f.write_bytes(CSV_BYTES)
        resp = make_response(200, content=PARQUET_BYTES,
                             headers={"x-reparatio-warning": "2 columns truncated"})
        with patch.object(client._client, "post", return_value=resp):
            result = client.convert(f, "parquet")
        assert result.warning == "2 columns truncated"

    def test_filename_fallback_when_no_content_disposition(self, client, tmp_path):
        f = tmp_path / "data.csv"
        f.write_bytes(CSV_BYTES)
        resp = make_response(200, content=PARQUET_BYTES)
        with patch.object(client._client, "post", return_value=resp):
            result = client.convert(f, "parquet")
        assert result.filename == "data.parquet"

    def test_encoding_override_sent(self, client, tmp_path):
        f = tmp_path / "mainframe.dat"
        f.write_bytes(b"\xc1\xc2\xc3")  # fake EBCDIC bytes
        resp = make_response(200, content=CSV_BYTES)
        with patch.object(client._client, "post", return_value=resp) as mock_post:
            client.convert(f, "csv", encoding_override="cp037")
        sent_data = mock_post.call_args.kwargs.get("data") or mock_post.call_args[1].get("data")
        assert sent_data.get("encoding_override") == "cp037"

    def test_encoding_override_absent_when_none(self, client, tmp_path):
        f = tmp_path / "data.csv"
        f.write_bytes(CSV_BYTES)
        resp = make_response(200, content=PARQUET_BYTES)
        with patch.object(client._client, "post", return_value=resp) as mock_post:
            client.convert(f, "parquet")
        sent_data = mock_post.call_args.kwargs.get("data") or mock_post.call_args[1].get("data")
        assert "encoding_override" not in sent_data

    def test_select_columns_serialised_as_json(self, client, tmp_path):
        f = tmp_path / "data.csv"
        f.write_bytes(CSV_BYTES)
        resp = make_response(200, content=CSV_BYTES)
        with patch.object(client._client, "post", return_value=resp) as mock_post:
            client.convert(f, "csv", select_columns=["id", "name"])
        sent_data = mock_post.call_args.kwargs.get("data") or mock_post.call_args[1].get("data")
        assert json.loads(sent_data["select_columns"]) == ["id", "name"]

    def test_cast_columns_serialised_as_json(self, client, tmp_path):
        f = tmp_path / "data.csv"
        f.write_bytes(CSV_BYTES)
        resp = make_response(200, content=PARQUET_BYTES)
        with patch.object(client._client, "post", return_value=resp) as mock_post:
            client.convert(f, "parquet", cast_columns={"price": {"type": "Float64"}})
        sent_data = mock_post.call_args.kwargs.get("data") or mock_post.call_args[1].get("data")
        assert json.loads(sent_data["cast_columns"]) == {"price": {"type": "Float64"}}

    def test_null_values_serialised_as_json(self, client, tmp_path):
        f = tmp_path / "data.csv"
        f.write_bytes(CSV_BYTES)
        resp = make_response(200, content=PARQUET_BYTES)
        with patch.object(client._client, "post", return_value=resp) as mock_post:
            client.convert(f, "parquet", null_values=["N/A", "NULL", "-"])
        sent_data = mock_post.call_args.kwargs.get("data") or mock_post.call_args[1].get("data")
        assert json.loads(sent_data["null_values"]) == ["N/A", "NULL", "-"]

    def test_deduplicate_flag_sent(self, client, tmp_path):
        f = tmp_path / "data.csv"
        f.write_bytes(CSV_BYTES)
        resp = make_response(200, content=PARQUET_BYTES)
        with patch.object(client._client, "post", return_value=resp) as mock_post:
            client.convert(f, "parquet", deduplicate=True)
        sent_data = mock_post.call_args.kwargs.get("data") or mock_post.call_args[1].get("data")
        assert sent_data["deduplicate"] == "true"

    def test_402_raises_insufficient_plan(self, client, tmp_path):
        f = tmp_path / "data.csv"
        f.write_bytes(CSV_BYTES)
        resp = make_response(402, json_data={"detail": "Requires Professional plan"})
        with patch.object(client._client, "post", return_value=resp):
            with pytest.raises(InsufficientPlanError):
                client.convert(f, "parquet")

    def test_413_raises_file_too_large(self, client, tmp_path):
        f = tmp_path / "huge.csv"
        f.write_bytes(CSV_BYTES)
        resp = make_response(413, json_data={"detail": "File too large"})
        with patch.object(client._client, "post", return_value=resp):
            with pytest.raises(FileTooLargeError):
                client.convert(f, "parquet")

    def test_422_raises_parse_error(self, client, tmp_path):
        f = tmp_path / "data.bin"
        f.write_bytes(b"\x00\x01\x02")
        resp = make_response(422, json_data={"detail": "Unsupported format"})
        with patch.object(client._client, "post", return_value=resp):
            with pytest.raises(ParseError):
                client.convert(f, "csv")

    def test_500_raises_api_error(self, client, tmp_path):
        f = tmp_path / "data.csv"
        f.write_bytes(CSV_BYTES)
        resp = make_response(500, json_data={"detail": "Server error"})
        with patch.object(client._client, "post", return_value=resp):
            with pytest.raises(APIError) as exc_info:
                client.convert(f, "parquet")
        assert exc_info.value.status_code == 500

    def test_raw_bytes_input(self, client):
        resp = make_response(200, content=CSV_BYTES)
        with patch.object(client._client, "post", return_value=resp):
            result = client.convert(CSV_BYTES, "csv", filename="upload.csv")
        assert result.content == CSV_BYTES


# ---------------------------------------------------------------------------
# batch_convert()
# ---------------------------------------------------------------------------

class TestBatchConvert:
    def test_success(self, client, tmp_path):
        z = tmp_path / "data.zip"
        z.write_bytes(b"PK\x03\x04")  # fake ZIP magic bytes
        zip_out = b"PK\x03\x04converted"
        resp = make_response(200, content=zip_out,
                             headers={"content-disposition": 'attachment; filename="converted.zip"'})
        with patch.object(client._client, "post", return_value=resp):
            result = client.batch_convert(z, "parquet")
        assert result.content == zip_out
        assert result.filename == "converted.zip"
        assert result.warning is None

    def test_errors_header_decoded(self, client, tmp_path):
        import urllib.parse
        z = tmp_path / "data.zip"
        z.write_bytes(b"PK\x03\x04")
        errors = json.dumps([{"file": "bad.bin", "error": "Cannot parse"}])
        resp = make_response(200, content=b"PK\x03\x04",
                             headers={"x-reparatio-errors": urllib.parse.quote(errors)})
        with patch.object(client._client, "post", return_value=resp):
            result = client.batch_convert(z, "csv")
        assert result.warning is not None
        parsed = json.loads(result.warning)
        assert parsed[0]["file"] == "bad.bin"

    def test_no_errors_header_gives_none_warning(self, client, tmp_path):
        z = tmp_path / "data.zip"
        z.write_bytes(b"PK\x03\x04")
        resp = make_response(200, content=b"PK\x03\x04")
        with patch.object(client._client, "post", return_value=resp):
            result = client.batch_convert(z, "parquet")
        assert result.warning is None


# ---------------------------------------------------------------------------
# merge()
# ---------------------------------------------------------------------------

class TestMerge:
    def test_success_left_join(self, client, tmp_path):
        f1 = tmp_path / "orders.csv"
        f2 = tmp_path / "customers.csv"
        f1.write_bytes(b"id,amount\n1,100\n")
        f2.write_bytes(b"id,name\n1,Alice\n")
        resp = make_response(200, content=PARQUET_BYTES,
                             headers={"content-disposition": 'attachment; filename="orders_left_customers.parquet"'})
        with patch.object(client._client, "post", return_value=resp) as mock_post:
            result = client.merge(f1, f2, "left", "parquet", join_on="id")
        assert result.filename == "orders_left_customers.parquet"
        sent_data = mock_post.call_args.kwargs.get("data") or mock_post.call_args[1].get("data")
        assert sent_data["operation"] == "left"
        assert sent_data["join_on"] == "id"

    def test_fallback_filename_without_content_disposition(self, client, tmp_path):
        f1 = tmp_path / "file1.csv"
        f2 = tmp_path / "file2.csv"
        f1.write_bytes(CSV_BYTES)
        f2.write_bytes(CSV_BYTES)
        resp = make_response(200, content=PARQUET_BYTES)
        with patch.object(client._client, "post", return_value=resp):
            result = client.merge(f1, f2, "inner", "parquet")
        assert result.filename == "file1_inner_file2.parquet"

    def test_warning_propagated(self, client, tmp_path):
        f1 = tmp_path / "a.csv"
        f2 = tmp_path / "b.csv"
        f1.write_bytes(CSV_BYTES)
        f2.write_bytes(CSV_BYTES)
        resp = make_response(200, content=PARQUET_BYTES,
                             headers={"x-reparatio-warning": "Column mismatch"})
        with patch.object(client._client, "post", return_value=resp):
            result = client.merge(f1, f2, "append", "parquet")
        assert result.warning == "Column mismatch"

    def test_bytes_inputs_accepted(self, client):
        resp = make_response(200, content=PARQUET_BYTES)
        with patch.object(client._client, "post", return_value=resp):
            result = client.merge(CSV_BYTES, CSV_BYTES, "append", "parquet",
                                  filename1="a.csv", filename2="b.csv")
        assert isinstance(result, ConvertResult)


# ---------------------------------------------------------------------------
# append()
# ---------------------------------------------------------------------------

class TestAppend:
    def test_success_two_files(self, client, tmp_path):
        f1 = tmp_path / "jan.csv"
        f2 = tmp_path / "feb.csv"
        f1.write_bytes(CSV_BYTES)
        f2.write_bytes(CSV_BYTES)
        resp = make_response(200, content=PARQUET_BYTES,
                             headers={"content-disposition": 'attachment; filename="appended.parquet"'})
        with patch.object(client._client, "post", return_value=resp):
            result = client.append([f1, f2], "parquet")
        assert result.filename == "appended.parquet"

    def test_success_three_files(self, client, tmp_path):
        files = []
        for month in ("jan", "feb", "mar"):
            f = tmp_path / f"{month}.csv"
            f.write_bytes(CSV_BYTES)
            files.append(f)
        resp = make_response(200, content=PARQUET_BYTES)
        with patch.object(client._client, "post", return_value=resp) as mock_post:
            client.append(files, "parquet")
        # All three files should be sent
        sent_files = mock_post.call_args.kwargs.get("files") or mock_post.call_args[1].get("files")
        assert len(sent_files) == 3

    def test_fewer_than_two_files_raises_value_error(self, client, tmp_path):
        f = tmp_path / "only.csv"
        f.write_bytes(CSV_BYTES)
        with pytest.raises(ValueError, match="2 files"):
            client.append([f], "parquet")

    def test_empty_list_raises_value_error(self, client):
        with pytest.raises(ValueError):
            client.append([], "parquet")

    def test_fallback_filename(self, client, tmp_path):
        f1 = tmp_path / "a.csv"
        f2 = tmp_path / "b.csv"
        f1.write_bytes(CSV_BYTES)
        f2.write_bytes(CSV_BYTES)
        resp = make_response(200, content=PARQUET_BYTES)
        with patch.object(client._client, "post", return_value=resp):
            result = client.append([f1, f2], "parquet")
        assert result.filename == "appended.parquet"

    def test_custom_filenames_used(self, client):
        resp = make_response(200, content=CSV_BYTES)
        with patch.object(client._client, "post", return_value=resp) as mock_post:
            client.append([CSV_BYTES, CSV_BYTES], "csv",
                          filenames=["jan.csv", "feb.csv"])
        sent_files = mock_post.call_args.kwargs.get("files") or mock_post.call_args[1].get("files")
        names = [t[1][0] for t in sent_files]
        assert "jan.csv" in names
        assert "feb.csv" in names


# ---------------------------------------------------------------------------
# query()
# ---------------------------------------------------------------------------

class TestQuery:
    def test_success(self, client, tmp_path):
        f = tmp_path / "events.parquet"
        f.write_bytes(PARQUET_BYTES)
        resp = make_response(200, content=CSV_BYTES,
                             headers={"content-disposition": 'attachment; filename="events_query.csv"'})
        with patch.object(client._client, "post", return_value=resp) as mock_post:
            result = client.query(f, "SELECT * FROM data LIMIT 10")
        assert result.filename == "events_query.csv"
        assert result.content == CSV_BYTES
        sent_data = mock_post.call_args.kwargs.get("data") or mock_post.call_args[1].get("data")
        assert sent_data["sql"] == "SELECT * FROM data LIMIT 10"

    def test_default_format_is_csv(self, client, tmp_path):
        f = tmp_path / "data.parquet"
        f.write_bytes(PARQUET_BYTES)
        resp = make_response(200, content=CSV_BYTES)
        with patch.object(client._client, "post", return_value=resp) as mock_post:
            client.query(f, "SELECT 1")
        sent_data = mock_post.call_args.kwargs.get("data") or mock_post.call_args[1].get("data")
        assert sent_data["target_format"] == "csv"

    def test_custom_format(self, client, tmp_path):
        f = tmp_path / "data.csv"
        f.write_bytes(CSV_BYTES)
        resp = make_response(200, content=PARQUET_BYTES)
        with patch.object(client._client, "post", return_value=resp) as mock_post:
            client.query(f, "SELECT * FROM data", target_format="parquet")
        sent_data = mock_post.call_args.kwargs.get("data") or mock_post.call_args[1].get("data")
        assert sent_data["target_format"] == "parquet"

    def test_fallback_filename(self, client, tmp_path):
        f = tmp_path / "sales.csv"
        f.write_bytes(CSV_BYTES)
        resp = make_response(200, content=CSV_BYTES)
        with patch.object(client._client, "post", return_value=resp):
            result = client.query(f, "SELECT * FROM data")
        assert result.filename == "sales_query.csv"

    def test_no_warning_field(self, client, tmp_path):
        f = tmp_path / "data.csv"
        f.write_bytes(CSV_BYTES)
        resp = make_response(200, content=CSV_BYTES)
        with patch.object(client._client, "post", return_value=resp):
            result = client.query(f, "SELECT * FROM data")
        # query() does not surface x-reparatio-warning; warning should be absent/None
        assert result.warning is None


# ---------------------------------------------------------------------------
# _raise_for_status edge cases
# ---------------------------------------------------------------------------

class TestRaiseForStatus:
    """Verify every HTTP error code maps to the right exception."""

    def test_200_ok_does_not_raise(self, client):
        resp = make_response(200, json_data=_FORMATS_JSON)
        with patch.object(client._client, "get", return_value=resp):
            client.formats()  # should not raise

    def test_401_raises_authentication_error(self, client):
        resp = make_response(401, json_data={"detail": "bad key"})
        with patch.object(client._client, "get", return_value=resp):
            with pytest.raises(AuthenticationError):
                client.formats()

    def test_403_raises_authentication_error(self, client):
        resp = make_response(403, json_data={"detail": "forbidden"})
        with patch.object(client._client, "get", return_value=resp):
            with pytest.raises(AuthenticationError):
                client.formats()

    def test_402_raises_insufficient_plan(self, client):
        resp = make_response(402, json_data={"detail": "need plan"})
        with patch.object(client._client, "get", return_value=resp):
            with pytest.raises(InsufficientPlanError):
                client.formats()

    def test_413_raises_file_too_large(self, client):
        resp = make_response(413, json_data={"detail": "too big"})
        with patch.object(client._client, "get", return_value=resp):
            with pytest.raises(FileTooLargeError):
                client.formats()

    def test_422_raises_parse_error(self, client):
        resp = make_response(422, json_data={"detail": "bad format"})
        with patch.object(client._client, "get", return_value=resp):
            with pytest.raises(ParseError):
                client.formats()

    def test_503_raises_api_error_with_code(self, client):
        resp = make_response(503, json_data={"detail": "down"})
        with patch.object(client._client, "get", return_value=resp):
            with pytest.raises(APIError) as exc_info:
                client.formats()
        assert exc_info.value.status_code == 503

    def test_non_json_error_body_still_raises(self, client):
        resp = make_response(500, content=b"Internal Server Error")
        with patch.object(client._client, "get", return_value=resp):
            with pytest.raises(APIError):
                client.formats()
