"""Tests for reparatio.models — dataclass parsing."""
import pytest

from reparatio.models import ColumnInfo, ConvertResult, FormatsResult, InspectResult, MeResult


# ---------------------------------------------------------------------------
# ColumnInfo
# ---------------------------------------------------------------------------

class TestColumnInfo:
    def test_from_dict_basic(self):
        d = {"name": "revenue", "dtype": "Float64", "null_count": 2, "unique_count": 50}
        col = ColumnInfo.from_dict(d)
        assert col.name == "revenue"
        assert col.dtype == "Float64"
        assert col.null_count == 2
        assert col.unique_count == 50

    def test_from_dict_zero_counts(self):
        d = {"name": "id", "dtype": "Int64", "null_count": 0, "unique_count": 0}
        col = ColumnInfo.from_dict(d)
        assert col.null_count == 0
        assert col.unique_count == 0


# ---------------------------------------------------------------------------
# InspectResult
# ---------------------------------------------------------------------------

class TestInspectResult:
    _base = {
        "filename": "sales.csv",
        "detected_encoding": "utf-8",
        "rows": 1000,
        "columns": [
            {"name": "date", "dtype": "Utf8", "null_count": 0, "unique_count": 365},
            {"name": "amount", "dtype": "Float64", "null_count": 3, "unique_count": 980},
        ],
        "preview": [{"date": "2024-01-01", "amount": "12.5"}],
    }

    def test_from_dict_full(self):
        result = InspectResult.from_dict(self._base)
        assert result.filename == "sales.csv"
        assert result.detected_encoding == "utf-8"
        assert result.rows == 1000
        assert len(result.columns) == 2
        assert result.columns[0].name == "date"
        assert result.columns[1].dtype == "Float64"
        assert result.preview == [{"date": "2024-01-01", "amount": "12.5"}]

    def test_from_dict_sheets_present(self):
        d = {**self._base, "sheets": ["Sheet1", "Sheet2"]}
        result = InspectResult.from_dict(d)
        assert result.sheets == ["Sheet1", "Sheet2"]

    def test_from_dict_sheets_absent(self):
        result = InspectResult.from_dict(self._base)
        assert result.sheets == []

    def test_from_dict_no_columns(self):
        d = {**self._base, "columns": []}
        result = InspectResult.from_dict(d)
        assert result.columns == []

    def test_from_dict_no_preview(self):
        d = {k: v for k, v in self._base.items() if k != "preview"}
        result = InspectResult.from_dict(d)
        assert result.preview == []


# ---------------------------------------------------------------------------
# FormatsResult
# ---------------------------------------------------------------------------

class TestFormatsResult:
    def test_from_dict(self):
        d = {"input": ["csv", "parquet", "xlsx"], "output": ["csv", "parquet"]}
        result = FormatsResult.from_dict(d)
        assert "csv" in result.input
        assert "parquet" in result.output
        assert len(result.input) == 3

    def test_from_dict_empty_lists(self):
        result = FormatsResult.from_dict({"input": [], "output": []})
        assert result.input == []
        assert result.output == []


# ---------------------------------------------------------------------------
# MeResult
# ---------------------------------------------------------------------------

class TestMeResult:
    def test_from_dict_with_expiry(self):
        d = {
            "email": "user@example.com",
            "plan": "pro",
            "active": True,
            "api_access": True,
            "expires_at": "2026-12-31T00:00:00Z",
        }
        result = MeResult.from_dict(d)
        assert result.email == "user@example.com"
        assert result.plan == "pro"
        assert result.active is True
        assert result.api_access is True
        assert result.expires_at == "2026-12-31T00:00:00Z"

    def test_from_dict_no_expiry(self):
        d = {
            "email": "user@example.com",
            "plan": "credits_25",
            "active": True,
            "api_access": True,
        }
        result = MeResult.from_dict(d)
        assert result.expires_at is None

    def test_from_dict_inactive(self):
        d = {"email": "x@y.com", "plan": "free", "active": False, "api_access": False}
        result = MeResult.from_dict(d)
        assert result.active is False
        assert result.api_access is False


# ---------------------------------------------------------------------------
# ConvertResult
# ---------------------------------------------------------------------------

class TestConvertResult:
    def test_basic(self):
        r = ConvertResult(content=b"data", filename="out.csv")
        assert r.content == b"data"
        assert r.filename == "out.csv"
        assert r.warning is None

    def test_with_warning(self):
        r = ConvertResult(content=b"data", filename="out.csv", warning="3 columns missing")
        assert r.warning == "3 columns missing"
