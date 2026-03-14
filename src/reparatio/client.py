"""Reparatio API client."""
from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional, Union

import httpx

from .exceptions import (
    APIError,
    AuthenticationError,
    FileTooLargeError,
    InsufficientPlanError,
    ParseError,
)
from .models import ConvertResult, FormatsResult, InspectResult, MeResult

_DEFAULT_BASE_URL = "https://reparatio.app"


def _raise_for_status(response: httpx.Response) -> None:
    if response.status_code < 400:
        return
    try:
        detail = response.json().get("detail", response.text)
    except Exception:
        detail = response.text

    if response.status_code in (401, 403):
        raise AuthenticationError(detail)
    if response.status_code == 402:
        raise InsufficientPlanError(detail)
    if response.status_code == 413:
        raise FileTooLargeError(detail)
    if response.status_code == 422:
        raise ParseError(detail)
    raise APIError(response.status_code, detail)


def _filename_from_response(response: httpx.Response, fallback: str) -> str:
    cd = response.headers.get("content-disposition", "")
    if 'filename="' in cd:
        return cd.split('filename="', 1)[1].rstrip('"')
    return fallback


class Reparatio:
    """Synchronous Reparatio API client.

    Parameters
    ----------
    api_key:
        Your ``rp_...`` API key.  If omitted, the ``REPARATIO_API_KEY``
        environment variable is used.
    base_url:
        Override the API base URL (useful for self-hosted instances or testing).
    timeout:
        HTTP timeout in seconds (default 120).
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: str = _DEFAULT_BASE_URL,
        timeout: float = 120.0,
    ) -> None:
        key = api_key or os.environ.get("REPARATIO_API_KEY", "")
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"),
            headers={"X-API-Key": key} if key else {},
            timeout=timeout,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "Reparatio":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def formats(self) -> FormatsResult:
        """Return the list of supported input and output formats.

        No API key required.
        """
        r = self._client.get("/api/v1/formats")
        _raise_for_status(r)
        return FormatsResult.from_dict(r.json())

    def me(self) -> MeResult:
        """Return subscription status for the current API key."""
        r = self._client.get("/api/v1/me")
        _raise_for_status(r)
        return MeResult.from_dict(r.json())

    def inspect(
        self,
        file: Union[str, Path, bytes],
        *,
        filename: str = "file",
        no_header: bool = False,
        fix_encoding: bool = True,
        preview_rows: int = 8,
        delimiter: str = "",
        sheet: str = "",
    ) -> InspectResult:
        """Inspect a file: return schema, encoding, stats, and a data preview.

        No API key required.

        Parameters
        ----------
        file:
            File path (str or Path) or raw bytes.
        filename:
            Original filename — used by the server to detect the format.
            Required when *file* is raw bytes.
        """
        content, fname = _load_file(file, filename)
        r = self._client.post(
            "/api/v1/inspect",
            files={"file": (fname, content)},
            data={
                "no_header": str(no_header).lower(),
                "fix_encoding": str(fix_encoding).lower(),
                "preview_rows": str(preview_rows),
                "delimiter": delimiter,
                "sheet": sheet,
            },
        )
        _raise_for_status(r)
        return InspectResult.from_dict(r.json())

    def convert(
        self,
        file: Union[str, Path, bytes],
        target_format: str,
        *,
        filename: str = "file",
        no_header: bool = False,
        fix_encoding: bool = True,
        delimiter: str = "",
        sheet: str = "",
        columns: Optional[List[str]] = None,
        select_columns: Optional[List[str]] = None,
        deduplicate: bool = False,
        sample_n: int = 0,
        sample_frac: float = 0.0,
        geometry_column: str = "geometry",
        cast_columns: Optional[dict] = None,
        null_values: Optional[List[str]] = None,
        encoding_override: Optional[str] = None,
    ) -> ConvertResult:
        """Convert a file to a different format.

        Requires a Professional plan API key ($79/mo). Standard plan ($29/mo)
        covers web UI conversions only; API access requires Professional.

        Returns a :class:`ConvertResult` whose ``.content`` attribute holds
        the raw bytes of the converted file.

        Parameters
        ----------
        file:
            File path (str or Path) or raw bytes.
        target_format:
            Output format, e.g. ``"parquet"``, ``"csv.gz"``, ``"xlsx"``.
        columns:
            Rename all columns (list of new names in order).
        select_columns:
            Whitelist of column names to include in output.
        cast_columns:
            Override inferred column types.  Dict mapping column name to a
            spec dict with ``"type"`` (required) and optional ``"format"``
            for date/datetime parsing.  Example::

                {"price": {"type": "Float64"},
                 "date":  {"type": "Date", "format": "%d/%m/%Y"}}

            Supported types: ``String``, ``Int8``–``Int64``, ``UInt8``–``UInt64``,
            ``Float32``, ``Float64``, ``Boolean``, ``Date``, ``Datetime``, ``Time``.
        null_values:
            List of strings to treat as null at load time, passed as Polars
            ``null_values=``.  Example: ``["N/A", "NULL", "-"]``.
        encoding_override:
            Force a specific encoding instead of auto-detecting.  Pass any
            Python codec name, e.g. ``"cp037"`` (EBCDIC US), ``"cp500"``
            (EBCDIC International), ``"cp1026"`` (EBCDIC Turkish),
            ``"cp1140"`` (EBCDIC US with Euro), ``"latin-1"``, etc.
            When absent or ``None``, auto-detection runs as normal.
        """
        import json as _json

        content, fname = _load_file(file, filename)
        form_data: dict = {
            "target_format": target_format,
            "no_header": str(no_header).lower(),
            "fix_encoding": str(fix_encoding).lower(),
            "delimiter": delimiter,
            "sheet": sheet,
            "columns": _json.dumps(columns or []),
            "select_columns": _json.dumps(select_columns or []),
            "deduplicate": str(deduplicate).lower(),
            "sample_n": str(sample_n),
            "sample_frac": str(sample_frac),
            "geometry_column": geometry_column,
            "cast_columns": _json.dumps(cast_columns or {}),
            "null_values": _json.dumps(null_values or []),
        }
        if encoding_override:
            form_data["encoding_override"] = encoding_override
        r = self._client.post(
            "/api/v1/convert",
            files={"file": (fname, content)},
            data=form_data,
        )
        _raise_for_status(r)
        out_name = _filename_from_response(r, fname.rsplit(".", 1)[0] + "." + target_format)
        warning = r.headers.get("x-reparatio-warning")
        return ConvertResult(content=r.content, filename=out_name, warning=warning)

    def batch_convert(
        self,
        zip_file: Union[str, Path, bytes],
        target_format: str,
        *,
        filename: str = "batch.zip",
        no_header: bool = False,
        fix_encoding: bool = True,
        delimiter: str = "",
        select_columns: Optional[List[str]] = None,
        deduplicate: bool = False,
        sample_n: int = 0,
        sample_frac: float = 0.0,
        cast_columns: Optional[dict] = None,
    ) -> ConvertResult:
        """Convert every file inside a ZIP and return a ZIP of converted files.

        Files that cannot be parsed are skipped; their names and error
        messages are available in ``result.warning`` (JSON string from the
        ``X-Reparatio-Errors`` response header).

        Parameters
        ----------
        zip_file:
            Path to a ``.zip`` archive, or raw bytes of the ZIP.
        target_format:
            Output format for every file inside the ZIP, e.g. ``"parquet"``.
        """
        import json as _json
        import urllib.parse as _up

        content, fname = _load_file(zip_file, filename)
        r = self._client.post(
            "/api/v1/batch-convert",
            files={"zip_file": (fname, content)},
            data={
                "target_format": target_format,
                "no_header": str(no_header).lower(),
                "fix_encoding": str(fix_encoding).lower(),
                "delimiter": delimiter,
                "select_columns": _json.dumps(select_columns or []),
                "deduplicate": str(deduplicate).lower(),
                "sample_n": str(sample_n),
                "sample_frac": str(sample_frac),
                "cast_columns": _json.dumps(cast_columns or {}),
            },
        )
        _raise_for_status(r)
        out_name = _filename_from_response(r, "converted.zip")
        raw_errors = r.headers.get("x-reparatio-errors")
        warning = _up.unquote(raw_errors) if raw_errors else None
        return ConvertResult(content=r.content, filename=out_name, warning=warning)

    def merge(
        self,
        file1: Union[str, Path, bytes],
        file2: Union[str, Path, bytes],
        operation: str,
        target_format: str,
        *,
        filename1: str = "file1",
        filename2: str = "file2",
        join_on: str = "",
        no_header: bool = False,
        fix_encoding: bool = True,
        geometry_column: str = "geometry",
    ) -> ConvertResult:
        """Merge or join two files.

        Parameters
        ----------
        operation:
            One of ``"append"``, ``"left"``, ``"right"``, ``"outer"``,
            ``"inner"``.
        join_on:
            Comma-separated column name(s) to join on.  Not needed for
            ``"append"``.
        """
        content1, fname1 = _load_file(file1, filename1)
        content2, fname2 = _load_file(file2, filename2)
        r = self._client.post(
            "/api/v1/merge",
            files={
                "file1": (fname1, content1),
                "file2": (fname2, content2),
            },
            data={
                "operation": operation,
                "target_format": target_format,
                "join_on": join_on,
                "no_header": str(no_header).lower(),
                "fix_encoding": str(fix_encoding).lower(),
                "geometry_column": geometry_column,
            },
        )
        _raise_for_status(r)
        base1 = fname1.rsplit(".", 1)[0]
        base2 = fname2.rsplit(".", 1)[0]
        out_name = _filename_from_response(
            r, f"{base1}_{operation}_{base2}.{target_format}"
        )
        warning = r.headers.get("x-reparatio-warning")
        return ConvertResult(content=r.content, filename=out_name, warning=warning)

    def append(
        self,
        files: List[Union[str, Path, bytes]],
        target_format: str,
        *,
        filenames: Optional[List[str]] = None,
        no_header: bool = False,
        fix_encoding: bool = True,
    ) -> ConvertResult:
        """Stack rows from two or more files vertically.

        Column mismatches are handled gracefully — missing values are filled
        with null.

        Parameters
        ----------
        files:
            List of file paths or raw bytes (minimum 2).
        filenames:
            Original filenames, used for format detection when *files*
            contains raw bytes.
        """
        if len(files) < 2:
            raise ValueError("At least 2 files are required")

        fnames = filenames or [f"file{i}" for i in range(len(files))]
        loaded = [_load_file(f, n) for f, n in zip(files, fnames)]

        multipart = [("files", (name, content)) for content, name in loaded]
        r = self._client.post(
            "/api/v1/append",
            files=multipart,
            data={
                "target_format": target_format,
                "no_header": str(no_header).lower(),
                "fix_encoding": str(fix_encoding).lower(),
            },
        )
        _raise_for_status(r)
        out_name = _filename_from_response(r, f"appended.{target_format}")
        warning = r.headers.get("x-reparatio-warning")
        return ConvertResult(content=r.content, filename=out_name, warning=warning)

    def query(
        self,
        file: Union[str, Path, bytes],
        sql: str,
        *,
        filename: str = "file",
        target_format: str = "csv",
        no_header: bool = False,
        fix_encoding: bool = True,
        delimiter: str = "",
        sheet: str = "",
    ) -> ConvertResult:
        """Run a SQL query against a file.

        The file is loaded as a table named ``data``.

        Parameters
        ----------
        sql:
            SQL query, e.g. ``"SELECT region, SUM(revenue) FROM data GROUP BY region"``.
        target_format:
            Format for the query result (default ``"csv"``).
        """
        content, fname = _load_file(file, filename)
        r = self._client.post(
            "/api/v1/query",
            files={"file": (fname, content)},
            data={
                "sql": sql,
                "target_format": target_format,
                "no_header": str(no_header).lower(),
                "fix_encoding": str(fix_encoding).lower(),
                "delimiter": delimiter,
                "sheet": sheet,
            },
        )
        _raise_for_status(r)
        base = fname.rsplit(".", 1)[0]
        out_name = _filename_from_response(r, f"{base}_query.{target_format}")
        return ConvertResult(content=r.content, filename=out_name)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_file(
    file: Union[str, Path, bytes], filename: str
) -> tuple[bytes, str]:
    if isinstance(file, bytes):
        return file, filename
    path = Path(file)
    return path.read_bytes(), path.name
