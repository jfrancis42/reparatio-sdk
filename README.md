# reparatio — Python SDK

> **Alpha software.** The API surface may change without notice between versions. Pin to a specific version in production.

Python client library for the [Reparatio](https://reparatio.app) data conversion API.

Inspect, convert, merge, append, and query CSV, Excel, Parquet, JSON, GeoJSON, SQLite, and 30+ other formats with a single function call.

**See also:** [reparatio-cli](https://github.com/jfrancis42/reparatio-cli) (command-line tool) · [reparatio-mcp](https://github.com/jfrancis42/reparatio-mcp) (MCP server for AI assistants)

---

## Installation

```bash
pip install reparatio
```

Requires Python 3.9 or later.

---

## Quick start

```python
from reparatio import Reparatio

client = Reparatio(api_key="rp_YOUR_KEY")

# Inspect a file
result = client.inspect("sales.csv")
print(f"{result.rows} rows, {len(result.columns)} columns")
for col in result.columns:
    print(f"  {col.name}: {col.dtype}")

# Convert to Parquet
out = client.convert("sales.csv", "parquet")
with open(out.filename, "wb") as f:
    f.write(out.content)

# SQL query
out = client.query("events.parquet", "SELECT region, SUM(revenue) FROM data GROUP BY region ORDER BY 2 DESC")
with open("by_region.csv", "wb") as f:
    f.write(out.content)
```

---

## Authentication

The API key can be supplied in three ways, in order of precedence:

1. Passed directly: `Reparatio(api_key="rp_...")`
2. Environment variable: `REPARATIO_API_KEY=rp_...`
3. Omitted entirely (only `inspect` and `formats` work without a key)

Get a key at [reparatio.app](https://reparatio.app) (Professional plan — $79/mo). API access requires the Professional plan; the Standard plan ($29/mo) covers web UI only.

---

## Context manager

The client holds an HTTP connection pool. Use it as a context manager to ensure it is closed:

```python
with Reparatio(api_key="rp_...") as client:
    result = client.inspect("data.csv")
```

Or call `client.close()` explicitly when done.

---

## Reference

### `Reparatio(api_key, base_url, timeout)`

| Parameter | Default | Description |
|---|---|---|
| `api_key` | `$REPARATIO_API_KEY` | Your `rp_...` API key |
| `base_url` | `https://reparatio.app` | Override the API host |
| `timeout` | `120.0` | Request timeout in seconds |

---

### `client.formats() → FormatsResult`

Return the list of supported input and output formats. No API key required.

```python
f = client.formats()
print(f.input)   # ["csv", "tsv", "xlsx", ...]
print(f.output)  # ["csv", "parquet", "json.gz", ...]
```

---

### `client.me() → MeResult`

Return subscription details for the current API key.

```python
me = client.me()
print(me.email, me.plan, me.active)
```

**`MeResult` fields:** `email`, `plan`, `active`, `api_access`, `expires_at`

---

### `client.inspect(file, ...) → InspectResult`

Detect encoding, count rows, list column types and statistics, and return a data preview.
No API key required.

```python
result = client.inspect(
    "data.csv",
    preview_rows=20,
    fix_encoding=True,
)
print(result.rows, result.detected_encoding)
for col in result.columns:
    print(col.name, col.dtype, col.null_count, col.unique_count)
for row in result.preview:
    print(row)
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `file` | str / Path / bytes | required | File path or raw bytes |
| `filename` | str | `"file"` | Original filename (required when passing bytes) |
| `no_header` | bool | `False` | Treat first row as data |
| `fix_encoding` | bool | `True` | Auto-detect and repair encoding |
| `preview_rows` | int | `8` | Number of preview rows (1–100) |
| `delimiter` | str | `""` | Custom delimiter (auto-detected if blank) |
| `sheet` | str | `""` | Sheet name for Excel, ODS, or SQLite |

**`InspectResult` fields:** `filename`, `detected_encoding`, `rows`, `columns` (list of `ColumnInfo`), `preview`, `sheets`

**`ColumnInfo` fields:** `name`, `dtype`, `null_count`, `unique_count`

---

### `client.convert(file, target_format, ...) → ConvertResult`

Convert a file from any supported input format to any supported output format.
Requires a Professional plan key ($79/mo).

```python
# Basic conversion
out = client.convert("sales.csv", "parquet")
Path(out.filename).write_bytes(out.content)

# Select and rename columns, compress output
out = client.convert(
    "big.csv",
    "csv.gz",
    select_columns=["date", "region", "revenue"],
    columns=["Date", "Region", "Revenue"],
)

# Deduplicate and take a 10% sample
out = client.convert("events.csv", "xlsx", deduplicate=True, sample_frac=0.1)
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `file` | str / Path / bytes | required | File path or raw bytes |
| `target_format` | str | required | Output format (see [formats](#supported-formats)) |
| `filename` | str | `"file"` | Original filename (required when passing bytes) |
| `no_header` | bool | `False` | Treat first row as data |
| `fix_encoding` | bool | `True` | Repair encoding |
| `delimiter` | str | `""` | Custom delimiter for CSV-like input |
| `sheet` | str | `""` | Sheet or table to read |
| `columns` | list[str] | `[]` | Rename all columns (new names in order) |
| `select_columns` | list[str] | `[]` | Columns to include (all if empty) |
| `deduplicate` | bool | `False` | Remove duplicate rows |
| `sample_n` | int | `0` | Random sample of N rows |
| `sample_frac` | float | `0.0` | Random sample fraction (e.g. `0.1` for 10%) |
| `geometry_column` | str | `"geometry"` | WKT geometry column for GeoJSON output |
| `cast_columns` | dict | `{}` | Override inferred column types (see below) |
| `null_values` | list[str] | `[]` | Strings to treat as null at load time, e.g. `["N/A", "NULL", "-"]` |
| `encoding_override` | str | `None` | Force a specific encoding, bypassing auto-detection. Any Python codec name: `"cp037"` (EBCDIC US), `"cp500"` (EBCDIC International), `"cp1026"` (EBCDIC Turkish), `"cp1140"` (EBCDIC US+Euro), `"latin-1"`, etc. |

**`cast_columns` format:**

```python
# Cast price to Float64 and parse date from "31/12/2024" format
out = client.convert(
    "sales.csv",
    "parquet",
    cast_columns={
        "price": {"type": "Float64"},
        "date":  {"type": "Date", "format": "%d/%m/%Y"},
    },
)
```

Supported types: `String`, `Int8`–`Int64`, `UInt8`–`UInt64`, `Float32`, `Float64`,
`Boolean`, `Date`, `Datetime`, `Time`. Values that cannot be cast are silently set to null.

**EBCDIC and encoding override:**

```python
# Convert an EBCDIC mainframe file (cp037 = EBCDIC US)
out = client.convert("mainframe.dat", "csv", encoding_override="cp037")
Path(out.filename).write_bytes(out.content)

# EBCDIC International (cp500)
out = client.convert("ibm_export.dat", "parquet", encoding_override="cp500")
Path(out.filename).write_bytes(out.content)
```

When `encoding_override` is set, chardet auto-detection is skipped entirely and the
specified codec is used directly. Omit the parameter (or pass `None`) to use the
default auto-detection behaviour, which also includes an EBCDIC heuristic.

---

### `client.batch_convert(zip_file, target_format, ...) → ConvertResult`

Convert every file inside a ZIP archive to a common format.
Returns a ZIP archive in `result.content`. Files that fail to parse are skipped;
their names and errors are available as a JSON string in `result.warning`.
Requires a Professional plan key ($79/mo).

```python
out = client.batch_convert("monthly_reports.zip", "parquet")
Path("converted.zip").write_bytes(out.content)
if out.warning:
    import json
    for err in json.loads(out.warning):
        print(f"Skipped {err['file']}: {err['error']}")
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `zip_file` | str / Path / bytes | required | ZIP archive path or raw bytes |
| `target_format` | str | required | Output format for every file |
| `filename` | str | `"batch.zip"` | Original filename (when passing bytes) |
| `no_header` | bool | `False` | Treat first row as data |
| `fix_encoding` | bool | `True` | Repair encoding |
| `delimiter` | str | `""` | Custom delimiter |
| `select_columns` | list[str] | `[]` | Columns to include (all if empty) |
| `deduplicate` | bool | `False` | Remove duplicate rows from each file |
| `sample_n` | int | `0` | Random sample of N rows per file |
| `sample_frac` | float | `0.0` | Random sample fraction per file |
| `cast_columns` | dict | `{}` | Column type overrides for every file |

---

### `client.merge(file1, file2, operation, target_format, ...) → ConvertResult`

Merge or join two files.
Requires a Professional plan key ($79/mo).

```python
out = client.merge(
    "orders.csv",
    "customers.xlsx",
    operation="left",
    target_format="parquet",
    join_on="customer_id",
)
Path(out.filename).write_bytes(out.content)
if out.warning:
    print("Warning:", out.warning)
```

**Operations:**

| Value | Behaviour |
|---|---|
| `append` | Stack all rows from both files; missing columns filled with null |
| `left` | All rows from file 1; matching columns from file 2 |
| `right` | All rows from file 2; matching columns from file 1 |
| `outer` | All rows from both files; nulls where no match |
| `inner` | Only rows present in both files |

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `file1` | str / Path / bytes | required | First file |
| `file2` | str / Path / bytes | required | Second file |
| `operation` | str | required | Join type (see table above) |
| `target_format` | str | required | Output format |
| `filename1` | str | `"file1"` | Original name of file1 (when passing bytes) |
| `filename2` | str | `"file2"` | Original name of file2 (when passing bytes) |
| `join_on` | str | `""` | Comma-separated column(s) to join on |
| `no_header` | bool | `False` | Treat first row as data |
| `fix_encoding` | bool | `True` | Repair encoding |
| `geometry_column` | str | `"geometry"` | WKT geometry column for GeoJSON output |

---

### `client.append(files, target_format, ...) → ConvertResult`

Stack rows from two or more files vertically.
Column mismatches are handled gracefully — missing values are filled with null.
Requires a Professional plan key ($79/mo).

```python
import glob

paths = sorted(glob.glob("monthly/*.csv"))
out = client.append(paths, "parquet")
Path("all_months.parquet").write_bytes(out.content)
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `files` | list[str / Path / bytes] | required | File paths or bytes (minimum 2) |
| `target_format` | str | required | Output format |
| `filenames` | list[str] | auto | Original filenames (when passing bytes) |
| `no_header` | bool | `False` | Treat first row as data |
| `fix_encoding` | bool | `True` | Repair encoding |

---

### `client.query(file, sql, ...) → ConvertResult`

Run a SQL query against a file.
The file is loaded as a table named `data`.
Requires a Professional plan key ($79/mo).

```python
out = client.query(
    "events.parquet",
    "SELECT region, SUM(revenue) AS total FROM data WHERE year = 2025 GROUP BY region ORDER BY total DESC",
    target_format="json",
)
print(out.content.decode())
```

Supports `SELECT`, `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`, aggregations, and most scalar functions.

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `file` | str / Path / bytes | required | File path or raw bytes |
| `sql` | str | required | SQL query (table name: `data`) |
| `filename` | str | `"file"` | Original filename (required when passing bytes) |
| `target_format` | str | `"csv"` | Output format |
| `no_header` | bool | `False` | Treat first row as data |
| `fix_encoding` | bool | `True` | Repair encoding |
| `delimiter` | str | `""` | Custom delimiter for CSV-like input |
| `sheet` | str | `""` | Sheet or table to read |

---

### `ConvertResult`

Returned by `convert()`, `merge()`, `append()`, and `query()`.

| Field | Type | Description |
|---|---|---|
| `content` | bytes | Raw file content |
| `filename` | str | Suggested output filename |
| `warning` | str or None | Server warning (e.g. column mismatch) |

Write the file to disk:

```python
out = client.convert("data.csv", "parquet")
Path(out.filename).write_bytes(out.content)
```

---

## Supported formats

### Input

CSV, TSV, CSV.GZ, CSV.BZ2, CSV.ZST, CSV.ZIP, TSV.GZ, TSV.BZ2, TSV.ZST, TSV.ZIP, GZ (any supported format), ZIP (any supported format), BZ2 (any supported format), ZST (any supported format), Excel (.xlsx / .xls), ODS, JSON, JSON.GZ, JSON.BZ2, JSON.ZST, JSON.ZIP, JSON Lines, GeoJSON, Parquet, Feather, Arrow, ORC, Avro, SQLite, YAML, BSON, SRT, VTT, HTML, Markdown, XML, SQL dump, PDF (text layer)

### Output

CSV, TSV, CSV.GZ, CSV.BZ2, CSV.ZST, CSV.ZIP, TSV.GZ, TSV.BZ2, TSV.ZST, TSV.ZIP, Excel (.xlsx), ODS, JSON, JSON.GZ, JSON.BZ2, JSON.ZST, JSON.ZIP, JSON Lines, JSON Lines.GZ, JSON Lines.BZ2, JSON Lines.ZST, JSON Lines.ZIP, GeoJSON, GeoJSON.GZ, GeoJSON.BZ2, GeoJSON.ZST, GeoJSON.ZIP, Parquet, Feather, Arrow, ORC, Avro, SQLite, YAML, BSON, SRT, VTT

---

## Error handling

All errors are subclasses of `reparatio.ReparatioError`:

| Exception | Cause |
|---|---|
| `AuthenticationError` | Missing, invalid, or expired API key |
| `InsufficientPlanError` | Operation requires a Professional plan |
| `FileTooLargeError` | File exceeds the server's size limit |
| `ParseError` | File could not be parsed in the detected format |
| `APIError` | Unexpected server error (has `.status_code` and `.detail`) |

```python
from reparatio import Reparatio, AuthenticationError, ParseError

try:
    out = client.convert("bad.csv", "parquet")
except AuthenticationError:
    print("Check your API key")
except ParseError as e:
    print(f"Could not read file: {e}")
```

---

## Privacy

Files are sent to the Reparatio API at `reparatio.app` for processing.
Files are handled in memory and never stored — see the [Privacy Policy](https://reparatio.app).
