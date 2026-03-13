"""Response models for the Reparatio API."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ColumnInfo:
    name: str
    dtype: str
    null_count: int
    unique_count: int

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ColumnInfo":
        return cls(
            name=d["name"],
            dtype=d["dtype"],
            null_count=d["null_count"],
            unique_count=d["unique_count"],
        )


@dataclass
class InspectResult:
    filename: str
    detected_encoding: str
    rows: int
    columns: List[ColumnInfo]
    preview: List[Dict[str, Any]]
    sheets: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "InspectResult":
        return cls(
            filename=d["filename"],
            detected_encoding=d["detected_encoding"],
            rows=d["rows"],
            columns=[ColumnInfo.from_dict(c) for c in d.get("columns", [])],
            preview=d.get("preview", []),
            sheets=d.get("sheets", []),
        )


@dataclass
class FormatsResult:
    input: List[str]
    output: List[str]

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "FormatsResult":
        return cls(input=d["input"], output=d["output"])


@dataclass
class MeResult:
    email: str
    plan: str
    active: bool
    api_access: bool
    expires_at: Optional[str]

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "MeResult":
        return cls(
            email=d["email"],
            plan=d["plan"],
            active=d["active"],
            api_access=d["api_access"],
            expires_at=d.get("expires_at"),
        )


@dataclass
class ConvertResult:
    """Returned by convert(), merge(), append(), and query(). Contains the raw bytes."""
    content: bytes
    filename: str
    warning: Optional[str] = None
