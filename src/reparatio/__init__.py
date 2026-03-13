"""Reparatio Python SDK."""

from .client import Reparatio
from .exceptions import (
    APIError,
    AuthenticationError,
    FileTooLargeError,
    InsufficientPlanError,
    ParseError,
    ReparatioError,
)
from .models import (
    ColumnInfo,
    ConvertResult,
    FormatsResult,
    InspectResult,
    MeResult,
)

__all__ = [
    "Reparatio",
    "ReparatioError",
    "APIError",
    "AuthenticationError",
    "FileTooLargeError",
    "InsufficientPlanError",
    "ParseError",
    "ColumnInfo",
    "ConvertResult",
    "FormatsResult",
    "InspectResult",
    "MeResult",
]

__version__ = "0.1.0"
