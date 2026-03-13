"""Reparatio SDK exceptions."""


class ReparatioError(Exception):
    """Base exception for all Reparatio SDK errors."""


class AuthenticationError(ReparatioError):
    """API key is missing, invalid, or does not have sufficient access."""


class InsufficientPlanError(ReparatioError):
    """Operation requires a Monthly plan."""


class FileTooLargeError(ReparatioError):
    """File exceeds the server's size limit."""


class ParseError(ReparatioError):
    """The file could not be parsed in the detected format."""


class APIError(ReparatioError):
    """Unexpected error response from the API."""

    def __init__(self, status_code: int, detail: str) -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"HTTP {status_code}: {detail}")
