"""Tests for reparatio.exceptions."""
import pytest

from reparatio.exceptions import (
    APIError,
    AuthenticationError,
    FileTooLargeError,
    InsufficientPlanError,
    ParseError,
    ReparatioError,
)


class TestExceptionHierarchy:
    def test_authentication_error_is_reparatio_error(self):
        assert issubclass(AuthenticationError, ReparatioError)

    def test_insufficient_plan_error_is_reparatio_error(self):
        assert issubclass(InsufficientPlanError, ReparatioError)

    def test_file_too_large_error_is_reparatio_error(self):
        assert issubclass(FileTooLargeError, ReparatioError)

    def test_parse_error_is_reparatio_error(self):
        assert issubclass(ParseError, ReparatioError)

    def test_api_error_is_reparatio_error(self):
        assert issubclass(APIError, ReparatioError)

    def test_all_are_exceptions(self):
        for cls in (ReparatioError, AuthenticationError, InsufficientPlanError,
                    FileTooLargeError, ParseError, APIError):
            assert issubclass(cls, Exception)


class TestAPIError:
    def test_attributes(self):
        err = APIError(500, "Internal server error")
        assert err.status_code == 500
        assert err.detail == "Internal server error"

    def test_str_includes_status_and_detail(self):
        err = APIError(429, "Too many requests")
        assert "429" in str(err)
        assert "Too many requests" in str(err)

    def test_can_be_raised_and_caught(self):
        with pytest.raises(APIError) as exc_info:
            raise APIError(503, "Service unavailable")
        assert exc_info.value.status_code == 503

    def test_can_be_caught_as_reparatio_error(self):
        with pytest.raises(ReparatioError):
            raise APIError(500, "oops")
