"""
Tests for superset_cli.lib, superset_cli.exceptions, and superset_cli.api.operators.

Covers:
  - remove_root
  - setup_logging
  - deserialize_error_level
  - is_sip_40_payload
  - validate_response
  - split_comma
  - dict_merge
  - raise_cli_errors
  - ErrorLevel, SupersetError, DatabaseNotFoundError, CLIError
  - Operator, Equal, OneToMany
"""

import json
import logging
from unittest.mock import MagicMock

import click
import pytest

from superset_cli.exceptions import (
    CLIError,
    DatabaseNotFoundError,
    ErrorLevel,
    SupersetError,
)
from superset_cli.lib import (
    deserialize_error_level,
    dict_merge,
    is_sip_40_payload,
    raise_cli_errors,
    remove_root,
    setup_logging,
    split_comma,
    validate_response,
)
from superset_cli.api.operators import Equal, OneToMany, Operator


# ─── remove_root ──────────────────────────────────────────────────────────
class TestRemoveRoot:
    def test_simple_path(self):
        result = remove_root("root/sub/file.txt")
        # Path uses OS-native separators
        assert result.replace("\\", "/") == "sub/file.txt"

    def test_deep_path(self):
        result = remove_root("a/b/c/d/e.txt")
        assert result.replace("\\", "/") == "b/c/d/e.txt"

    def test_two_parts(self):
        assert remove_root("root/file.txt") == "file.txt"


# ─── setup_logging ────────────────────────────────────────────────────────
class TestSetupLogging:
    def test_valid_level(self):
        setup_logging("DEBUG")
        assert logging.getLogger().level == logging.DEBUG

    def test_case_insensitive(self):
        setup_logging("info")
        assert logging.getLogger().level == logging.INFO

    def test_invalid_level_raises(self):
        with pytest.raises(ValueError, match="Invalid log level"):
            setup_logging("INVALID")


# ─── deserialize_error_level ──────────────────────────────────────────────
class TestDeserializeErrorLevel:
    def test_converts_string_to_enum(self):
        errors = [{"level": "error", "message": "test"}]
        result = deserialize_error_level(errors)
        assert result[0]["level"] == ErrorLevel.ERROR

    def test_handles_already_enum(self):
        errors = [{"level": ErrorLevel.WARNING, "message": "test"}]
        result = deserialize_error_level(errors)
        assert result[0]["level"] == ErrorLevel.WARNING

    def test_handles_missing_level(self):
        errors = [{"message": "no level"}]
        result = deserialize_error_level(errors)
        assert "level" not in result[0]


# ─── is_sip_40_payload ───────────────────────────────────────────────────
class TestIsSip40Payload:
    def test_valid_payload(self):
        errors = [
            {"message": "err", "error_type": "UNKNOWN", "level": "error"},
        ]
        assert is_sip_40_payload(errors) is True

    def test_valid_with_extra(self):
        errors = [
            {
                "message": "err",
                "error_type": "UNKNOWN",
                "level": "error",
                "extra": {"foo": "bar"},
            },
        ]
        assert is_sip_40_payload(errors) is True

    def test_invalid_extra_key(self):
        errors = [{"message": "err", "error_type": "UNKNOWN", "invalid_key": True}]
        assert is_sip_40_payload(errors) is False

    def test_not_list(self):
        assert is_sip_40_payload("not a list") is False

    def test_empty_list(self):
        assert is_sip_40_payload([]) is True


# ─── validate_response ───────────────────────────────────────────────────
class TestValidateResponse:
    def test_ok_response(self):
        response = MagicMock()
        response.ok = True
        assert validate_response(response) is None  # no exception

    def test_json_error_with_sip40(self):
        response = MagicMock()
        response.ok = False
        response.headers = {"content-type": "application/json"}
        response.json.return_value = {
            "errors": [
                {"message": "bad", "error_type": "UNKNOWN", "level": "error"},
            ],
        }
        with pytest.raises(SupersetError):
            validate_response(response)

    def test_json_error_without_sip40(self):
        response = MagicMock()
        response.ok = False
        response.headers = {"content-type": "application/json"}
        response.json.return_value = {"some": "payload"}
        with pytest.raises(SupersetError):
            validate_response(response)

    def test_non_json_error(self):
        response = MagicMock()
        response.ok = False
        response.headers = {"content-type": "text/html"}
        response.text = "Server Error"
        with pytest.raises(SupersetError):
            validate_response(response)


# ─── split_comma ──────────────────────────────────────────────────────────
class TestSplitComma:
    def test_basic_split(self):
        ctx = MagicMock()
        assert split_comma(ctx, "param", "a,b,c") == ["a", "b", "c"]

    def test_strips_whitespace(self):
        ctx = MagicMock()
        assert split_comma(ctx, "param", " a , b , c ") == ["a", "b", "c"]

    def test_none_returns_empty(self):
        ctx = MagicMock()
        assert split_comma(ctx, "param", None) == []

    def test_single_value(self):
        ctx = MagicMock()
        assert split_comma(ctx, "param", "single") == ["single"]


# ─── dict_merge ───────────────────────────────────────────────────────────
class TestDictMerge:
    def test_simple_override(self):
        base = {"a": 1, "b": 2}
        overrides = {"b": 3, "c": 4}
        dict_merge(base, overrides)
        assert base == {"a": 1, "b": 3, "c": 4}

    def test_recursive_merge(self):
        base = {"nested": {"a": 1, "b": 2}}
        overrides = {"nested": {"b": 3, "c": 4}}
        dict_merge(base, overrides)
        assert base == {"nested": {"a": 1, "b": 3, "c": 4}}

    def test_non_dict_replaces_dict(self):
        base = {"a": {"nested": True}}
        overrides = {"a": "replaced"}
        dict_merge(base, overrides)
        assert base == {"a": "replaced"}

    def test_deep_nested(self):
        base = {"l1": {"l2": {"l3": "old"}}}
        overrides = {"l1": {"l2": {"l3": "new", "l3b": "extra"}}}
        dict_merge(base, overrides)
        assert base == {"l1": {"l2": {"l3": "new", "l3b": "extra"}}}


# ─── raise_cli_errors ─────────────────────────────────────────────────────
class TestRaiseCliErrors:
    def test_no_error(self):
        @raise_cli_errors
        def ok_function():
            return "success"

        assert ok_function() == "success"

    def test_cli_error_causes_exit(self):
        @raise_cli_errors
        def failing_function():
            raise CLIError("test error", 42)

        with pytest.raises(SystemExit) as exc_info:
            failing_function()
        assert exc_info.value.code == 42


# ─── Exceptions ───────────────────────────────────────────────────────────
class TestExceptions:
    def test_error_level_values(self):
        assert ErrorLevel.INFO == "info"
        assert ErrorLevel.WARNING == "warning"
        assert ErrorLevel.ERROR == "error"

    def test_superset_error(self):
        errors = [{"message": "err", "error_type": "TEST"}]
        exc = SupersetError(errors=errors)
        assert exc.errors == errors

    def test_database_not_found_error(self):
        exc = DatabaseNotFoundError()
        assert len(exc.errors) == 1
        assert exc.errors[0]["error_type"] == "DATABASE_NOT_FOUND_ERROR"

    def test_cli_error(self):
        exc = CLIError("test message", 99)
        assert str(exc) == "test message"
        assert exc.exit_code == 99


# ─── Operators ────────────────────────────────────────────────────────────
class TestOperators:
    def test_base_operator(self):
        op = Operator("test")
        assert op.value == "test"
        assert op.operator == "invalid"

    def test_equal_operator(self):
        op = Equal("value")
        assert op.operator == "eq"
        assert op.value == "value"

    def test_one_to_many_operator(self):
        op = OneToMany(42)
        assert op.operator == "rel_o_m"
        assert op.value == 42


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
