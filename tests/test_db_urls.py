"""
test_db_urls.py — Unit tests for database configuration loading.

Tests that config.py correctly reads DB settings from environment variables,
applies sensible defaults, and returns correct types.
Does NOT require a running database.
"""
import os
import pytest


def test_db_host_is_string():
    """DB_HOST is always a string."""
    from config.config import DB_HOST
    assert isinstance(DB_HOST, str)
    assert len(DB_HOST) > 0


def test_db_port_is_integer():
    """DB_PORT is always returned as an integer."""
    from config.config import DB_PORT
    assert isinstance(DB_PORT, int)
    assert 1 <= DB_PORT <= 65535


def test_db_name_is_string():
    """DB_NAME is a non-empty string."""
    from config.config import DB_NAME
    assert isinstance(DB_NAME, str)
    assert len(DB_NAME) > 0


def test_db_user_is_string():
    """DB_USER is a non-empty string."""
    from config.config import DB_USER
    assert isinstance(DB_USER, str)
    assert len(DB_USER) > 0


def test_openai_key_loaded():
    """OPENAI_API_KEY is available from config (may be 'dummy' in CI)."""
    from config.config import OPENAI_API_KEY
    assert isinstance(OPENAI_API_KEY, str)


def test_no_hardcoded_localhost_password():
    """DB_PASSWORD does not contain the literal string 'YOUR_MYSQL_PASSWORD'."""
    from config.config import DB_PASSWORD
    assert DB_PASSWORD != "YOUR_MYSQL_PASSWORD", (
        "DB_PASSWORD is still the placeholder. Set DB_PASSWORD in .env"
    )
