"""Unit tests for Cloudflare R2 Storage Service."""

import pytest
from unittest.mock import patch, AsyncMock
from app.services.storage_r2 import StorageR2Service


def test_storage_r2_configured():
    svc = StorageR2Service(
        endpoint="https://0b3856e3bc0d783e76a90f2d54c3d2c9.r2.cloudflarestorage.com",
        access_key="test-key",
        secret_key="test-secret",
        bucket="wasid-voice-recordings",
    )
    assert svc.is_configured is True
    assert svc.bucket == "wasid-voice-recordings"


def test_storage_r2_presigned_url():
    svc = StorageR2Service(
        endpoint="https://0b3856e3bc0d783e76a90f2d54c3d2c9.r2.cloudflarestorage.com",
        access_key="test-key",
        secret_key="test-secret",
        bucket="wasid-voice-recordings",
    )
    url = svc.generate_presigned_url(
        "recordings/2026/09/15/inbound/rec_001.m4a",
        expires_in=900,
        download=True,
        filename="call.m4a",
    )
    assert url is not None
    assert "wasid-voice-recordings" in url
    assert "X-Amz-Signature=" in url
    assert "X-Amz-Algorithm=AWS4-HMAC-SHA256" in url
    assert "response-content-disposition=" in url


def test_storage_r2_unconfigured_fallback():
    svc = StorageR2Service(
        endpoint="",
        access_key="",
        secret_key="",
        bucket="",
    )
    assert svc.is_configured is False
    assert not svc.generate_presigned_url("test.m4a")
