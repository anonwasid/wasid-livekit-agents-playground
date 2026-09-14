"""Cloudflare R2 S3-Compatible Storage Service for LiveKit Voice Recordings.

Provides:
  - AWS SigV4 signed URL generation for secure audio playback and downloads.
  - Streaming audio proxy with HTTP Range header support for seeking in HTML5 players.
  - Object deletion for managing storage lifecycle.
"""

import datetime
import hashlib
import hmac
import logging
import os
import urllib.parse
from typing import Any, AsyncGenerator, Dict, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

DEFAULT_R2_ACCOUNT_ID = "0b3856e3bc0d783e76a90f2d54c3d2c9"
DEFAULT_R2_ENDPOINT = f"https://{DEFAULT_R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
DEFAULT_R2_BUCKET = "n8n-production-backups"


class StorageR2Service:
    """Async Cloudflare R2 Storage Manager."""

    def __init__(
        self,
        endpoint: Optional[str] = None,
        bucket: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region: str = "auto",
    ):
        self.endpoint = (endpoint if endpoint is not None else os.getenv("R2_ENDPOINT", DEFAULT_R2_ENDPOINT)).rstrip("/")
        self.bucket = bucket if bucket is not None else os.getenv("R2_BUCKET", "n8n-production-backups")
        self.access_key = access_key if access_key is not None else os.getenv("R2_ACCESS_KEY_ID", "")
        self.secret_key = secret_key if secret_key is not None else os.getenv("R2_SECRET_ACCESS_KEY", "")
        self.region = region or os.getenv("R2_REGION", "auto")

        # Extract host from endpoint
        parsed = urllib.parse.urlparse(self.endpoint)
        self.host = parsed.netloc

    @property
    def is_configured(self) -> bool:
        return bool(self.access_key and self.secret_key and self.endpoint and self.bucket)

    def _sign(self, key: bytes, msg: str) -> bytes:
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    def _get_signature_key(self, date_stamp: str) -> bytes:
        k_date = self._sign(("AWS4" + self.secret_key).encode("utf-8"), date_stamp)
        k_region = self._sign(k_date, self.region)
        k_service = self._sign(k_region, "s3")
        return self._sign(k_service, "aws4_request")

    def generate_presigned_url(
        self,
        object_key: str,
        expires_in: int = 900,
        download: bool = False,
        filename: Optional[str] = None,
    ) -> str:
        """Generate an AWS SigV4 pre-signed GET URL for Cloudflare R2."""
        if not self.is_configured:
            logger.warning("R2 storage credentials not configured, cannot generate pre-signed URL")
            return ""

        now = datetime.datetime.now(datetime.timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")

        # Normalize key
        clean_key = object_key.lstrip("/")
        canonical_uri = f"/{self.bucket}/{urllib.parse.quote(clean_key, safe='/')}"

        query_params = {
            "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
            "X-Amz-Credential": f"{self.access_key}/{date_stamp}/{self.region}/s3/aws4_request",
            "X-Amz-Date": amz_date,
            "X-Amz-Expires": str(expires_in),
            "X-Amz-SignedHeaders": "host",
        }

        if download:
            disp_filename = filename or clean_key.split("/")[-1]
            query_params["response-content-disposition"] = f'attachment; filename="{disp_filename}"'

        canonical_querystring = urllib.parse.urlencode(
            sorted(query_params.items()),
            quote_via=urllib.parse.quote,
        )
        canonical_headers = f"host:{self.host}\n"
        signed_headers = "host"
        payload_hash = "UNSIGNED-PAYLOAD"

        canonical_request = (
            f"GET\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        )

        algorithm = "AWS4-HMAC-SHA256"
        credential_scope = f"{date_stamp}/{self.region}/s3/aws4_request"
        string_to_sign = (
            f"{algorithm}\n{amz_date}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}"
        )

        signing_key = self._get_signature_key(date_stamp)
        signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

        return f"{self.endpoint}{canonical_uri}?{canonical_querystring}&X-Amz-Signature={signature}"

    async def get_object_metadata(self, object_key: str) -> Optional[Dict[str, Any]]:
        """Fetch HEAD metadata for an object in Cloudflare R2."""
        if not self.is_configured:
            return None

        url = self.generate_presigned_url(object_key, expires_in=60)
        if not url:
            return None

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                res = await client.head(url)
                if res.status_code == 200:
                    return {
                        "content_length": int(res.headers.get("content-length", 0)),
                        "content_type": res.headers.get("content-type", "audio/mp4"),
                        "last_modified": res.headers.get("last-modified"),
                        "etag": res.headers.get("etag", "").strip('"'),
                    }
        except Exception as e:
            logger.debug("Failed to fetch R2 object metadata for %s: %s", object_key, e)
        return None

    async def stream_object(
        self,
        object_key: str,
        range_header: Optional[str] = None,
    ) -> Tuple[int, Dict[str, str], AsyncGenerator[bytes, None]]:
        """Stream an object from Cloudflare R2 with HTTP Range support for seeking."""
        url = self.generate_presigned_url(object_key, expires_in=300)
        req_headers = {}
        if range_header:
            req_headers["Range"] = range_header

        client = httpx.AsyncClient(timeout=30.0)
        req = client.build_request("GET", url, headers=req_headers)
        res = await client.send(req, stream=True)

        resp_headers = {
            "Content-Type": res.headers.get("Content-Type", "audio/mp4"),
            "Accept-Ranges": "bytes",
        }
        if "Content-Range" in res.headers:
            resp_headers["Content-Range"] = res.headers["Content-Range"]
        if "Content-Length" in res.headers:
            resp_headers["Content-Length"] = res.headers["Content-Length"]

        async def body_stream() -> AsyncGenerator[bytes, None]:
            try:
                async for chunk in res.aiter_bytes():
                    yield chunk
            finally:
                await res.aclose()
                await client.aclose()

        return res.status_code, resp_headers, body_stream()

    async def delete_object(self, object_key: str) -> bool:
        """Delete an object from Cloudflare R2."""
        if not self.is_configured:
            return False

        now = datetime.datetime.now(datetime.timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")

        clean_key = object_key.lstrip("/")
        canonical_uri = f"/{self.bucket}/{urllib.parse.quote(clean_key, safe='/')}"

        payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        canonical_headers = (
            f"host:{self.host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n"
        )
        signed_headers = "host;x-amz-content-sha256;x-amz-date"
        canonical_request = f"DELETE\n{canonical_uri}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}"

        algorithm = "AWS4-HMAC-SHA256"
        credential_scope = f"{date_stamp}/{self.region}/s3/aws4_request"
        string_to_sign = (
            f"{algorithm}\n{amz_date}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}"
        )

        signing_key = self._get_signature_key(date_stamp)
        signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

        auth_header = (
            f"{algorithm} Credential={self.access_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        )

        headers = {
            "x-amz-date": amz_date,
            "x-amz-content-sha256": payload_hash,
            "Authorization": auth_header,
        }

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                res = await client.delete(f"{self.endpoint}{canonical_uri}", headers=headers)
                return res.status_code in (200, 204)
        except Exception as e:
            logger.warning("Error deleting object %s from R2: %s", object_key, e)
            return False


# Singleton instance
storage_r2 = StorageR2Service()
