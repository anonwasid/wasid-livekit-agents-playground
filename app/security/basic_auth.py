"""
Enterprise Session Authentication & Security Layer for LiveKit Dashboard.
Provides:
- Server-side session cookie management (HttpOnly, SameSite, Secure)
- Server-side IP rate limiting against brute-force / password guessing
- Constant-time credential verification against ADMIN_USERNAME & ADMIN_PASSWORD
- Backward-compatible HTTP Basic Auth support for API clients
- Route protection dependencies and middleware
"""

import base64
import logging
import os
import secrets
import time
from typing import Dict, List, Optional, Tuple

from fastapi import HTTPException, Request, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

logger = logging.getLogger(__name__)

# Basic auth scheme with auto_error=False so it doesn't hijack browser with 401 popup
security = HTTPBasic(auto_error=False)

# Server-side In-Memory Rate Limiter for Login Attempts
# Format: {ip_address: [timestamp_1, timestamp_2, ...]}
_FAILED_LOGIN_ATTEMPTS: Dict[str, List[float]] = {}
RATE_LIMIT_MAX_ATTEMPTS = 5
RATE_LIMIT_WINDOW_SECONDS = 300  # 5 minutes
SESSION_MAX_AGE_SECONDS = 86400  # 24 hours


def get_client_ip(request: Request) -> str:
    """Extract real client IP considering forward headers."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    real_ip = request.headers.get("x-real-ip")
    if real_ip:
        return real_ip.strip()
    return request.client.host if request.client else "unknown"


def check_rate_limit(client_ip: str) -> Tuple[bool, int]:
    """
    Checks if client IP has exceeded failed login attempts.
    Returns (is_allowed, seconds_until_unlock).
    """
    now = time.time()
    cutoff = now - RATE_LIMIT_WINDOW_SECONDS

    # Clean expired timestamps
    attempts = [t for t in _FAILED_LOGIN_ATTEMPTS.get(client_ip, []) if t > cutoff]
    _FAILED_LOGIN_ATTEMPTS[client_ip] = attempts

    if len(attempts) >= RATE_LIMIT_MAX_ATTEMPTS:
        oldest_active = attempts[0]
        seconds_remaining = max(1, int(RATE_LIMIT_WINDOW_SECONDS - (now - oldest_active)))
        return False, seconds_remaining

    return True, 0


def record_failed_attempt(client_ip: str) -> None:
    """Records a failed authentication attempt."""
    now = time.time()
    cutoff = now - RATE_LIMIT_WINDOW_SECONDS
    attempts = [t for t in _FAILED_LOGIN_ATTEMPTS.get(client_ip, []) if t > cutoff]
    attempts.append(now)
    _FAILED_LOGIN_ATTEMPTS[client_ip] = attempts
    logger.warning("Failed login attempt from IP %s (count: %d/%d)", client_ip, len(attempts), RATE_LIMIT_MAX_ATTEMPTS)


def reset_rate_limit(client_ip: str) -> None:
    """Clears failed login attempts for an IP upon successful authentication."""
    _FAILED_LOGIN_ATTEMPTS.pop(client_ip, None)


def verify_username_password(username: str, password: str) -> bool:
    """
    Verify username and password against environment variables
    using constant-time comparison to prevent timing attacks.
    """
    correct_username = os.environ.get("ADMIN_USERNAME", "admin")
    correct_password = os.environ.get("ADMIN_PASSWORD", "changeme")

    username_correct = secrets.compare_digest(
        username.strip().encode("utf8"),
        correct_username.strip().encode("utf8"),
    )
    password_correct = secrets.compare_digest(
        password.encode("utf8"),
        correct_password.encode("utf8"),
    )

    return username_correct and password_correct


def verify_credentials(credentials: Optional[HTTPBasicCredentials]) -> bool:
    """Verify HTTPBasicCredentials for API backward compatibility."""
    if not credentials:
        return False
    return verify_username_password(credentials.username, credentials.password)


def is_authenticated(request: Request) -> bool:
    """Checks whether the request has an active, valid session."""
    if "session" not in request.scope:
        return False
    session = request.scope.get("session")
    if not session or not isinstance(session, dict):
        return False

    auth_val = session.get("authenticated")
    if not auth_val:
        return False

    # Check session expiration
    auth_time = session.get("authenticated_at", 0)
    if time.time() - auth_time > SESSION_MAX_AGE_SECONDS:
        session.clear()
        return False

    return True


def login_user(request: Request, username: str) -> None:
    """Establishes an authenticated session."""
    if "session" in request.scope:
        session = request.scope["session"]
        session["authenticated"] = True
        session["user"] = username
        session["authenticated_at"] = time.time()


def logout_user(request: Request) -> None:
    """Destroys the current authenticated session."""
    if "session" in request.scope:
        request.scope["session"].clear()


def get_current_user(request: Request) -> Optional[str]:
    """Get currently authenticated username from session or Authorization header."""
    # 1. Check session
    if "session" in request.scope and is_authenticated(request):
        return request.scope["session"].get("user", "admin")

    # 2. Check Authorization header
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Basic "):
        try:
            encoded = auth_header.replace("Basic ", "").strip()
            decoded = base64.b64decode(encoded).decode("utf-8")
            username, pwd = decoded.split(":", 1)
            if verify_username_password(username, pwd):
                return username
        except Exception:
            pass

    return None


async def requires_admin(request: Request) -> str:
    """
    Dependency that enforces admin authentication.
    Accepts valid session cookie or valid HTTP Basic auth.
    """
    user = get_current_user(request)
    if user:
        return user

    accept_header = request.headers.get("Accept", "").lower()
    if "text/html" in accept_header and request.method == "GET":
        next_path = request.url.path
        raise HTTPException(
            status_code=status.HTTP_307_TEMPORARY_REDIRECT,
            headers={"Location": f"/login?next={next_path}"},
        )

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Authentication required",
        headers={"WWW-Authenticate": "Basic"},
    )
