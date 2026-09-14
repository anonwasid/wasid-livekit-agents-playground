"""
Tests for LiveKit Dashboard Authentication, Session Protection, and Rate Limiting.
"""

import os
import re
import pytest
from starlette.testclient import TestClient
from app.main import app
from app.security.basic_auth import _FAILED_LOGIN_ATTEMPTS, RATE_LIMIT_MAX_ATTEMPTS


@pytest.fixture(autouse=True)
def clean_rate_limits():
    """Clear in-memory rate limiter before and after each test."""
    _FAILED_LOGIN_ATTEMPTS.clear()
    yield
    _FAILED_LOGIN_ATTEMPTS.clear()


def _extract_csrf_token(html: str) -> str:
    """Extract csrf_token input value from HTML via regex."""
    match = re.search(r'name="csrf_token"\s+value="([^"]+)"', html)
    return match.group(1) if match else ""


def test_public_health_endpoint():
    """Health check must be accessible without authentication."""
    client = TestClient(app, follow_redirects=False)
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.text == "OK"


def test_unauthenticated_browser_redirects_to_login():
    """HTML GET requests to protected routes without credentials must redirect to /login?next=..."""
    client = TestClient(app, follow_redirects=False)
    resp = client.get("/", headers={"Accept": "text/html"})
    assert resp.status_code == 307
    assert resp.headers["location"] == "/login?next=/"


def test_unauthenticated_api_returns_401():
    """API requests without credentials must return 401 Unauthorized with WWW-Authenticate header."""
    client = TestClient(app, follow_redirects=False)
    resp = client.get("/api/overview", headers={"Accept": "application/json"})
    assert resp.status_code == 401
    assert "Basic realm=" in resp.headers.get("www-authenticate", "")


def test_login_page_renders_form():
    """GET /login must return 200 HTML with CSRF token and login form."""
    client = TestClient(app, follow_redirects=False)
    resp = client.get("/login")
    assert resp.status_code == 200
    assert "Sign In" in resp.text
    assert 'name="csrf_token"' in resp.text
    assert 'name="username"' in resp.text
    assert 'name="password"' in resp.text


def test_login_invalid_credentials_returns_401():
    """POST /login with wrong password returns 401."""
    client = TestClient(app, follow_redirects=False)
    get_resp = client.get("/login")
    csrf_token = _extract_csrf_token(get_resp.text)
    assert csrf_token != ""

    post_resp = client.post(
        "/login",
        data={"username": "admin", "password": "wrong_password", "csrf_token": csrf_token, "next": "/"},
    )
    assert post_resp.status_code == 401
    assert "Invalid username or password" in post_resp.text


def test_rate_limiting_after_five_failed_attempts():
    """5 consecutive failed logins must trigger HTTP 429 Too Many Requests."""
    client = TestClient(app, follow_redirects=False)
    get_resp = client.get("/login")
    csrf_token = _extract_csrf_token(get_resp.text)
    assert csrf_token != ""

    for i in range(RATE_LIMIT_MAX_ATTEMPTS):
        resp = client.post(
            "/login",
            data={"username": "admin", "password": f"wrong_{i}", "csrf_token": csrf_token, "next": "/"},
        )
        assert resp.status_code == 401

    # 6th attempt must be rate-limited (HTTP 429)
    locked_resp = client.post(
        "/login",
        data={"username": "admin", "password": "wrong_again", "csrf_token": csrf_token, "next": "/"},
    )
    assert locked_resp.status_code == 429
    assert "Too many failed login attempts" in locked_resp.text


def test_successful_login_and_logout_flow():
    """Successful login sets session, permits access, and logout revokes access."""
    admin_user = os.environ.get("ADMIN_USERNAME", "admin")
    admin_pass = os.environ.get("ADMIN_PASSWORD", "changeme")

    client = TestClient(app, follow_redirects=False)

    # 1. Access login page to fetch CSRF token
    get_resp = client.get("/login")
    csrf_token = _extract_csrf_token(get_resp.text)
    assert csrf_token != ""

    # 2. Submit valid credentials
    login_resp = client.post(
        "/login",
        data={"username": admin_user, "password": admin_pass, "csrf_token": csrf_token, "next": "/agents"},
    )
    assert login_resp.status_code == 303
    assert login_resp.headers["location"] == "/agents"

    # 3. Access protected route with established cookie
    agents_resp = client.get("/agents", headers={"Accept": "text/html"})
    assert agents_resp.status_code == 200
    assert "Agents" in agents_resp.text

    # 4. Logout
    logout_resp = client.get("/logout")
    assert logout_resp.status_code == 303
    assert logout_resp.headers["location"] == "/login"

    # 5. Route is protected again
    recheck_resp = client.get("/agents", headers={"Accept": "text/html"})
    assert recheck_resp.status_code == 307
    assert recheck_resp.headers["location"] == "/login?next=/agents"
