"""
Authentication routes for LiveKit Dashboard.
Provides:
- GET /login: Interactive sign-in page
- POST /login: Form authentication with rate limiting & session establishment
- GET /logout & POST /logout: Terminate session and redirect to /login
"""

import logging
from urllib.parse import urlparse

from fastapi import APIRouter, Form, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse

from app.security.basic_auth import (
    check_rate_limit,
    get_client_ip,
    is_authenticated,
    login_user,
    logout_user,
    record_failed_attempt,
    reset_rate_limit,
    verify_username_password,
)
from app.security.csrf import get_csrf_token, validate_csrf_token

logger = logging.getLogger(__name__)

router = APIRouter()


def is_safe_redirect(url: str) -> bool:
    """Ensure redirect URL is local to avoid open redirect vulnerabilities."""
    if not url:
        return False
    if url.startswith("//") or url.startswith("/\\"):
        return False
    if url.startswith("/"):
        parsed = urlparse(url)
        return not parsed.netloc
    return False


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    """Render the LiveKit dashboard login form."""
    next_url = request.query_params.get("next", "/")
    if not is_safe_redirect(next_url):
        next_url = "/"

    # If user already has a valid session, redirect to next or /
    if is_authenticated(request):
        return RedirectResponse(url=next_url, status_code=status.HTTP_302_FOUND)

    return request.app.state.templates.TemplateResponse(
        request,
        "auth/login.html.j2",
        {
            "request": request,
            "csrf_token": get_csrf_token(request),
            "next": next_url,
            "error_message": None,
            "username": "",
        },
    )


@router.post("/login", response_class=HTMLResponse)
async def process_login(
    request: Request,
    username: str = Form(""),
    password: str = Form(""),
    csrf_token: str = Form(""),
    next: str = Form("/"),
):
    """Process login form submission with server-side rate limiting and session creation."""
    client_ip = get_client_ip(request)

    # 1. Validate CSRF token
    if not validate_csrf_token(csrf_token):
        logger.warning("Invalid CSRF token during login attempt from IP %s", client_ip)
        return request.app.state.templates.TemplateResponse(
            request,
            "auth/login.html.j2",
            {
                "request": request,
                "csrf_token": get_csrf_token(request),
                "next": next if is_safe_redirect(next) else "/",
                "error_message": "Security token expired or invalid. Please try again.",
                "username": username,
            },
            status_code=status.HTTP_403_FORBIDDEN,
        )

    # 2. Server-side Rate Limiting (max 5 failed attempts per 5 minutes per IP)
    allowed, seconds_left = check_rate_limit(client_ip)
    if not allowed:
        logger.warning("Rate limit exceeded for IP %s on login (locked for %ds)", client_ip, seconds_left)
        return request.app.state.templates.TemplateResponse(
            request,
            "auth/login.html.j2",
            {
                "request": request,
                "csrf_token": get_csrf_token(request),
                "next": next if is_safe_redirect(next) else "/",
                "error_message": f"Too many failed login attempts. Access temporarily locked for {seconds_left} seconds.",
                "username": username,
            },
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        )

    # 3. Verify Credentials using constant-time comparison
    if not verify_username_password(username, password):
        record_failed_attempt(client_ip)
        logger.warning("Failed login for user '%s' from IP %s", username, client_ip)
        return request.app.state.templates.TemplateResponse(
            request,
            "auth/login.html.j2",
            {
                "request": request,
                "csrf_token": get_csrf_token(request),
                "next": next if is_safe_redirect(next) else "/",
                "error_message": "Invalid username or password. Please verify your credentials.",
                "username": username,
            },
            status_code=status.HTTP_401_UNAUTHORIZED,
        )

    # 4. Successful Authentication
    reset_rate_limit(client_ip)
    login_user(request, username)
    logger.info("Successful login for user '%s' from IP %s", username, client_ip)

    target_url = next if is_safe_redirect(next) else "/"
    return RedirectResponse(url=target_url, status_code=status.HTTP_303_SEE_OTHER)


@router.get("/logout")
@router.post("/logout")
async def logout(request: Request):
    """Terminate the authenticated session and redirect to login."""
    logout_user(request)
    return RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER)
