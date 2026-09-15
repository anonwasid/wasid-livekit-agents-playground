"""
LiveKit Dashboard - Main Application
Stateless SSR dashboard for LiveKit server management
"""

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from starlette.middleware.sessions import SessionMiddleware

from app.routes import overview, rooms, egress, ingress, sip, settings, sandbox, auth, agents, homer, search, views, alerts, audit, diagnostics, events, webhooks
from app.security.basic_auth import get_current_user
from app.security.csrf import get_csrf_token
from app.utils.formatters import format_duration, format_pct, status_color, format_number


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown events"""
    # Startup
    print("🚀 LiveKit Dashboard starting up...")
    print(f"   LiveKit URL: {os.environ.get('LIVEKIT_URL', 'Not set')}")
    print(f"   SIP Enabled: {os.environ.get('ENABLE_SIP', 'false')}")
    print(f"   Homer Enabled: {os.environ.get('ENABLE_HOMER', 'false')}")

    # Verify required environment variables
    required_vars = ["LIVEKIT_URL", "LIVEKIT_API_KEY", "LIVEKIT_API_SECRET"]
    missing_vars = [var for var in required_vars if not os.environ.get(var)]

    if missing_vars:
        print(f"⚠️  WARNING: Missing required environment variables: {', '.join(missing_vars)}")
    else:
        print("✅ All required environment variables are set")

    # Initialize Telephony Database
    try:
        from app.services.db import telephony_db
        await telephony_db.init_db()
        print("✅ Telephony PostgreSQL/SQLite Database initialized")
        stale_reset = await telephony_db.reset_stale_transcriptions()
        if stale_reset:
            print(f"🔄 Reset {stale_reset} stale 'transcribing' records to 'pending'")
    except Exception as e:
        print(f"⚠️ Failed to initialize Telephony Database: {e}")

    # Start Recording Supervisor
    try:
        from app.services.recording_supervisor import start_recording_supervisor, stop_recording_supervisor
        start_recording_supervisor()
        print("✅ LiveKit Call Recording Supervisor started")
    except Exception as e:
        print(f"⚠️ Failed to start Recording Supervisor: {e}")

    # Start Automated 30-Day Retention Pruning Loop
    prune_task = None
    async def _retention_pruning_loop():
        await asyncio.sleep(10)  # Wait 10s after startup
        while True:
            try:
                from app.services.db import telephony_db
                res = await telephony_db.prune_expired_recordings(days=30)
                if res.get("deleted_count", 0) > 0:
                    print(f"🧹 Automated Retention Pruned: {res['deleted_count']} recording(s) older than 30 days ({res['freed_bytes']} bytes freed from R2 and DB)")
            except Exception as e:
                print(f"⚠️ Retention pruning error: {e}")
            await asyncio.sleep(6 * 3600)  # Check every 6 hours

    prune_task = asyncio.create_task(_retention_pruning_loop())
    print("✅ Automated 1-Month Call Recording Retention policy initialized (runs every 6h)")

    yield

    # Shutdown
    if prune_task:
        prune_task.cancel()
    try:
        from app.services.recording_supervisor import stop_recording_supervisor
        stop_recording_supervisor()
    except Exception:
        pass
    print("👋 LiveKit Dashboard shutting down...")


# Create FastAPI app
app = FastAPI(
    title="LiveKit Dashboard",
    description="Self-hosted SSR dashboard for LiveKit server management",
    version="0.1.0",
    lifespan=lifespan,
    docs_url=None,  # Disable Swagger UI in production
    redoc_url=None,  # Disable ReDoc in production
)

# Route protection and authentication guard middleware
@app.middleware("http")
async def auth_guard_middleware(request: Request, call_next):
    """
    Enterprise Authentication Guard:
    Intercepts and validates sessions / basic auth on all routes.
    Whitelisted public paths:
    - /login
    - /logout
    - /health
    - /static/*
    - /favicon.ico
    - /api/webhooks/*
    - /api/v1/transcriptions
    """
    path = request.url.path

    # Allow public endpoints
    if (
        path in ("/login", "/logout", "/health", "/favicon.ico")
        or path.startswith("/static/")
        or path.startswith("/api/webhooks/")
        or path.startswith("/api/v1/transcriptions")
    ):
        return await call_next(request)

    user = get_current_user(request)
    if user:
        request.state.user = user
        return await call_next(request)

    # If unauthenticated, determine response type
    # Check if request is HTMX
    if request.headers.get("hx-request") == "true":
        from fastapi.responses import Response as _Resp
        target = f"/login?next={path}"
        return _Resp(status_code=401, headers={"HX-Redirect": target})

    # Check if request is browser HTML navigation
    accept = request.headers.get("accept", "").lower()
    if ("text/html" in accept) and (request.method == "GET"):
        from fastapi.responses import RedirectResponse
        target = f"/login?next={path}"
        return RedirectResponse(url=target, status_code=307)

    # API / non-HTML requests return 401 with WWW-Authenticate header
    from fastapi.responses import Response as _Resp
    return _Resp(
        content="Authentication required",
        status_code=401,
        headers={"WWW-Authenticate": 'Basic realm="WASID LiveKit Control Center"'},
    )


@app.middleware("http")
async def ensure_csrf_token(request: Request, call_next):
    """Ensure every request has a CSRF token available for templates."""
    get_csrf_token(request)
    return await call_next(request)


@app.middleware("http")
async def enforce_readonly_mode(request: Request, call_next):
    """Block mutating requests when DASHBOARD_ROLE=readonly."""
    if os.environ.get("DASHBOARD_ROLE", "admin").lower() == "readonly":
        if request.method in ("POST", "PUT", "PATCH", "DELETE"):
            if not request.url.path.startswith("/auth") and request.url.path not in ("/login", "/logout"):
                from fastapi.responses import Response as _Resp
                return _Resp("Read-only mode — mutations are disabled.", status_code=403)
    return await call_next(request)


# Session middleware MUST wrap around HTTP middleware to populate request.session first
app.add_middleware(
    SessionMiddleware,
    secret_key=os.environ.get("APP_SECRET_KEY", "dev-secret-key-change-in-production"),
    session_cookie="wasid_lk_session",
    max_age=86400,
    same_site="lax",
)

# Add CORS middleware (restrictive by default)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[],  # No CORS by default for security
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# Setup Jinja2 templates
templates = Jinja2Templates(directory="app/templates")


# Add custom template globals
CSS_VERSION = "0.2.1"

templates.env.globals["css_version"] = CSS_VERSION
templates.env.globals["homer_enabled"] = lambda: os.environ.get("ENABLE_HOMER", "false").lower() == "true"
templates.env.globals["sip_enabled"] = lambda: os.environ.get("ENABLE_SIP", "false").lower() == "true"
templates.env.globals["is_readonly"] = lambda: os.environ.get("DASHBOARD_ROLE", "admin").lower() == "readonly"
templates.env.globals["get_current_user"] = get_current_user


def _datetimeformat(value: int) -> str:
    """Format a Unix timestamp (seconds) as a human-readable string."""
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(value)


templates.env.filters["datetimeformat"] = _datetimeformat


def _proto_map_tojson(value) -> str:
    """Convert a protobuf ScalarMapContainer (or any dict-like) to a JSON string."""
    import json
    try:
        return json.dumps(dict(value))
    except Exception:
        return "{}"


templates.env.filters["proto_map_tojson"] = _proto_map_tojson

# Formatting helpers (from app.utils.formatters)
templates.env.filters["duration"] = format_duration
templates.env.filters["pct"] = format_pct
templates.env.filters["status_color"] = status_color
templates.env.filters["numformat"] = format_number

# Store templates in app state for route access
app.state.templates = templates

# Mount static files
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Include routers
app.include_router(overview.router, tags=["Overview"])
app.include_router(agents.router, tags=["Agents"])
app.include_router(rooms.router, tags=["Rooms"])
app.include_router(egress.router, tags=["Egress"])
app.include_router(ingress.router, tags=["Ingress"])
app.include_router(sip.router, tags=["SIP"])
app.include_router(settings.router, tags=["Settings"])
app.include_router(sandbox.router, tags=["Sandbox"])
app.include_router(auth.router, tags=["Auth"])
app.include_router(homer.router, tags=["Homer"])
app.include_router(search.router, tags=["Search"])
app.include_router(views.router, tags=["Views"])
app.include_router(alerts.router, tags=["Alerts"])
app.include_router(audit.router, tags=["Audit"])
app.include_router(diagnostics.router, tags=["Diagnostics"])
app.include_router(events.router, tags=["Events"])
app.include_router(webhooks.router, tags=["Webhooks"])


# Security headers middleware
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    """Add security headers to all responses"""
    response = await call_next(request)

    # Security headers
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"

    # Only add HSTS in production with HTTPS
    if os.environ.get("DEBUG", "false").lower() != "true":
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"

    # Content Security Policy (adjust as needed)
    csp = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://unpkg.com; "
        "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
        "font-src 'self' https://cdn.jsdelivr.net; "
        "img-src 'self' data: https:; "
        "connect-src 'self';"
    )
    response.headers["Content-Security-Policy"] = csp

    # Disable caching for HTML pages
    if response.headers.get("content-type", "").startswith("text/html"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"

    return response


# Error handlers
@app.exception_handler(404)
async def not_found_handler(request: Request, exc):
    """Custom 404 page"""
    return templates.TemplateResponse(request, 
        "base.html.j2",
        {
            "request": request,
            "error": "Page not found",
        },
        status_code=404,
    )


@app.exception_handler(500)
async def server_error_handler(request: Request, exc):
    """Custom 500 page"""
    return templates.TemplateResponse(request, 
        "base.html.j2",
        {
            "request": request,
            "error": "Internal server error",
        },
        status_code=500,
    )


# Health check endpoint (no auth required)
@app.get("/health", response_class=HTMLResponse)
async def health_check():
    """Health check endpoint"""
    return HTMLResponse(content="OK", status_code=200)


if __name__ == "__main__":
    import uvicorn

    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8000"))
    debug = os.environ.get("DEBUG", "false").lower() == "true"

    uvicorn.run(
        "app.main:app",
        host=host,
        port=port,
        reload=debug,
        log_level="info" if debug else "warning",
    )
