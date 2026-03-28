"""
Unified DB FastMCP server.
"""

import asyncio
import base64
import json
import logging

from mcp.server.fastmcp import Context, FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request as StarletteRequest
from starlette.responses import JSONResponse

from unified_db_mcp.config import APP_NAME, HOST, MCP_PATH, PORT, SUPPORTED_DATABASES
from unified_db_mcp.tools.migrate_schema_tool import migrate_schema_text
from unified_db_mcp.tools.schema_connector_tools import apply_schema_tool, connect_db, extract_schema_tool

logging.basicConfig(
    format="[%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

mcp = FastMCP(
    APP_NAME,
    instructions=(
        "Unified DB schema migration MCP server. "
        "Migrate schema between supported databases using schema_migrate.py."
    ),
    json_response=True,
    streamable_http_path=MCP_PATH,
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
    stateless_http=True,
)


def _extract_headers_from_context(ctx: Context = None) -> dict:
    """Extract request headers from FastMCP context as lowercase keys."""
    if not ctx:
        return {}
    try:
        request_context = ctx.request_context
        if hasattr(request_context, "request") and request_context.request:
            request = request_context.request
            return {name.lower(): request.headers[name] for name in request.headers.keys()}
        if hasattr(request_context, "headers") and request_context.headers:
            raw_headers = request_context.headers
            return {str(name).lower(): str(value) for name, value in raw_headers.items()}
    except Exception:
        return {}
    return {}


def _header_value(headers: dict, *names: str) -> str:
    """
    Return first non-empty header value from accepted aliases.

    Also supports underscore variants for clients that normalize names.
    """
    for name in names:
        key = name.lower().strip()
        value = headers.get(key, "")
        if value:
            return str(value).strip()
        alt_key = key.replace("-", "_")
        value = headers.get(alt_key, "")
        if value:
            return str(value).strip()
    return ""


def _normalize_credentials_value(value: str) -> str:
    """
    Normalize credentials string from headers.

    Supports:
    - raw JSON string
    - base64-encoded JSON string
    """
    if not value:
        return ""
    candidate = value.strip()
    if not candidate:
        return ""

    # Tolerate UI-entered separator punctuation around JSON payloads.
    # Example accepted input:
    #   , { "api_key": "...", "db_password": "...", "project_name": "..." }
    candidate = candidate.strip(" \t\r\n,;")
    if not candidate:
        return ""

    # Unwrap a single layer of quotes if the JSON was pasted as a quoted string.
    if len(candidate) >= 2 and candidate[0] == candidate[-1] and candidate[0] in ('"', "'"):
        candidate = candidate[1:-1].strip()
        candidate = candidate.strip(" \t\r\n,;")
        if not candidate:
            return ""

    # If already valid JSON object text, use it directly.
    try:
        parsed = json.loads(candidate)
        if isinstance(parsed, dict):
            return json.dumps(parsed)
        # Also accept JSON strings that contain JSON object text.
        if isinstance(parsed, str):
            nested = parsed.strip().strip(" \t\r\n,;")
            try:
                nested_obj = json.loads(nested)
                if isinstance(nested_obj, dict):
                    return json.dumps(nested_obj)
            except Exception:
                pass
    except Exception:
        pass

    # Try base64 decode then parse as JSON object.
    try:
        decoded = base64.b64decode(candidate, validate=False).decode("utf-8")
        decoded = decoded.strip().strip(" \t\r\n,;")
        parsed = json.loads(decoded)
        if isinstance(parsed, dict):
            return json.dumps(parsed)
        if isinstance(parsed, str):
            nested = parsed.strip().strip(" \t\r\n,;")
            try:
                nested_obj = json.loads(nested)
                if isinstance(nested_obj, dict):
                    return json.dumps(nested_obj)
            except Exception:
                pass
    except Exception:
        pass

    # Fallback to raw value; downstream validation will raise clear errors.
    return candidate


def _resolve_credentials_from_headers(
    db_type: str,
    credentials_json: str = "",
    sqlite_path: str = "",
    ctx: Context = None,
) -> tuple[str, str]:
    """
    Resolve credentials/sqlite_path for MCP tools from HTTP headers only.

    Priority:
    1) database-specific header: x-<db>-credentials
    2) global header: x-db-credentials
    3) sqlite path header: x-sqlite-path (sqlite only)
    """
    headers = _extract_headers_from_context(ctx)
    normalized_db = db_type.lower().strip()

    # Header-only mode for /unified-db/mcp tool calls.
    db_header = f"x-{normalized_db}-credentials"
    header_credentials = _header_value(headers, db_header, "x-db-credentials")
    header_sqlite_path = ""
    header_credentials = _normalize_credentials_value(header_credentials)

    if not header_sqlite_path and normalized_db == "sqlite":
        header_sqlite_path = _header_value(headers, "x-sqlite-path")

    resolved_credentials = header_credentials
    resolved_sqlite_path = (header_sqlite_path or "").strip()

    logger.info(
        "mcp credential resolution: operation=single db_type=%s credentials_from_headers=%s sqlite_path_from_headers=%s header_keys=%s",
        normalized_db,
        bool(header_credentials),
        bool(header_sqlite_path),
        sorted(headers.keys()),
    )

    return resolved_credentials, resolved_sqlite_path


def _resolve_migration_credentials_from_headers(
    source_db: str,
    target_db: str,
    source_credentials_json: str = "",
    target_credentials_json: str = "",
    source_sqlite_path: str = "",
    target_sqlite_path: str = "",
    ctx: Context = None,
) -> tuple[str, str, str, str]:
    headers = _extract_headers_from_context(ctx)
    source_key = source_db.lower().strip()
    target_key = target_db.lower().strip()

    # Header-only mode for /unified-db/mcp tool calls.
    src_creds_header = _header_value(headers, "x-source-db-credentials")
    tgt_creds_header = _header_value(headers, "x-target-db-credentials")

    if not src_creds_header:
        src_creds_header = _header_value(headers, f"x-{source_key}-credentials", "x-db-credentials")
    if not tgt_creds_header:
        tgt_creds_header = _header_value(headers, f"x-{target_key}-credentials", "x-db-credentials")

    src_sqlite_header = _header_value(headers, "x-source-sqlite-path")
    tgt_sqlite_header = _header_value(headers, "x-target-sqlite-path")
    if not src_sqlite_header and source_key == "sqlite":
        src_sqlite_header = _header_value(headers, "x-sqlite-path")
    if not tgt_sqlite_header and target_key == "sqlite":
        tgt_sqlite_header = _header_value(headers, "x-sqlite-path")

    src_creds = _normalize_credentials_value(src_creds_header)
    tgt_creds = _normalize_credentials_value(tgt_creds_header)
    src_sqlite = (src_sqlite_header or "").strip()
    tgt_sqlite = (tgt_sqlite_header or "").strip()

    logger.info(
        "mcp credential resolution: operation=migrate source_db=%s target_db=%s source_credentials_from_headers=%s target_credentials_from_headers=%s source_sqlite_path_from_headers=%s target_sqlite_path_from_headers=%s header_keys=%s",
        source_key,
        target_key,
        bool(src_creds_header),
        bool(tgt_creds_header),
        bool(src_sqlite_header),
        bool(tgt_sqlite_header),
        sorted(headers.keys()),
    )

    return (src_creds, tgt_creds, src_sqlite, tgt_sqlite)


@mcp.custom_route(MCP_PATH, methods=["GET"])
async def discovery(_request: StarletteRequest) -> JSONResponse:
    return JSONResponse(
        {
            "transport": "HTTP_STREAMABLE",
            "protocol": "streamable-http",
            "message": "Unified DB MCP Server - Set transport to HTTP_STREAMABLE",
            "supported_databases": SUPPORTED_DATABASES,
        }
    )


@mcp.tool()
def connect_database(
    db_type: str,
    sqlite_path: str = "",
    credentials_json: str = "",
    ctx: Context = None,
) -> str:
    """Connect to a database using connector credentials/config (supports header-based credentials)."""
    credentials_json, sqlite_path = _resolve_credentials_from_headers(
        db_type=db_type,
        credentials_json=credentials_json,
        sqlite_path=sqlite_path,
        ctx=ctx,
    )
    logger.info(
        "connect_database: db_type=%s credentials_from_headers=%s sqlite_path_from_headers=%s",
        db_type,
        bool(credentials_json),
        bool(sqlite_path),
    )
    return connect_db(
        db_type=db_type,
        sqlite_path=sqlite_path or None,
        credentials_json=credentials_json or None,
    )


@mcp.tool()
def extract_schema(
    db_type: str,
    tables: str = "",
    sqlite_path: str = "",
    credentials_json: str = "",
    ctx: Context = None,
) -> str:
    """Extract schema from a source database and return JSON text (supports header-based credentials)."""
    credentials_json, sqlite_path = _resolve_credentials_from_headers(
        db_type=db_type,
        credentials_json=credentials_json,
        sqlite_path=sqlite_path,
        ctx=ctx,
    )
    logger.info(
        "extract_schema: db_type=%s tables=%s credentials_from_headers=%s sqlite_path_from_headers=%s",
        db_type,
        tables,
        bool(credentials_json),
        bool(sqlite_path),
    )
    return extract_schema_tool(
        db_type=db_type,
        tables=tables or None,
        sqlite_path=sqlite_path or None,
        credentials_json=credentials_json or None,
    )


@mcp.tool()
def apply_schema(
    target_db: str,
    schema_json: str,
    sqlite_path: str = "",
    credentials_json: str = "",
    ctx: Context = None,
) -> str:
    """Apply provided schema JSON to target database (supports header-based credentials)."""
    credentials_json, sqlite_path = _resolve_credentials_from_headers(
        db_type=target_db,
        credentials_json=credentials_json,
        sqlite_path=sqlite_path,
        ctx=ctx,
    )
    logger.info(
        "apply_schema: target_db=%s credentials_from_headers=%s sqlite_path_from_headers=%s",
        target_db,
        bool(credentials_json),
        bool(sqlite_path),
    )
    return apply_schema_tool(
        target_db=target_db,
        schema_json=schema_json,
        sqlite_path=sqlite_path or None,
        credentials_json=credentials_json or None,
    )


@mcp.tool()
def migrate_schema(
    source_db: str,
    target_db: str,
    tables: str = "",
    dry_run: bool = False,
    require_confirmation: bool = False,
    source_credentials_json: str = "",
    target_credentials_json: str = "",
    source_sqlite_path: str = "",
    target_sqlite_path: str = "",
    ctx: Context = None,
) -> str:
    """
    High-level migration tool.
    - tables: optional comma-separated names; empty means migrate all tables.
    """
    (
        source_credentials_json,
        target_credentials_json,
        source_sqlite_path,
        target_sqlite_path,
    ) = _resolve_migration_credentials_from_headers(
        source_db=source_db,
        target_db=target_db,
        source_credentials_json=source_credentials_json,
        target_credentials_json=target_credentials_json,
        source_sqlite_path=source_sqlite_path,
        target_sqlite_path=target_sqlite_path,
        ctx=ctx,
    )
    logger.info(
        "migrate_schema: source_db=%s target_db=%s tables=%s dry_run=%s require_confirmation=%s source_credentials_from_headers=%s target_credentials_from_headers=%s source_sqlite_path_from_headers=%s target_sqlite_path_from_headers=%s",
        source_db,
        target_db,
        tables,
        dry_run,
        require_confirmation,
        bool(source_credentials_json),
        bool(target_credentials_json),
        bool(source_sqlite_path),
        bool(target_sqlite_path),
    )

    return migrate_schema_text(
        source_db=source_db,
        target_db=target_db,
        tables=tables or None,
        dry_run=dry_run,
        require_confirmation=require_confirmation,
        source_credentials_json=source_credentials_json or None,
        target_credentials_json=target_credentials_json or None,
        source_sqlite_path=source_sqlite_path or None,
        target_sqlite_path=target_sqlite_path or None,
    )


async def main() -> None:
    mcp.settings.host = HOST
    mcp.settings.port = PORT
    mcp.settings.log_level = "INFO"

    logger.info("Starting Unified DB MCP on http://%s:%s", HOST, PORT)
    logger.info(
        "Registered custom routes: %s",
        [(r.path, r.methods) for r in mcp._custom_starlette_routes],
    )
    await mcp.run_streamable_http_async()


if __name__ == "__main__":
    asyncio.run(main())

