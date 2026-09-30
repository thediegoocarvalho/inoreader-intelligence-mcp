from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, JSONResponse
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
import os
import secrets
import requests
import psycopg


mcp = FastMCP(
    "Inoreader Intelligence MCP",
    json_response=True,
    streamable_http_path="/",
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=[
            "inoreader-intelligence-mcp-production.up.railway.app",
            "inoreader-intelligence-mcp-production.up.railway.app:*",
        ],
        allowed_origins=[],
    ),
)


REDIRECT_URI = (
    "https://inoreader-intelligence-mcp-production.up.railway.app/oauth/callback"
)

OAUTH_STATE_COOKIE = "inoreader_oauth_state"

INOREADER_TOKEN_URL = "https://www.inoreader.com/oauth2/token"
INOREADER_API_BASE = "https://www.inoreader.com/reader/api/0"
def get_db_connection():
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise RuntimeError("DATABASE_URL is not configured.")

    return psycopg.connect(database_url)

def get_stream_checkpoint(stream_id: str):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    stream_id,
                    last_start_time,
                    last_timestamp_usec,
                    last_continuation,
                    last_run_at,
                    updated_at
                FROM radar_checkpoints
                WHERE stream_id = %s
                """,
                (stream_id,),
            )

            row = cur.fetchone()

    if row is None:
        return None

    return {
        "stream_id": row[0],
        "last_start_time": row[1],
        "last_timestamp_usec": row[2],
        "last_continuation": row[3],
        "last_run_at": row[4],
        "updated_at": row[5],
    }

def init_database():
    with get_db_connection() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS radar_articles (
                    id BIGSERIAL PRIMARY KEY,
                    inoreader_item_id TEXT UNIQUE NOT NULL,
                    title TEXT,
                    source_title TEXT,
                    url TEXT,
                    published INTEGER,
                    timestamp_usec BIGINT,
                    crawl_time_msec BIGINT,
                    summary TEXT,
                    content_text TEXT,
                    raw_item JSONB NOT NULL,
                    status TEXT NOT NULL DEFAULT 'captured',
                    strategic_relevance BOOLEAN,
                    signal_type TEXT,
                    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    processed_at TIMESTAMPTZ,
                    marked_read_at TIMESTAMPTZ
                )
                """
            )

            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS radar_articles_timestamp_idx
                ON radar_articles (timestamp_usec)
                """
            )

            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS radar_articles_published_idx
                ON radar_articles (published)
                """
            )

            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS radar_articles_source_idx
                ON radar_articles (source_title)
                """
            )

            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS radar_articles_status_idx
                ON radar_articles (status)
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS radar_checkpoints (
                    stream_id TEXT PRIMARY KEY,
                    last_start_time BIGINT,
                    last_timestamp_usec BIGINT,
                    last_continuation TEXT,
                    last_run_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS radar_ingestion_runs (
                    id BIGSERIAL PRIMARY KEY,
                    stream_id TEXT NOT NULL,
                    start_time BIGINT,
                    next_start_time BIGINT,
                    items_received INTEGER NOT NULL DEFAULT 0,
                    items_inserted INTEGER NOT NULL DEFAULT 0,
                    items_existing INTEGER NOT NULL DEFAULT 0,
                    pages_fetched INTEGER NOT NULL DEFAULT 0,
                    continuation TEXT,
                    status TEXT NOT NULL DEFAULT 'running',
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    finished_at TIMESTAMPTZ,
                    error_message TEXT
                )
                """
            )

        conn.commit()

def archive_articles(items: list[dict]) -> dict:
    inserted = 0
    existing = 0

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for item in items:
                item_id = item.get("id")

                if not item_id:
                    continue

                title = item.get("title")

                origin = item.get("origin") or {}
                source_title = origin.get("title")

                alternate = item.get("alternate") or []
                url = None
                if alternate and isinstance(alternate, list):
                    url = alternate[0].get("href")

                summary_obj = item.get("summary") or {}
                summary = summary_obj.get("content")

                content_obj = item.get("content") or {}
                content_text = content_obj.get("content")

                published = item.get("published")

                timestamp_usec = item.get("timestampUsec")
                if timestamp_usec is not None:
                    timestamp_usec = int(timestamp_usec)

                crawl_time_msec = item.get("crawlTimeMsec")
                if crawl_time_msec is not None:
                    crawl_time_msec = int(crawl_time_msec)

                cur.execute(
                    """
                    INSERT INTO radar_articles (
                        inoreader_item_id,
                        title,
                        source_title,
                        url,
                        published,
                        timestamp_usec,
                        crawl_time_msec,
                        summary,
                        content_text,
                        raw_item
                    )
                    VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (inoreader_item_id)
                    DO NOTHING
                    RETURNING id
                    """,
                    (
                        item_id,
                        title,
                        source_title,
                        url,
                        published,
                        timestamp_usec,
                        crawl_time_msec,
                        summary,
                        content_text,
                        psycopg.types.json.Jsonb(item),
                    ),
                )

                result = cur.fetchone()

                if result:
                    inserted += 1
                else:
                    existing += 1

        conn.commit()

    return {
        "received": len(items),
        "inserted": inserted,
        "existing": existing,
    }


def get_access_token():
    response = requests.post(
        INOREADER_TOKEN_URL,
        data={
            "client_id": os.getenv("INOREADER_CLIENT_ID"),
            "client_secret": os.getenv("INOREADER_CLIENT_SECRET"),
            "refresh_token": os.getenv("INOREADER_REFRESH_TOKEN"),
            "grant_type": "refresh_token",
        },
        timeout=20,
    )

    response.raise_for_status()

    token_data = response.json()

    return token_data["access_token"]


def inoreader_get(path: str, params: dict | None = None):
    access_token = get_access_token()

    response = requests.get(
        f"{INOREADER_API_BASE}{path}",
        headers={
            "Authorization": f"Bearer {access_token}",
        },
        params=params,
        timeout=30,
    )

    response.raise_for_status()

    return response.json()

def ingest_stream_to_archive(
    stream_id: str,
    max_items: int = 3000,
    page_size: int = 100,
    start_time: int | None = None,
    continuation: str | None = None,
):

    if max_items < 1:
        max_items = 1

    if max_items > 3000:
        max_items = 3000

    if page_size < 1:
        page_size = 1

    if page_size > 100:
        page_size = 100

    if start_time is None and continuation is None:
        checkpoint = get_stream_checkpoint(stream_id)

        if (
            checkpoint is not None
            and checkpoint["last_start_time"] is not None
        ):
            start_time = checkpoint["last_start_time"]

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO radar_ingestion_runs (
                    stream_id,
                    start_time,
                    status
                )
                VALUES (%s, %s, 'running')
                RETURNING id
                """,
                (
                    stream_id,
                    start_time,
                ),
            )

            run_id = cur.fetchone()[0]

        conn.commit()

    items_received = 0
    items_inserted = 0
    items_existing = 0
    pages_fetched = 0
    max_timestamp_usec = None

    try:

        while items_received < max_items:

            params = {
                "n": min(
                    page_size,
                    max_items - items_received,
                ),
            }

            if start_time is not None:
                params["ot"] = start_time

            if continuation:
                params["c"] = continuation

            data = inoreader_get(
                f"/stream/contents/{stream_id}",
                params=params,
            )

            page_items = data.get("items", [])

            if not page_items:
                continuation = data.get("continuation")
                break

            archive_result = archive_articles(page_items)

            items_received += len(page_items)
            items_inserted += archive_result["inserted"]
            items_existing += archive_result["existing"]
            pages_fetched += 1


            for item in page_items:

                timestamp_usec = item.get("timestampUsec")

                if timestamp_usec is None:
                    continue

                timestamp_usec = int(timestamp_usec)

                if (
                    max_timestamp_usec is None
                    or timestamp_usec > max_timestamp_usec
                ):
                    max_timestamp_usec = timestamp_usec


            continuation = data.get("continuation")

            if not continuation:
                break


        next_start_time = start_time

        if max_timestamp_usec is not None:
            next_start_time = max_timestamp_usec // 1_000_000


        complete = continuation is None

        run_status = "completed" if complete else "partial"

        checkpoint_start_time = None
        checkpoint_timestamp_usec = None

    if complete and start_time is not None:
        checkpoint = get_stream_checkpoint(stream_id)

        previous_timestamp_usec = None
        if checkpoint is not None:
            previous_timestamp_usec = checkpoint["last_timestamp_usec"]

        checkpoint_timestamp_usec = max_timestamp_usec

        if (
            previous_timestamp_usec is not None
            and (
                checkpoint_timestamp_usec is None
                or previous_timestamp_usec > checkpoint_timestamp_usec
            )
        ):
            checkpoint_timestamp_usec = previous_timestamp_usec

        if checkpoint_timestamp_usec is not None:
            checkpoint_start_time = checkpoint_timestamp_usec // 1_000_000

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO radar_checkpoints (
                        stream_id,
                        last_start_time,
                        last_timestamp_usec,
                        last_continuation,
                        last_run_at,
                        updated_at
                    )
                    VALUES (%s, %s, %s, NULL, NOW(), NOW())

                    ON CONFLICT (stream_id)

                    DO UPDATE SET
                        last_start_time = EXCLUDED.last_start_time,
                        last_timestamp_usec = EXCLUDED.last_timestamp_usec,
                        last_continuation = NULL,
                        last_run_at = NOW(),
                        updated_at = NOW()
                    """,
                    (
                        stream_id,
                        checkpoint_start_time,
                        checkpoint_timestamp_usec,
                    ),
                )

                conn.commit()

        with get_db_connection() as conn:

            with conn.cursor() as cur:

                cur.execute(
                    """
                    UPDATE radar_ingestion_runs
                    SET
                        next_start_time = %s,
                        items_received = %s,
                        items_inserted = %s,
                        items_existing = %s,
                        pages_fetched = %s,
                        continuation = %s,
                        status = %s,
                        finished_at = NOW()

                    WHERE id = %s
                    """,
                    (
                        next_start_time,
                        items_received,
                        items_inserted,
                        items_existing,
                        pages_fetched,
                        continuation,
                        run_status,
                        run_id,
                    ),
                )

            conn.commit()


        return {
            "run_id": run_id,
            "stream_id": stream_id,
            "start_time": start_time,
            "next_start_time": next_start_time,
            "items_received": items_received,
            "items_inserted": items_inserted,
            "items_existing": items_existing,
            "pages_fetched": pages_fetched,
            "continuation": continuation,
            "complete": complete,
            "inoreader_modified": False,
        }


    except Exception as exc:

        with get_db_connection() as conn:

            with conn.cursor() as cur:

                cur.execute(
                    """
                    UPDATE radar_ingestion_runs
                    SET
                        items_received = %s,
                        items_inserted = %s,
                        items_existing = %s,
                        pages_fetched = %s,
                        continuation = %s,
                        status = 'error',
                        finished_at = NOW(),
                        error_message = %s

                    WHERE id = %s
                    """,
                    (
                        items_received,
                        items_inserted,
                        items_existing,
                        pages_fetched,
                        continuation,
                        str(exc),
                        run_id,
                    ),
                )

            conn.commit()

        raise

# Segurança do transporte MCP para o hostname público do Railway.
mcp_app = mcp.streamable_http_app()

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_database()

    async with mcp.session_manager.run():
        yield



app = FastAPI(lifespan=lifespan)

# Endpoint MCP público:
# https://inoreader-intelligence-mcp-production.up.railway.app/mcp
app.mount("/mcp", mcp_app)


@app.get("/")
def health():
    return {
        "status": "online",
        "service": "Inoreader Intelligence MCP",
        "mcp_endpoint": "/mcp",
    }

@app.get("/db/health")
def database_health():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()

        return {
            "status": "ok",
            "database_connected": result == (1,),
        }

    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "database_connected": False,
                "message": str(exc),
            },
        )


@app.get("/config")
def config_check():
    return {
        "client_id": bool(os.getenv("INOREADER_CLIENT_ID")),
        "client_secret": bool(os.getenv("INOREADER_CLIENT_SECRET")),
        "refresh_token": bool(os.getenv("INOREADER_REFRESH_TOKEN")),
    }


@app.get("/token")
def token_check():
    try:
        token = get_access_token()

        return {
            "status": "ok",
            "access_token_received": bool(token),
        }

    except requests.RequestException as exc:
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": str(exc),
            },
        )


@app.get("/oauth/login")
def oauth_login():
    state = secrets.token_urlsafe(32)

    authorization_url = "https://www.inoreader.com/oauth2/auth"

    params = {
        "client_id": os.getenv("INOREADER_CLIENT_ID"),
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": "read",
        "state": state,
    }

    prepared = requests.Request(
        "GET",
        authorization_url,
        params=params,
    ).prepare()

    response = RedirectResponse(prepared.url)

    response.set_cookie(
        key=OAUTH_STATE_COOKIE,
        value=state,
        httponly=True,
        secure=True,
        samesite="lax",
        max_age=600,
    )

    return response


@app.get("/oauth/callback")
def oauth_callback(
    request: Request,
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
    error_description: str | None = None,
):
    if error:
        return JSONResponse(
            status_code=400,
            content={
                "status": "oauth_error",
                "error": error,
                "error_description": error_description,
            },
        )

    expected_state = request.cookies.get(OAUTH_STATE_COOKIE)

    if not state or not expected_state or not secrets.compare_digest(
        state,
        expected_state,
    ):
        return JSONResponse(
            status_code=400,
            content={
                "status": "error",
                "message": "OAuth state validation failed.",
            },
        )

    if not code:
        return JSONResponse(
            status_code=400,
            content={
                "status": "error",
                "message": "Authorization code not received.",
            },
        )

    response = requests.post(
        INOREADER_TOKEN_URL,
        data={
            "code": code,
            "client_id": os.getenv("INOREADER_CLIENT_ID"),
            "client_secret": os.getenv("INOREADER_CLIENT_SECRET"),
            "redirect_uri": REDIRECT_URI,
            "grant_type": "authorization_code",
        },
        timeout=20,
    )

    if response.status_code != 200:
        return JSONResponse(
            status_code=400,
            content={
                "status": "token_exchange_error",
                "status_code": response.status_code,
                "response": response.text,
            },
        )

    token_data = response.json()

    result = JSONResponse(
        content={
            "status": "authorized",
            "access_token_received": bool(
                token_data.get("access_token")
            ),
            "refresh_token_received": bool(
                token_data.get("refresh_token")
            ),
            "message": (
                "OAuth concluído. Configure o refresh token "
                "somente nas variáveis seguras do Railway."
            ),
        }
    )

    result.delete_cookie(OAUTH_STATE_COOKIE)

    return result


@mcp.tool()
def user_info():
    """
    Retorna informações do usuário autenticado no Inoreader.
    Operação exclusivamente de leitura.
    """
    return inoreader_get("/user-info")


@mcp.tool()
def subscription_list():
    """
    Retorna a lista de feeds assinados no Inoreader.
    Operação exclusivamente de leitura.
    """
    return inoreader_get("/subscription/list")

@mcp.tool()
def stream_catalog():
    """
    Retorna catálogo dos feeds assinados no Inoreader.
    Facilita descoberta de fontes pelo agente.
    Operação exclusivamente de leitura.
    """

    data = inoreader_get("/subscription/list")

    subscriptions = data.get("subscriptions", [])

    catalog = []

    for item in subscriptions:
        catalog.append(
            {
                "id": item.get("id"),
                "title": item.get("title"),
                "categories": [
                    category.get("label")
                    for category in item.get("categories", [])
                ],
            }
        )

    return {
        "total_feeds": len(catalog),
        "feeds": catalog,
    }

@mcp.tool()
def stream_contents(
    stream_id: str,
    count: int = 100,
    continuation: str | None = None,
):
    if count < 1:
        count = 1
    if count > 100:
        count = 100

    params = {
        "n": count,
    }

    if continuation:
        params["c"] = continuation

    return inoreader_get(
        f"/stream/contents/{stream_id}",
        params=params,
    )

@mcp.tool()
def archive_stream_sample(
    stream_id: str,
    count: int = 10,
):
    """
    Busca um pequeno lote de itens no Inoreader e os arquiva
    no Radar Archive PostgreSQL.

    Não altera estado de leitura no Inoreader.
    """

    if count < 1:
        count = 1

    if count > 50:
        count = 50

    data = inoreader_get(
        f"/stream/contents/{stream_id}",
        params={"n": count},
    )

    items = data.get("items", [])

    archive_result = archive_articles(items)

    return {
        "stream_id": stream_id,
        "items_received": len(items),
        "items_inserted": archive_result["inserted"],
        "items_existing": archive_result["existing"],
        "inoreader_modified": False,
    }

@mcp.tool()
def archive_stream_paged(
    stream_id: str,
    max_items: int = 3000,
    page_size: int = 100,
    start_time: int | None = None,
    continuation: str | None = None,
):

    """
    Arquiva um stream do Inoreader no PostgreSQL com paginação.

    Não retorna os artigos individualmente.
    Não altera o estado de leitura no Inoreader.
    """

    return ingest_stream_to_archive(
    stream_id=stream_id,
    max_items=max_items,
    page_size=page_size,
    start_time=start_time,
    continuation=continuation,
)


@mcp.tool()
def stream_contents_paged(
    stream_id: str,
    max_items: int = 500,
    page_size: int = 100,
    start_time: int | None = None,
):

    if max_items < 1:
        max_items = 1

    if max_items > 3000:
        max_items = 3000

    if page_size < 1:
        page_size = 1

    if page_size > 100:
        page_size = 100

    items = []
    continuation = None
    pages = 0

    while len(items) < max_items:

        params = {
            "n": min(page_size, max_items - len(items)),
        }

        if start_time:
            params["ot"] = start_time

        if continuation:
            params["c"] = continuation

        data = inoreader_get(
            f"/stream/contents/{stream_id}",
            params=params,
        )

        page_items = data.get("items", [])

        if not page_items:
            break

        items.extend(page_items)
        pages += 1

        continuation = data.get("continuation")

        if not continuation:
            break

    next_start_time = start_time

    if items:
        timestamps = [
            int(item["timestampUsec"])
            for item in items
            if item.get("timestampUsec")
        ]

        if timestamps:
            next_start_time = max(timestamps) // 1_000_000

    return {
        "items": items,
        "items_returned": len(items),
        "pages_fetched": pages,
        "continuation": continuation,
        "start_time": start_time,
        "next_start_time": next_start_time,
    }
