from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, JSONResponse
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
import os
import secrets
import requests


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


# Segurança do transporte MCP para o hostname público do Railway.
mcp_app = mcp.streamable_http_app()



@asynccontextmanager
async def lifespan(app: FastAPI):
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
