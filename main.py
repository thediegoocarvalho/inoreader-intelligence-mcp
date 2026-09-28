from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse, JSONResponse
from mcp.server.fastmcp import FastMCP
import os
import secrets
import requests


app = FastAPI()

mcp = FastMCP("Inoreader Intelligence MCP")

REDIRECT_URI = (
    "https://inoreader-intelligence-mcp-production.up.railway.app/oauth/callback"
)

OAUTH_STATE_COOKIE = "inoreader_oauth_state"


def get_access_token():
    response = requests.post(
        "https://www.inoreader.com/oauth2/token",
        data={
            "client_id": os.getenv("INOREADER_CLIENT_ID"),
            "client_secret": os.getenv("INOREADER_CLIENT_SECRET"),
            "refresh_token": os.getenv("INOREADER_REFRESH_TOKEN"),
            "grant_type": "refresh_token",
        },
        timeout=20,
    )

    return {
        "status_code": response.status_code,
        "response": response.text,
    }


@app.get("/")
def health():
    return {
        "status": "online",
        "service": "Inoreader Intelligence MCP",
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
    return get_access_token()


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
        "https://www.inoreader.com/oauth2/token",
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
        "refresh_token": token_data.get("refresh_token"),
        "message": (
            "Copie o refresh_token diretamente para a variável "
            "INOREADER_REFRESH_TOKEN no Railway. "
            "Não compartilhe este valor."
        ),
    }
)

    result.delete_cookie(OAUTH_STATE_COOKIE)

    return result


@mcp.tool()
def user_info():
    """
    Retorna informações do usuário autenticado no Inoreader.
    """
    return {
        "status": "ok",
        "operation": "user_info",
    }


@mcp.tool()
def subscription_list():
    """
    Retorna os feeds assinados no Inoreader.
    """
    return {
        "status": "ok",
        "operation": "subscription_list",
        "subscriptions": [],
    }
