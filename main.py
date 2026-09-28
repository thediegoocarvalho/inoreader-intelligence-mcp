from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from mcp.server.fastmcp import FastMCP
import os
import requests


app = FastAPI()

mcp = FastMCP("Inoreader Intelligence MCP")

REDIRECT_URI = (
    "https://inoreader-intelligence-mcp-production.up.railway.app/oauth/callback"
)


def get_access_token():
    response = requests.post(
        "https://www.inoreader.com/oauth2/token",
        data={
            "client_id": os.getenv("INOREADER_CLIENT_ID"),
            "client_secret": os.getenv("INOREADER_CLIENT_SECRET"),
            "refresh_token": os.getenv("INOREADER_REFRESH_TOKEN"),
            "grant_type": "refresh_token",
        },
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
    authorization_url = "https://www.inoreader.com/oauth2/auth"

    params = {
        "client_id": os.getenv("INOREADER_CLIENT_ID"),
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": "read",
    }

    request = requests.Request(
        "GET",
        authorization_url,
        params=params,
    ).prepare()

    return RedirectResponse(request.url)


@app.get("/oauth/callback")
def oauth_callback(code: str):
    response = requests.post(
        "https://www.inoreader.com/oauth2/token",
        data={
            "code": code,
            "client_id": os.getenv("INOREADER_CLIENT_ID"),
            "client_secret": os.getenv("INOREADER_CLIENT_SECRET"),
            "redirect_uri": REDIRECT_URI,
            "grant_type": "authorization_code",
        },
    )

    if response.status_code != 200:
        return {
            "status": "error",
            "status_code": response.status_code,
            "response": response.text,
        }

    token_data = response.json()

    return {
        "status": "authorized",
        "refresh_token_received": bool(token_data.get("refresh_token")),
        "access_token_received": bool(token_data.get("access_token")),
        "message": (
            "OAuth autorizado. Os tokens não são exibidos "
            "por segurança."
        ),
    }


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
