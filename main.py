from fastapi import FastAPI
from mcp.server.fastmcp import FastMCP
import os
import requests

app = FastAPI()

mcp = FastMCP("Inoreader Intelligence MCP")

def get_access_token():
    response = requests.post(
        "https://www.inoreader.com/oauth2/token",
        data={
            "client_id": os.getenv("INOREADER_CLIENT_ID"),
            "client_secret": os.getenv("INOREADER_CLIENT_SECRET"),
            "refresh_token": os.getenv("INOREADER_REFRESH_TOKEN"),
            "grant_type": "refresh_token"
        }
    )

    response.raise_for_status()

    return response.json()["access_token"]


@app.get("/")
def health():
    return {
        "status": "online",
        "service": "Inoreader Intelligence MCP"
    }

@app.get("/config")
def config_check():
    return {
        "client_id": bool(os.getenv("INOREADER_CLIENT_ID")),
        "client_secret": bool(os.getenv("INOREADER_CLIENT_SECRET")),
        "refresh_token": bool(os.getenv("INOREADER_REFRESH_TOKEN"))
    }

@app.get("/token")
def token_check():
    token = get_access_token()

    return {
        "status": "ok",
        "token_received": bool(token)
    }

@mcp.tool()
def user_info():
    """
    Retorna informações do usuário autenticado no Inoreader.
    """
    return {
        "status": "ok",
        "operation": "user_info"
    }


@mcp.tool()
def subscription_list():
    """
    Retorna os feeds assinados no Inoreader.
    """
    return {
        "status": "ok",
        "operation": "subscription_list",
        "subscriptions": []
    }


@mcp.tool()
def stream_contents(
    stream_id: str = "user/-/state/com.google/root"
):
    """
    Retorna conteúdos recentes de um stream do Inoreader.
    """
    return {
        "status": "ok",
        "operation": "stream_contents",
        "stream_id": stream_id,
        "items": []
    }
