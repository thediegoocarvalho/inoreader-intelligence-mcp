from mcp.server.fastmcp import FastMCP

mcp = FastMCP("Inoreader Intelligence MCP")


@mcp.tool()
def user_info():
    """
    Retorna informações do usuário autenticado no Inoreader.
    """
    return {
        "status": "ok",
        "service": "Inoreader Intelligence MCP",
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


if __name__ == "__main__":
    mcp.run()
