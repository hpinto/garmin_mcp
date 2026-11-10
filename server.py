"""
Garmin MCP Multi-Tenant Gateway (Streamable HTTP)

- Cada usuario tiene su URL: https://www.enduralab.org/garmin_mcp/<token>/mcp
- Modo stateless: sin sesiones en memoria, un solo Server para todos.
- El token de la URL se resuelve a la Garmin key del usuario y se pasa mediante un ContextVar.
"""
import asyncio
import contextvars
import json
import re
import sqlite3
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import uvicorn
from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse
from mcp.server import Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp.types import TextContent, Tool
from starlette.middleware.cors import CORSMiddleware

# --------------------------------------------------------------------------
# Configuración
# --------------------------------------------------------------------------
BASE_URL = "https://www.enduralab.org"
DB_PATH = str(Path(__file__).resolve().parent / "garmin_mcp.db")

TOKEN_PATH_RE = re.compile(
    r"^/garmin_mcp/([0-9a-fA-F-]{36})/(?:mcp|sse)/?$"
)

# Garmin key del usuario de la petición en curso
garmin_key_ctx: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "garmin_key", default=None
)

GARMIN_API_BASE = "https://personal.connectapi.garmin.com"


# --------------------------------------------------------------------------
# Base de datos
# --------------------------------------------------------------------------
def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                mcp_token TEXT PRIMARY KEY,
                name TEXT,
                email TEXT,
                garmin_key TEXT
            )
            """
        )


def _lookup_key(token: str) -> str | None:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT garmin_key FROM users WHERE mcp_token = ?", (token,)
        ).fetchone()
    return row[0] if row else None


async def verify_token_in_db(token: str) -> str | None:
    return await asyncio.to_thread(_lookup_key, token)


init_db()


# --------------------------------------------------------------------------
# Servidor MCP y Definición de Herramientas (Pasarela Completa)
# --------------------------------------------------------------------------
mcp_server = Server("garmin-mcp")


@mcp_server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        # --- ACTIVIDADES ---
        Tool(
            name="get_activity_summary",
            description="Obtiene el resumen detallado (SummaryDTO) de una actividad específica[cite: 4, 9].",
            inputSchema={
                "type": "object",
                "properties": {"activity_id": {"type": "string"}},
                "required": ["activity_id"],
            },
        ),
        Tool(
            name="list_activities",
            description="Lista las actividades físicas del usuario en un rango de fechas (YYYY-MM-DD).",
            inputSchema={
                "type": "object",
                "properties": {
                    "start_date": {"type": "string"},
                    "end_date": {"type": "string"},
                },
                "required": ["start_date", "end_date"],
            },
        ),
        # --- SALUD Y BIENESTAR ---
        Tool(
            name="get_garmin_health_summary",
            description="Obtiene el resumen diario de métricas de salud (Dailies)[cite: 55].",
            inputSchema={
                "type": "object",
                "properties": {
                    "date_start_epoch": {
                        "type": "integer",
                        "description": "Timestamp de inicio en epoch (segundos).",
                    }
                },
                "required": ["date_start_epoch"],
            },
        ),
        Tool(
            name="get_sleep_summary",
            description="Obtiene los datos detallados de sueño (DailySleepDTO)[cite: 50, 51].",
            inputSchema={
                "type": "object",
                "properties": {"calendar_date": {"type": "string", "description": "Formato YYYY-MM-DD"}},
                "required": ["calendar_date"],
            },
        ),
        Tool(
            name="get_hrv_summary",
            description="Obtiene el resumen de Variabilidad de la Frecuencia Cardíaca (HRV) nocturna[cite: 42].",
            inputSchema={
                "type": "object",
                "properties": {"calendar_date": {"type": "string", "description": "Formato YYYY-MM-DD"}},
                "required": ["calendar_date"],
            },
        ),
        Tool(
            name="get_body_battery",
            description="Obtiene las métricas de carga y drenaje de Body Battery[cite: 61].",
            inputSchema={
                "type": "object",
                "properties": {"calendar_date": {"type": "string", "description": "Formato YYYY-MM-DD"}},
                "required": ["calendar_date"],
            },
        ),
        # --- PERFIL Y CONFIGURACIÓN ---
        Tool(
            name="get_user_profile",
            description="Obtiene las configuraciones y perfil general del usuario (UserSettingsDTO)[cite: 53].",
            inputSchema={"type": "object", "properties": {}},
        ),
    ]


@mcp_server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    garmin_key = garmin_key_ctx.get()
    if not garmin_key:
        return [TextContent(type="text", text='{"error": "Usuario no identificado"}')]

    headers = {"Authorization": f"Bearer {garmin_key}", "Accept": "application/json"}
    
    endpoint_map = {
        "get_activity_summary": f"/wellness-api/rest/activities/{arguments.get('activity_id')}",
        "list_activities": "/wellness-api/rest/activities",
        "get_garmin_health_summary": "/wellness-api/rest/dailies",
        "get_sleep_summary": f"/wellness-api/rest/sleeps/{arguments.get('calendar_date')}",
        "get_hrv_summary": f"/wellness-api/rest/hrv/{arguments.get('calendar_date')}",
        "get_body_battery": f"/wellness-api/rest/bodyBattery/{arguments.get('calendar_date')}",
        "get_user_profile": "/wellness-api/rest/user/settings",
    }

    if name not in endpoint_map:
        return [TextContent(type="text", text='{"error": "Herramienta no encontrada"}')]

    url = f"{GARMIN_API_BASE}{endpoint_map[name]}"
    
    # Mapeo de parámetros específicos según la herramienta
    params = {}
    if name == "list_activities":
        params = {"startDate": arguments.get("start_date"), "endDate": arguments.get("end_date")}
    elif name == "get_garmin_health_summary":
        try:
            params = {"uploadStartTimeInSeconds": int(arguments.get("date_start_epoch"))}
        except (TypeError, ValueError):
            return [TextContent(type="text", text='{"error": "date_start_epoch inválido"}')]

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            resp = await client.get(url, params=params, headers=headers)
            resp.raise_for_status()
            return [TextContent(type="text", text=json.dumps(resp.json(), indent=2))]
        except Exception as e:
            return [TextContent(type="text", text=json.dumps({"error": str(e)}))]


session_manager = StreamableHTTPSessionManager(
    app=mcp_server,
    json_response=True,
    stateless=True,
)


# --------------------------------------------------------------------------
# FastAPI (UI de registro) + Ciclo de vida
# --------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(_: FastAPI):
    async with session_manager.run():
        yield


app = FastAPI(title="Garmin MCP Multi-Tenant Gateway", lifespan=lifespan)


@app.get("/garmin_ui", response_class=HTMLResponse)
async def landing_page():
    return HTMLResponse(
        content="""
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Garmin MCP Multi-Usuario</title>
        <style>
            body { font-family: system-ui; background: #f4f4f5; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }
            .container { background: white; padding: 2rem; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); width: 100%; max-width: 400px; }
            input, button { width: 100%; padding: 0.75rem; margin-bottom: 1rem; border-radius: 4px; border: 1px solid #e4e4e7; box-sizing: border-box; }
            button { background: #2563eb; color: white; border: none; font-weight: bold; cursor: pointer; }
        </style>
    </head>
    <body>
        <div class="container">
            <h2 style="text-align:center; margin-top:0;">Registro Garmin MCP</h2>
            <form action="/garmin_ui/register" method="post">
                <input type="text" name="name" placeholder="Nombre completo" required>
                <input type="email" name="email" placeholder="Correo electrónico" required>
                <input type="password" name="garmin_key" placeholder="Garmin Personal API Key" required>
                <button type="submit">Generar URL de Conexión</button>
            </form>
        </div>
    </body>
    </html>
    """
    )


def _insert_user(token: str, name: str, email: str, garmin_key: str) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO users (mcp_token, name, email, garmin_key) VALUES (?, ?, ?, ?)",
            (token, name, email, garmin_key),
        )


@app.post("/garmin_ui/register")
async def register(
    name: str = Form(...),
    email: str = Form(...),
    garmin_key: str = Form(...),
):
    mcp_token = str(uuid.uuid4())
    await asyncio.to_thread(_insert_user, mcp_token, name, email, garmin_key)

    return HTMLResponse(
        content=f"""
    <div style="font-family: system-ui; padding: 2rem; text-align:center;">
        <h2>Registro Exitoso</h2>
        <p>Añade esta URL como conector personalizado en Claude:</p>
        <div style="background: #e0f2fe; padding: 1rem; font-family: monospace; font-size: 1.0rem; border-radius: 4px; display:inline-block; user-select: all;">
            {BASE_URL}/garmin_mcp/{mcp_token}/mcp
        </div>
        <p style="color:#b91c1c;">Guarda esta URL: es tu clave de acceso. No la compartas.</p>
    </div>
    """
    )


# --------------------------------------------------------------------------
# Gateway ASGI
# --------------------------------------------------------------------------
async def _send_json(send, status: int, body: dict) -> None:
    payload = json.dumps(body).encode()
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(payload)).encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": payload})


class MCPGateway:
    def __init__(self, fastapi_app):
        self.fastapi_app = fastapi_app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            m = TOKEN_PATH_RE.match(scope["path"])
            if m:
                token = m.group(1).lower()
                garmin_key = await verify_token_in_db(token)
                if not garmin_key:
                    await _send_json(send, 401, {"error": "Token inválido"})
                    return

                ctx_token = garmin_key_ctx.set(garmin_key)
                try:
                    await session_manager.handle_request(scope, receive, send)
                finally:
                    garmin_key_ctx.reset(ctx_token)
                return

        await self.fastapi_app(scope, receive, send)


final_app = CORSMiddleware(
    app=MCPGateway(app),
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Mcp-Session-Id"],
)


if __name__ == "__main__":
    uvicorn.run(
        final_app,
        host="127.0.0.1",
        port=8000,
        proxy_headers=True,
        forwarded_allow_ips="*",
    )