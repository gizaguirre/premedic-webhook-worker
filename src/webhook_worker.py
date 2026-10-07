"""
Worker de webhooks Premedic.

Lee eventos pendientes de dbo.WebhookEvento, los envía por HTTP y registra
el resultado con dbo.WebhookEvento_RegistrarResultado.

Uso:
    python webhook_worker.py           # loop continuo (como servicio)
    python webhook_worker.py --once    # vacía lo pendiente y termina (Task Scheduler / cron)
"""
from __future__ import annotations

import argparse
import logging
import os
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import pyodbc
import requests
from dotenv import load_dotenv

load_dotenv()

SQL_CONN_STR = os.environ["SQL_CONN_STR"]
LOTE = int(os.getenv("WEBHOOK_LOTE", "50"))
HILOS = int(os.getenv("WEBHOOK_HILOS", "8"))
POLL_SEG = float(os.getenv("WEBHOOK_POLL_SEG", "5"))
CONNECT_TIMEOUT = float(os.getenv("WEBHOOK_CONNECT_TIMEOUT", "5"))
MAX_BODY = 2000
MAX_ERROR = 1000

log = logging.getLogger("webhook_worker")
_detener = threading.Event()
_local = threading.local()


@dataclass
class Evento:
    id: int
    evento_id: str
    destino: str
    url: str
    timeout_seg: int
    payload: str


@dataclass
class Resultado:
    evento: Evento
    resultado: str  # OK | REINTENTAR | FALLIDO
    http_status: int | None = None
    duracion_ms: int | None = None
    respuesta: str | None = None
    error: str | None = None
    espera_seg: int | None = None
    respuesta_estado: str | None = None   # enviado | duplicado | sin_destinatarios | ignorado
    destinatarios: int | None = None
    push_enviados: int | None = None
    push_fallidos: int | None = None
    codigo_error: str | None = None       # "code" de la API
    request_id: str | None = None


# ---------------------------------------------------------------- HTTP

def credenciales(destino: str) -> tuple[str, str]:
    """Busca WEBHOOK_<DESTINO>_CLIENT/SECRET y si no, WEBHOOK_CLIENT/SECRET."""
    pref = f"WEBHOOK_{destino.upper().replace('-', '_')}_"
    client = os.getenv(pref + "CLIENT") or os.getenv("WEBHOOK_CLIENT")
    secret = os.getenv(pref + "SECRET") or os.getenv("WEBHOOK_SECRET")
    if not client or not secret:
        raise RuntimeError(f"Credenciales no configuradas para destino '{destino}'")
    # strip() evita saltos de línea o espacios colados en los headers
    return client.strip(), secret.strip()


def sesion_http() -> requests.Session:
    """Una sesión por hilo (reutiliza conexiones TLS)."""
    if not hasattr(_local, "session"):
        _local.session = requests.Session()
    return _local.session


def parse_retry_after(valor: str | None) -> int | None:
    try:
        return max(0, int(valor)) if valor else None
    except ValueError:
        return None


def clasificar(status: int) -> str:
    """Regla de la API: 2xx listo, 4xx no reintentar (salvo 429), 5xx reintentar."""
    if 200 <= status < 300:
        return "OK"
    if status in (408, 429) or status >= 500:
        return "REINTENTAR"
    return "FALLIDO"


def leer_json(r: requests.Response) -> dict:
    try:
        d = r.json()
        return d if isinstance(d, dict) else {}
    except ValueError:
        return {}


def entero(v) -> int | None:
    return v if isinstance(v, int) else None


def enviar(ev: Evento) -> Resultado:
    try:
        client, secret = credenciales(ev.destino)
    except RuntimeError as e:
        log.error(str(e))
        return Resultado(ev, "REINTENTAR", error=str(e))

    headers = {"client": client, "secret": secret, "Content-Type": "application/json"}
    inicio = time.monotonic()
    try:
        r = sesion_http().post(
            ev.url,
            data=ev.payload.encode("utf-8"),  # se envía el JSON tal cual está guardado
            headers=headers,
            timeout=(CONNECT_TIMEOUT, ev.timeout_seg),
        )
    except requests.RequestException as e:
        return Resultado(
            ev, "REINTENTAR",
            duracion_ms=int((time.monotonic() - inicio) * 1000),
            error=f"{type(e).__name__}: {e}"[:MAX_ERROR],
        )

    s = r.status_code
    resultado = clasificar(s)
    cuerpo = leer_json(r)

    espera = None
    if s in (429, 503):
        espera = parse_retry_after(r.headers.get("Retry-After"))
    if s == 429 and espera is None:
        espera = 60  # la API indica reintentar en un minuto

    error = None
    if resultado != "OK":
        detalle = cuerpo.get("detail") or cuerpo.get("title") or ""
        campos = ", ".join(
            f"{e.get('path')}: {e.get('message')}"
            for e in cuerpo.get("errors", []) if isinstance(e, dict)
        )
        error = f"HTTP {s} {cuerpo.get('code', '')} {detalle} {campos}".strip()[:MAX_ERROR]

    return Resultado(
        ev, resultado,
        http_status=s,
        duracion_ms=int((time.monotonic() - inicio) * 1000),
        respuesta=(r.text or None) and r.text[:MAX_BODY],
        error=error,
        espera_seg=espera,
        respuesta_estado=cuerpo.get("estado") if resultado == "OK" else None,
        destinatarios=entero(cuerpo.get("destinatarios")),
        push_enviados=entero(cuerpo.get("enviados")),
        push_fallidos=entero(cuerpo.get("fallidos")),
        codigo_error=cuerpo.get("code"),
        request_id=cuerpo.get("requestId") or r.headers.get("x-request-id"),
    )


# ---------------------------------------------------------------- Base de datos

def conectar() -> pyodbc.Connection:
    return pyodbc.connect(SQL_CONN_STR, autocommit=True)


def reclamar(cn: pyodbc.Connection) -> list[Evento]:
    cur = cn.cursor()
    cur.execute("EXEC dbo.WebhookEvento_Reclamar @Lote = ?", LOTE)
    return [Evento(*fila) for fila in cur.fetchall()]


def registrar(cn: pyodbc.Connection, r: Resultado) -> None:
    cn.cursor().execute(
        "EXEC dbo.WebhookEvento_RegistrarResultado "
        "@Id=?, @Resultado=?, @HttpStatus=?, @DuracionMs=?, @RespuestaEstado=?, "
        "@Destinatarios=?, @PushEnviados=?, @PushFallidos=?, @CodigoError=?, @RequestId=?, "
        "@RespuestaBody=?, @Error=?, @EsperaSeg=?",
        r.evento.id, r.resultado, r.http_status, r.duracion_ms, r.respuesta_estado,
        r.destinatarios, r.push_enviados, r.push_fallidos, r.codigo_error, r.request_id,
        r.respuesta, r.error, r.espera_seg,
    )


# ---------------------------------------------------------------- Loop

def procesar_lote(cn: pyodbc.Connection, pool: ThreadPoolExecutor) -> int:
    eventos = reclamar(cn)
    if not eventos:
        return 0
    for r in pool.map(enviar, eventos):
        registrar(cn, r)
        if r.http_status in (401, 503):
            nivel = logging.ERROR  # credenciales o configuración desalineadas: hay que avisar
        elif r.resultado == "OK":
            nivel = logging.INFO
        else:
            nivel = logging.WARNING
        log.log(
            nivel,
            "%s [%s] -> %s status=%s %s %sms %s %s",
            r.evento.evento_id, r.evento.destino, r.resultado, r.http_status,
            r.respuesta_estado or "", r.duracion_ms, r.error or "",
            f"requestId={r.request_id}" if r.request_id else "",
        )
    return len(eventos)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="procesa lo pendiente y termina")
    args = parser.parse_args()

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: _detener.set())

    log.info("Worker iniciado (lote=%s, hilos=%s, once=%s)", LOTE, HILOS, args.once)
    cn: pyodbc.Connection | None = None

    with ThreadPoolExecutor(max_workers=HILOS) as pool:
        while not _detener.is_set():
            try:
                if cn is None:
                    cn = conectar()
                n = procesar_lote(cn, pool)
            except pyodbc.Error:
                log.exception("Error de base de datos; se reintenta la conexión")
                try:
                    cn and cn.close()
                except pyodbc.Error:
                    pass
                cn = None
                _detener.wait(POLL_SEG)
                continue

            if n < LOTE:
                if args.once:
                    break
                _detener.wait(POLL_SEG)

    if cn:
        cn.close()
    log.info("Worker detenido")


if __name__ == "__main__":
    main()