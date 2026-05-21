"""
messaging_node.py - Núcleo de comunicación del nodo
Taller de Sistemas Distribuidos - UIS 2026-1

Qué hace este archivo:
  - Define dónde se guarda la base de datos SQLite (mensajes.db).
  - Crea las tablas si no existen (mensajes, peers, historial de relojes).
  - Implementa las funciones para guardar y leer mensajes.
  - Implementa las funciones para registrar y leer peers (otros nodos).
  - Implementa los listeners TCP y UDP (reciben mensajes entrantes).
  - Implementa las funciones para enviar mensajes TCP y UDP.
  - Implementa el broadcast (enviar a todos los peers a la vez).

Sobre la base de datos:
  En Docker, los datos se guardan en /app/data/ que es el punto de montaje
  del volumen. Así los mensajes sobreviven aunque el contenedor se reinicie.
  Si se corre fuera de Docker y no hay variable DATA_DIR, también usa /app/data.
"""

import json
import logging
import os
import socket
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional


# --- Logger de este módulo ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("messaging_node")


# --- Ruta de la base de datos ---
# DATA_DIR se puede sobreescribir con variable de entorno.
# Por defecto apunta a /app/data (el volumen de Docker).
_DATA_DIR = Path(os.environ.get("DATA_DIR", "/app/data"))
_DATA_DIR.mkdir(parents=True, exist_ok=True)   # Crear carpeta si no existe
DB_PATH: Path = _DATA_DIR / "mensajes.db"

# Lock para que varios hilos no escriban en SQLite al mismo tiempo
_db_lock = threading.Lock()


# =============================================================================
# CONEXIÓN A LA BASE DE DATOS
# =============================================================================

def _get_conn() -> sqlite3.Connection:
    """Abre y retorna una conexión a SQLite. Cada llamada abre una nueva."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row   # Las filas se leen como diccionarios
    return conn


# =============================================================================
# INICIALIZACIÓN DE TABLAS
# =============================================================================

def init_db() -> None:
    """
    Crea todas las tablas necesarias si todavía no existen.
    Se llama una vez al arrancar el nodo.

    Tablas:
      messages         : mensajes recibidos por este nodo.
      peers            : otros nodos conocidos (IP, puertos).
      historial_lamport: cada evento del reloj de Lamport.
      historial_vector : cada evento del reloj vectorial.
    """
    with _db_lock:
        conn = _get_conn()
        try:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS messages (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    received_at TEXT    NOT NULL,        -- hora en que llegó el mensaje
                    sent_at     TEXT,                   -- hora en que el emisor lo envió
                    sender      TEXT    NOT NULL,        -- nombre del remitente
                    content     TEXT    NOT NULL,        -- texto del mensaje
                    protocol    TEXT    NOT NULL CHECK(protocol IN ('tcp','udp')),
                    lamport_ts  INTEGER                  -- reloj de Lamport del emisor
                );

                CREATE TABLE IF NOT EXISTS peers (
                    name     TEXT PRIMARY KEY,
                    ip       TEXT NOT NULL,
                    tcp_port INTEGER NOT NULL,
                    udp_port INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS historial_lamport (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    seq         INTEGER NOT NULL,        -- número de evento
                    tipo        TEXT    NOT NULL,        -- 'interno', 'envio' o 'recepcion'
                    tiempo_l    INTEGER NOT NULL,        -- valor de L después del evento
                    nodo_id     TEXT    NOT NULL,
                    hora_real   TEXT    NOT NULL,
                    nodo_remoto TEXT,                   -- con quién se comunicó (puede ser NULL)
                    ts_remoto   INTEGER,                -- L del nodo remoto (en recepciones)
                    descripcion TEXT
                );

                CREATE TABLE IF NOT EXISTS historial_vector (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    seq            INTEGER NOT NULL,
                    tipo           TEXT    NOT NULL,
                    vector         TEXT    NOT NULL,    -- JSON del vector completo
                    nodo_id        TEXT    NOT NULL,
                    hora_real      TEXT    NOT NULL,
                    nodo_remoto    TEXT,
                    vector_remoto  TEXT,               -- JSON del vector recibido (en recepciones)
                    descripcion    TEXT
                );
            """)
            conn.commit()
        finally:
            conn.close()

    logger.info("Base de datos lista en %s", DB_PATH)


# =============================================================================
# MENSAJES
# =============================================================================

def save_message(sender: str, content: str, protocol: str,
                 sent_at: Optional[str] = None,
                 lamport_ts: Optional[int] = None) -> None:
    """
    Guarda un mensaje recibido en la base de datos.

    Parámetros:
      sender    : nombre del nodo que envió el mensaje.
      content   : texto del mensaje.
      protocol  : 'tcp' o 'udp'.
      sent_at   : hora ISO en que el emisor lo envió (puede ser None).
      lamport_ts: valor del reloj de Lamport del emisor (puede ser None).
    """
    received_at = datetime.now().isoformat(timespec="milliseconds")

    with _db_lock:
        conn = _get_conn()
        try:
            conn.execute(
                "INSERT INTO messages "
                "(received_at, sent_at, sender, content, protocol, lamport_ts) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (received_at, sent_at, sender, content, protocol, lamport_ts),
            )
            conn.commit()
        finally:
            conn.close()

    logger.info(
        "Mensaje guardado | de='%s' | proto=%s | enviado=%s | recibido=%s | L=%s",
        sender, protocol.upper(), sent_at or "?", received_at, lamport_ts
    )


def get_messages(protocol: Optional[str] = None,
                 sender: Optional[str] = None) -> list[dict]:
    """
    Recupera mensajes con filtros opcionales.
    Retorna la lista del más reciente al más antiguo.
    """
    query  = "SELECT * FROM messages WHERE 1=1"
    params = []

    if protocol:
        query += " AND protocol = ?"
        params.append(protocol.lower())
    if sender:
        query += " AND sender = ?"
        params.append(sender)

    query += " ORDER BY id DESC"

    with _db_lock:
        conn = _get_conn()
        try:
            rows = conn.execute(query, params).fetchall()
        finally:
            conn.close()

    return [dict(row) for row in rows]


# =============================================================================
# PEERS
# =============================================================================

def add_peer(name: str, ip: str, tcp_port: int, udp_port: int) -> None:
    """
    Registra un peer (otro nodo). Si ya existe, actualiza sus datos.
    Esto permite que los peers cambien de IP sin problema.
    """
    with _db_lock:
        conn = _get_conn()
        try:
            conn.execute(
                "INSERT INTO peers (name, ip, tcp_port, udp_port) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT(name) DO UPDATE SET "
                "ip=excluded.ip, tcp_port=excluded.tcp_port, udp_port=excluded.udp_port",
                (name, ip, tcp_port, udp_port),
            )
            conn.commit()
        finally:
            conn.close()
    logger.info("Peer registrado: %s -> %s (TCP:%d UDP:%d)", name, ip, tcp_port, udp_port)


def remove_peer(name: str) -> bool:
    """Elimina un peer. Retorna True si existía, False si no."""
    with _db_lock:
        conn = _get_conn()
        try:
            cur = conn.execute("DELETE FROM peers WHERE name = ?", (name,))
            conn.commit()
            deleted = cur.rowcount > 0
        finally:
            conn.close()
    if deleted:
        logger.info("Peer eliminado: %s", name)
    return deleted


def get_peers() -> list[dict]:
    """Retorna la lista de todos los peers registrados."""
    with _db_lock:
        conn = _get_conn()
        try:
            rows = conn.execute("SELECT * FROM peers ORDER BY name").fetchall()
        finally:
            conn.close()
    return [dict(row) for row in rows]


# =============================================================================
# HISTORIAL DEL RELOJ DE LAMPORT
# =============================================================================

def guardar_evento_lamport(evento: dict) -> None:
    """Guarda un evento del reloj de Lamport en SQLite."""
    with _db_lock:
        conn = _get_conn()
        try:
            conn.execute(
                "INSERT INTO historial_lamport "
                "(seq, tipo, tiempo_l, nodo_id, hora_real, nodo_remoto, ts_remoto, descripcion) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    evento.get("seq"),
                    evento.get("tipo"),
                    evento.get("tiempo_l"),
                    evento.get("nodo_id"),
                    evento.get("hora_real"),
                    evento.get("nodo_remoto"),
                    evento.get("ts_remoto"),
                    evento.get("descripcion", ""),
                ),
            )
            conn.commit()
        finally:
            conn.close()


def get_historial_lamport(nodo_id: Optional[str] = None) -> list[dict]:
    """Recupera el historial del reloj de Lamport desde SQLite."""
    query  = "SELECT * FROM historial_lamport WHERE 1=1"
    params = []
    if nodo_id:
        query += " AND nodo_id = ?"
        params.append(nodo_id)
    query += " ORDER BY seq ASC"

    with _db_lock:
        conn = _get_conn()
        try:
            rows = conn.execute(query, params).fetchall()
        finally:
            conn.close()

    return [dict(row) for row in rows]


# =============================================================================
# HISTORIAL DEL RELOJ VECTORIAL
# =============================================================================

def guardar_evento_vector(evento: dict) -> None:
    """
    Guarda un evento del reloj vectorial en SQLite.
    Los campos 'vector' y 'vector_remoto' se serializan a JSON string.
    """
    with _db_lock:
        conn = _get_conn()
        try:
            conn.execute(
                "INSERT INTO historial_vector "
                "(seq, tipo, vector, nodo_id, hora_real, nodo_remoto, vector_remoto, descripcion) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    evento.get("seq"),
                    evento.get("tipo"),
                    json.dumps(evento.get("vector", {})),
                    evento.get("nodo_id"),
                    evento.get("hora_real"),
                    evento.get("nodo_remoto"),
                    json.dumps(evento.get("vector_remoto")) if evento.get("vector_remoto") else None,
                    evento.get("descripcion", ""),
                ),
            )
            conn.commit()
        finally:
            conn.close()


def get_historial_vector(nodo_id: Optional[str] = None) -> list[dict]:
    """
    Recupera el historial del reloj vectorial desde SQLite.
    Deserializa los campos JSON de vuelta a diccionarios Python.
    """
    query  = "SELECT * FROM historial_vector WHERE 1=1"
    params = []
    if nodo_id:
        query += " AND nodo_id = ?"
        params.append(nodo_id)
    query += " ORDER BY seq ASC"

    with _db_lock:
        conn = _get_conn()
        try:
            rows = conn.execute(query, params).fetchall()
        finally:
            conn.close()

    result = []
    for row in rows:
        d = dict(row)
        d["vector"]        = json.loads(d["vector"]) if d.get("vector") else {}
        d["vector_remoto"] = json.loads(d["vector_remoto"]) if d.get("vector_remoto") else None
        result.append(d)
    return result


# =============================================================================
# TCP - RECIBIR MENSAJES
# =============================================================================

def _handle_tcp_client(conn: socket.socket, addr: tuple) -> None:
    """
    Maneja una conexión TCP entrante en su propio hilo.
    Espera un JSON con: sender, content, sent_at (opcional), lamport_ts (opcional).
    Responde con un ACK: {"status": "ok"}.
    """
    with conn:
        try:
            raw  = conn.recv(4096)
            if not raw:
                return
            data = json.loads(raw.decode("utf-8"))

            sender     = data.get("sender", "desconocido")
            content    = data.get("content", "")
            sent_at    = data.get("sent_at",    None)
            lamport_ts = data.get("lamport_ts", None)

            save_message(sender, content, "tcp", sent_at, lamport_ts)

            ack = json.dumps({"status": "ok", "received": len(raw)})
            conn.sendall(ack.encode("utf-8"))
            logger.info("TCP <- %s:%d | de='%s' | L=%s", addr[0], addr[1], sender, lamport_ts)

        except (json.JSONDecodeError, KeyError) as exc:
            logger.warning("TCP: payload inválido de %s - %s", addr, exc)
            conn.sendall(b'{"status":"error","detail":"invalid json"}')
        except OSError as exc:
            logger.error("TCP: error de socket con %s - %s", addr, exc)


def tcp_listener(host: str, port: int) -> None:
    """
    Servidor TCP. Acepta conexiones en bucle y lanza un hilo por cada cliente.
    Escucha en host:port indefinidamente.
    """
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen(10)
    logger.info("TCP listener activo en %s:%d", host, port)

    while True:
        try:
            conn, addr = srv.accept()
            t = threading.Thread(
                target=_handle_tcp_client,
                args=(conn, addr),
                daemon=True,
            )
            t.start()
        except OSError as exc:
            logger.error("TCP listener: error al aceptar conexión - %s", exc)
            break


# =============================================================================
# TCP - ENVIAR MENSAJES
# =============================================================================

def send_tcp_message(peer_ip: str, peer_port: int,
                     sender_name: str, message: str,
                     timeout: float = 5.0,
                     lamport_ts: Optional[int] = None) -> bool:
    """
    Envía un mensaje TCP a otro nodo.
    Incluye la hora de envío y el timestamp de Lamport en el payload.
    Retorna True si recibió ACK, False si hubo algún error.
    """
    payload = json.dumps({
        "sender":     sender_name,
        "content":    message,
        "sent_at":    datetime.now().isoformat(timespec="milliseconds"),
        "lamport_ts": lamport_ts,
    }).encode("utf-8")

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            s.connect((peer_ip, peer_port))
            s.sendall(payload)
            ack_raw = s.recv(1024)
            ack     = json.loads(ack_raw.decode("utf-8"))
            logger.info("TCP -> %s:%d | ACK=%s", peer_ip, peer_port, ack)
            return ack.get("status") == "ok"
    except ConnectionRefusedError:
        logger.warning("TCP -> %s:%d | Conexión rechazada", peer_ip, peer_port)
    except TimeoutError:
        logger.warning("TCP -> %s:%d | Timeout (%.1fs)", peer_ip, peer_port, timeout)
    except (OSError, json.JSONDecodeError) as exc:
        logger.error("TCP -> %s:%d | Error: %s", peer_ip, peer_port, exc)
    return False


# =============================================================================
# UDP - RECIBIR MENSAJES
# =============================================================================

def udp_listener(host: str, port: int) -> None:
    """
    Servidor UDP. Escucha datagramas en bucle indefinidamente.
    UDP no tiene conexión, así que todo llega en el mismo socket.
    """
    srv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    logger.info("UDP listener activo en %s:%d", host, port)

    while True:
        try:
            raw, addr = srv.recvfrom(4096)
            data = json.loads(raw.decode("utf-8"))

            sender     = data.get("sender", "desconocido")
            content    = data.get("content", "")
            sent_at    = data.get("sent_at",    None)
            lamport_ts = data.get("lamport_ts", None)

            save_message(sender, content, "udp", sent_at, lamport_ts)
            logger.info("UDP <- %s:%d | de='%s' | L=%s", addr[0], addr[1], sender, lamport_ts)

        except json.JSONDecodeError as exc:
            logger.warning("UDP: JSON inválido de %s - %s", addr, exc)
        except OSError as exc:
            logger.error("UDP listener: error de socket - %s", exc)
            break


# =============================================================================
# UDP - ENVIAR MENSAJES
# =============================================================================

def send_udp_message(peer_ip: str, peer_port: int,
                     sender_name: str, message: str,
                     lamport_ts: Optional[int] = None) -> None:
    """
    Envía un mensaje UDP a otro nodo.
    UDP no espera ACK (fire and forget).
    """
    payload = json.dumps({
        "sender":     sender_name,
        "content":    message,
        "sent_at":    datetime.now().isoformat(timespec="milliseconds"),
        "lamport_ts": lamport_ts,
    }).encode("utf-8")

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.sendto(payload, (peer_ip, peer_port))
            logger.info("UDP -> %s:%d | %d bytes", peer_ip, peer_port, len(payload))
    except OSError as exc:
        logger.error("UDP -> %s:%d | Error: %s", peer_ip, peer_port, exc)


# =============================================================================
# BROADCAST
# =============================================================================

def broadcast_message(sender_name: str, message: str,
                      protocol: str = "tcp",
                      lamport_ts: Optional[int] = None) -> dict:
    """
    Envía el mismo mensaje a todos los peers registrados.
    Retorna un dict con el resultado de cada envío: {"nodo2": True, "nodo3": False, ...}
    """
    peers   = get_peers()
    results = {}

    for peer in peers:
        name = peer["name"]
        ip   = peer["ip"]

        if protocol == "tcp":
            ok = send_tcp_message(ip, peer["tcp_port"], sender_name, message, lamport_ts=lamport_ts)
        else:
            send_udp_message(ip, peer["udp_port"], sender_name, message, lamport_ts=lamport_ts)
            ok = True   # UDP no da ACK, asumimos que llegó

        results[name] = ok
        logger.info("Broadcast [%s] -> %s: %s", protocol.upper(), name, "OK" if ok else "FALLO")

    return results
