"""
main.py - Punto de entrada del sistema de mensajería
Taller de Sistemas Distribuidos - UIS 2026-1

Este archivo arranca el nodo completo:
  1. Lee la configuración desde variables de entorno (para Docker).
  2. Crea las instancias compartidas: HeartbeatManager, RelojLamport, RelojVectorial.
  3. Registra automáticamente los peers indicados en la variable PEERS.
  4. Lanza el listener TCP en un hilo separado.
  5. Lanza el listener UDP en un hilo separado.
  6. Arranca el sistema de heartbeat.
  7. Inicia el servidor HTTP con uvicorn.

Variable PEERS:
  Formato: "nodo2:5001,nodo3:5001"
  Docker resuelve el nombre del contenedor a su IP interna automáticamente.
  Así no tenemos que conocer las IPs de antemano.
"""

import logging
import os
import threading

import uvicorn

from messaging_node import init_db, tcp_listener, udp_listener, add_peer
from heartbeat      import HeartbeatManager
from lamport_clock  import RelojLamport
from vector_clock   import RelojVectorial


# --- Configuración de logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("main")


# --- Leer configuración desde variables de entorno ---
# Si la variable no existe, se usa el valor por defecto.
MY_HOST   = "0.0.0.0"       # Escucha en todas las interfaces de red
MY_NAME   = os.environ.get("NODE_NAME", "nodo1")
TCP_PORT  = int(os.environ.get("TCP_PORT", "5001"))
UDP_PORT  = int(os.environ.get("UDP_PORT", "5002"))
API_PORT  = int(os.environ.get("API_PORT", "5003"))
HB_PORT   = int(os.environ.get("HB_PORT",  "5004"))   # Puerto exclusivo para heartbeat

# PEERS = lista de otros nodos en formato "nombre:tcp_port,nombre:tcp_port"
_PEERS_RAW = os.environ.get("PEERS", "")


def _parsear_peers() -> list[tuple[str, int]]:
    """
    Convierte el string de PEERS en una lista de tuplas (nombre, tcp_port).

    Ejemplo:
      "nodo2:5001,nodo3:5001"  ->  [("nodo2", 5001), ("nodo3", 5001)]
    """
    if not _PEERS_RAW.strip():
        return []

    resultado = []
    for entrada in _PEERS_RAW.split(","):
        entrada = entrada.strip()
        if not entrada:
            continue
        partes = entrada.split(":")
        nombre = partes[0].strip()
        # Si no viene el puerto, usar TCP_PORT por defecto
        try:
            puerto = int(partes[1]) if len(partes) > 1 else TCP_PORT
        except ValueError:
            puerto = TCP_PORT
        resultado.append((nombre, puerto))
    return resultado


# --- Construir la lista de todos los nodos (yo + peers) ---
# Esto se hace a nivel de módulo para que reloj_vectorial
# esté disponible cuando api_server.py lo importe.
_peer_list = _parsear_peers()
ALL_NODES  = [MY_NAME] + [nombre for nombre, _ in _peer_list if nombre != MY_NAME]


# --- Instancias globales ---
# IMPORTANTE: deben estar aquí (nivel de módulo), NO dentro del bloque
# "if __name__ == '__main__'", porque api_server.py hace "import main"
# y necesita acceder a estos objetos al manejar las peticiones HTTP.

heartbeat_manager = HeartbeatManager(
    mi_nombre=MY_NAME,
    mi_host=MY_HOST,
    puerto_hb=HB_PORT,
)

reloj_lamport   = RelojLamport(nodo_id=MY_NAME)
reloj_vectorial = RelojVectorial(nodo_id=MY_NAME, todos_nodos=ALL_NODES)


# --- Punto de entrada ---
if __name__ == "__main__":

    # Paso 1: Inicializar la base de datos SQLite
    init_db()

    # Paso 2: Registrar peers automáticamente
    # Docker DNS resuelve el nombre del contenedor a su IP interna,
    # por eso usamos el nombre directamente como hostname.
    for nombre, tcp_port in _peer_list:
        heartbeat_manager.registrar_peer(nombre, nombre, HB_PORT)
        add_peer(nombre, nombre, tcp_port, UDP_PORT)
        logger.info("Peer registrado: %s  TCP:%d  UDP:%d  HB:%d",
                    nombre, tcp_port, UDP_PORT, HB_PORT)

    # Paso 3: Arrancar listener TCP en un hilo daemon
    # Los hilos daemon se detienen solos cuando el proceso principal termina.
    hilo_tcp = threading.Thread(
        target=tcp_listener,
        args=(MY_HOST, TCP_PORT),
        daemon=True,
        name="tcp-listener",
    )
    hilo_tcp.start()
    logger.info("TCP listener activo en %s:%d", MY_HOST, TCP_PORT)

    # Paso 4: Arrancar listener UDP en un hilo daemon
    hilo_udp = threading.Thread(
        target=udp_listener,
        args=(MY_HOST, UDP_PORT),
        daemon=True,
        name="udp-listener",
    )
    hilo_udp.start()
    logger.info("UDP listener activo en %s:%d", MY_HOST, UDP_PORT)

    # Paso 5: Arrancar heartbeat (lanza sus propios hilos internamente)
    heartbeat_manager.iniciar()
    logger.info("Heartbeat activo en puerto UDP %d", HB_PORT)

    # Paso 6: Registrar el inicio del nodo en ambos relojes
    reloj_lamport.tick(descripcion="Nodo iniciado")
    reloj_vectorial.tick(descripcion="Nodo iniciado")
    logger.info(
        "Relojes iniciados | Nodo=%s | Lamport L=%d | Vector V=%s",
        MY_NAME,
        reloj_lamport.get_tiempo(),
        reloj_vectorial.get_vector(),
    )

    # Paso 7: Iniciar el servidor HTTP (bloquea hasta que se detenga)
    logger.info("Dashboard: http://0.0.0.0:%d/", API_PORT)
    logger.info("API docs:  http://0.0.0.0:%d/docs", API_PORT)

    uvicorn.run(
        "api_server:app",
        host=MY_HOST,
        port=API_PORT,
        log_level="warning",   # Solo errores de uvicorn; nuestro logger ya muestra info
    )
