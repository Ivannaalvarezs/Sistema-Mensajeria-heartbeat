"""
heartbeat.py - Detección de fallos entre nodos
Taller de Sistemas Distribuidos - UIS 2026-1

Cómo funciona:
  Cada nodo envía un "ping" UDP pequeño a sus vecinos cada 3 segundos.
  Si un vecino no manda ningún ping en 10 segundos → se marca como INACTIVO.

  El HeartbeatManager usa dos hilos:
    - hb-sender  : envía pings a todos los peers cada INTERVALO_PING segundos.
    - hb-listener: recibe pings entrantes y actualiza el tiempo del último ping.

  La función get_estado() compara el tiempo actual con el último ping
  recibido de cada nodo para decidir si está activo o inactivo.
"""

import json
import logging
import socket
import threading
import time
from datetime import datetime

logger = logging.getLogger("heartbeat")

# Cuántos segundos entre cada ping enviado
INTERVALO_PING   = 3.0

# Si un nodo no responde en este tiempo, se marca como inactivo
TIMEOUT_INACTIVO = 10.0


class HeartbeatManager:
    """
    Monitorea si los otros nodos están vivos o no.

    Uso:
      hb = HeartbeatManager("nodo1", "0.0.0.0", 5004)
      hb.registrar_peer("nodo2", "192.168.1.2", 5004)
      hb.iniciar()
      estado = hb.get_estado()   # {"nodo2": {"estado": "activo", ...}}
    """

    def __init__(self, mi_nombre: str, mi_host: str, puerto_hb: int):
        self.mi_nombre  = mi_nombre
        self.mi_host    = mi_host
        self.puerto_hb  = puerto_hb

        # Diccionario de peers: nombre -> {"ip": ..., "puerto": ...}
        self._peers       = {}

        # Último ping recibido de cada peer: nombre -> timestamp (float)
        self._ultimo_ping = {}

        self._lock      = threading.Lock()
        self._corriendo = False

    # =========================================================================
    # GESTIÓN DE PEERS
    # =========================================================================

    def registrar_peer(self, nombre: str, ip: str, puerto: int):
        """
        Agrega un nodo a la lista de monitoreo.
        Se inicializa con tiempo actual para no marcarlo inactivo de entrada.
        """
        with self._lock:
            self._peers[nombre] = {"ip": ip, "puerto": puerto}
            # Solo inicializar si no existe, para no resetear el timer
            if nombre not in self._ultimo_ping:
                self._ultimo_ping[nombre] = time.time()
        logger.info("Heartbeat: peer registrado %s -> %s:%d", nombre, ip, puerto)

    def eliminar_peer(self, nombre: str):
        """Quita un nodo del monitoreo."""
        with self._lock:
            self._peers.pop(nombre, None)
            self._ultimo_ping.pop(nombre, None)
        logger.info("Heartbeat: peer eliminado %s", nombre)

    # =========================================================================
    # CONSULTA DE ESTADO
    # =========================================================================

    def get_estado(self) -> dict:
        """
        Retorna el estado actual de todos los peers monitoreados.

        Ejemplo de respuesta:
          {
            "nodo2": {
              "estado": "activo",
              "ultimo_ping": "2026-05-01T10:00:00.000",
              "segundos_sin_ping": 1.5
            }
          }
        """
        ahora     = time.time()
        resultado = {}

        with self._lock:
            for nombre, ts in self._ultimo_ping.items():
                segundos = ahora - ts
                resultado[nombre] = {
                    "estado": "activo" if segundos < TIMEOUT_INACTIVO else "inactivo",
                    "ultimo_ping": datetime.fromtimestamp(ts).isoformat(timespec="milliseconds"),
                    "segundos_sin_ping": round(segundos, 1),
                }
        return resultado

    # =========================================================================
    # HILO 1: ENVIAR PINGS
    # =========================================================================

    def _enviar_pings(self):
        """
        Hilo que envía un ping UDP a cada peer cada INTERVALO_PING segundos.
        El mensaje es: {"tipo": "heartbeat", "de": "nodo1"}
        """
        sock    = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        mensaje = json.dumps({"tipo": "heartbeat", "de": self.mi_nombre}).encode("utf-8")

        logger.info("Heartbeat SENDER activo (intervalo %.0fs)", INTERVALO_PING)

        while self._corriendo:
            # Sacar copia del diccionario para no bloquear mientras enviamos
            with self._lock:
                peers_actuales = dict(self._peers)

            for nombre, info in peers_actuales.items():
                try:
                    sock.sendto(mensaje, (info["ip"], info["puerto"]))
                    logger.debug("Ping -> %s (%s:%d)", nombre, info["ip"], info["puerto"])
                except OSError as e:
                    logger.warning("Error enviando ping a %s: %s", nombre, e)

            # Revisar si algún nodo lleva mucho tiempo sin responder
            self._revisar_inactivos()

            time.sleep(INTERVALO_PING)

        sock.close()

    # =========================================================================
    # HILO 2: RECIBIR PINGS
    # =========================================================================

    def _escuchar_pings(self):
        """
        Hilo que escucha pings UDP entrantes.
        Por cada ping recibido, actualiza el timestamp del remitente.
        El timeout de 1s permite revisar la bandera _corriendo periódicamente.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.settimeout(1.0)   # Espera máximo 1 segundo por cada recvfrom
        sock.bind((self.mi_host, self.puerto_hb))
        logger.info("Heartbeat LISTENER activo en %s:%d", self.mi_host, self.puerto_hb)

        while self._corriendo:
            try:
                datos, direccion = sock.recvfrom(512)
                mensaje          = json.loads(datos.decode("utf-8"))

                if mensaje.get("tipo") != "heartbeat":
                    continue   # Ignorar mensajes que no sean heartbeats

                remitente = mensaje.get("de", "desconocido")

                with self._lock:
                    self._ultimo_ping[remitente] = time.time()

                logger.debug("Ping <- %s (%s:%d)", remitente, direccion[0], direccion[1])

            except socket.timeout:
                continue   # Normal, volvemos al inicio del bucle
            except json.JSONDecodeError:
                logger.warning("Heartbeat: mensaje con formato inválido recibido")
            except OSError as e:
                if self._corriendo:
                    logger.error("Heartbeat listener error: %s", e)
                break

        sock.close()

    # =========================================================================
    # REVISAR NODOS CAÍDOS
    # =========================================================================

    def _revisar_inactivos(self):
        """Loguea los nodos que llevan más de TIMEOUT_INACTIVO segundos sin ping."""
        ahora = time.time()
        with self._lock:
            for nombre, ts in self._ultimo_ping.items():
                if ahora - ts >= TIMEOUT_INACTIVO:
                    logger.warning(
                        "NODO INACTIVO: '%s' lleva %.1f segundos sin responder",
                        nombre, ahora - ts
                    )

    # =========================================================================
    # CICLO DE VIDA
    # =========================================================================

    def iniciar(self):
        """Arranca los dos hilos daemon del heartbeat."""
        if self._corriendo:
            return   # Ya está corriendo, no arrancar de nuevo

        self._corriendo = True

        threading.Thread(target=self._escuchar_pings, daemon=True, name="hb-listener").start()
        threading.Thread(target=self._enviar_pings,   daemon=True, name="hb-sender").start()

        logger.info("HeartbeatManager iniciado como '%s'", self.mi_nombre)

    def detener(self):
        """Señaliza a los hilos que deben parar en su próxima iteración."""
        self._corriendo = False
        logger.info("HeartbeatManager detenido")
