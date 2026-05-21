"""
lamport_clock.py - Reloj de Lamport con persistencia en SQLite
Taller de Sistemas Distribuidos - UIS 2026-1

Qué es un reloj de Lamport:
  Es un contador entero que permite ordenar eventos en un sistema distribuido.
  No mide tiempo real, solo establece un orden relativo entre eventos.

Reglas:
  - Evento interno  : L = L + 1
  - Enviar mensaje  : L = L + 1  (el valor se incluye en el mensaje)
  - Recibir mensaje : L = max(L_local, L_recibido) + 1

Persistencia:
  Cada evento se guarda en SQLite. Al reiniciar el nodo, se restaura
  el último valor de L para no empezar desde 0.
"""

import logging
import threading
from datetime import datetime

logger = logging.getLogger("lamport_clock")


class RelojLamport:
    """
    Implementación del reloj de Lamport con persistencia automática en SQLite.
    Thread-safe: usa un lock para que varios hilos no modifiquen L al mismo tiempo.
    """

    def __init__(self, nodo_id: str):
        self.nodo_id    = nodo_id
        self._tiempo    = 0          # Valor actual del reloj
        self._lock      = threading.Lock()
        self._historial = []         # Copia en memoria del historial
        self._cargar_desde_db()

    # =========================================================================
    # CARGA INICIAL DESDE SQLITE
    # =========================================================================

    def _cargar_desde_db(self):
        """
        Al iniciar, recupera el historial previo de SQLite y restaura
        el último valor de L. Así el reloj no empieza desde 0 al reiniciar.
        """
        try:
            from messaging_node import get_historial_lamport
            eventos = get_historial_lamport(nodo_id=self.nodo_id)
            if eventos:
                self._historial = eventos
                self._tiempo    = eventos[-1]["tiempo_l"]   # Último L guardado
                logger.info("[%s] Lamport restaurado: %d eventos, L=%d",
                            self.nodo_id, len(eventos), self._tiempo)
            else:
                logger.info("[%s] Lamport iniciando en L=0", self.nodo_id)
        except Exception as e:
            logger.warning("[%s] No se pudo cargar historial Lamport: %s", self.nodo_id, e)

    # =========================================================================
    # GUARDAR EVENTO EN SQLITE
    # =========================================================================

    def _persistir_evento(self, evento: dict):
        """Guarda un evento en la tabla historial_lamport de SQLite."""
        try:
            from messaging_node import guardar_evento_lamport
            guardar_evento_lamport(evento)
        except Exception as e:
            logger.warning("[%s] No se pudo guardar evento Lamport: %s", self.nodo_id, e)

    # =========================================================================
    # REGLA 1: EVENTO INTERNO
    # =========================================================================

    def tick(self, descripcion: str = "evento interno") -> int:
        """
        Llamar cuando ocurre algo en este nodo que no involucra comunicación.
        Regla: L = L + 1
        Retorna el nuevo valor de L.
        """
        with self._lock:
            self._tiempo += 1
            valor_actual  = self._tiempo
            evento = self._construir_evento("interno", valor_actual, descripcion=descripcion)
            self._historial.append(evento)

        self._persistir_evento(evento)
        logger.info("[%s] Tick interno -> L=%d", self.nodo_id, valor_actual)
        return valor_actual

    # =========================================================================
    # REGLA 2: EVENTO DE ENVÍO
    # =========================================================================

    def evento_envio(self, destino: str = "?", descripcion: str = "") -> int:
        """
        Llamar justo antes de enviar un mensaje.
        Regla: L = L + 1
        El valor retornado debe incluirse en el mensaje para que el receptor lo use.
        """
        with self._lock:
            self._tiempo += 1
            valor_actual  = self._tiempo
            evento = self._construir_evento("envio", valor_actual,
                                            nodo_remoto=destino, descripcion=descripcion)
            self._historial.append(evento)

        self._persistir_evento(evento)
        logger.info("[%s] Envío -> %s | L=%d", self.nodo_id, destino, valor_actual)
        return valor_actual

    # =========================================================================
    # REGLA 3: EVENTO DE RECEPCIÓN
    # =========================================================================

    def evento_recepcion(self, ts_remoto: int,
                         remitente: str = "?",
                         descripcion: str = "") -> int:
        """
        Llamar al recibir un mensaje con su timestamp de Lamport.
        Regla: L = max(L_local, L_recibido) + 1
        """
        with self._lock:
            self._tiempo = max(self._tiempo, ts_remoto) + 1
            valor_actual = self._tiempo
            evento = self._construir_evento("recepcion", valor_actual,
                                            nodo_remoto=remitente,
                                            ts_remoto=ts_remoto,
                                            descripcion=descripcion)
            self._historial.append(evento)

        self._persistir_evento(evento)
        logger.info("[%s] Recepción <- %s | L_remoto=%d -> L_local=%d",
                    self.nodo_id, remitente, ts_remoto, valor_actual)
        return valor_actual

    # =========================================================================
    # CONSULTAS
    # =========================================================================

    def get_tiempo(self) -> int:
        """Retorna el valor actual de L."""
        with self._lock:
            return self._tiempo

    def get_historial(self) -> list:
        """Retorna copia del historial en memoria."""
        with self._lock:
            return list(self._historial)

    def get_resumen(self) -> dict:
        """Retorna un resumen del estado del reloj para la API."""
        with self._lock:
            return {
                "nodo_id":        self.nodo_id,
                "tiempo_lamport": self._tiempo,
                "total_eventos":  len(self._historial),
            }

    @staticmethod
    def ocurrio_antes(ts_a: int, ts_b: int) -> bool:
        """Retorna True si el evento A ocurrió antes que B según Lamport."""
        return ts_a < ts_b

    # =========================================================================
    # PRIVADO
    # =========================================================================

    def _construir_evento(self, tipo: str, tiempo: int,
                          nodo_remoto=None, ts_remoto=None,
                          descripcion: str = "") -> dict:
        """Construye el dict de un evento. Siempre llamar bajo lock."""
        return {
            "seq":         len(self._historial) + 1,
            "tipo":        tipo,
            "tiempo_l":    tiempo,
            "nodo_id":     self.nodo_id,
            "hora_real":   datetime.now().isoformat(timespec="milliseconds"),
            "nodo_remoto": nodo_remoto,
            "ts_remoto":   ts_remoto,
            "descripcion": descripcion,
        }
