"""
vector_clock.py - Reloj Vectorial con persistencia en SQLite
Taller de Sistemas Distribuidos - UIS 2026-1

Qué es un reloj vectorial:
  En vez de un solo contador, hay un contador por nodo.
  Ejemplo con 3 nodos: {"nodo1": 2, "nodo2": 0, "nodo3": 1}
  Permite detectar si dos eventos son concurrentes (ninguno causó al otro).

Reglas:
  - Evento interno  : V[yo] = V[yo] + 1
  - Enviar mensaje  : V[yo] = V[yo] + 1  (se envía V completo)
  - Recibir mensaje : V[i] = max(V[i], V_recibido[i]) para todo i,
                      luego V[yo] = V[yo] + 1

Relación causal entre eventos A y B:
  A -> B (A antes que B): Va[i] <= Vb[i] para todo i, y Va != Vb
  B -> A (B antes que A): Vb[i] <= Va[i] para todo i, y Va != Vb
  A || B (concurrentes) : ninguno de los anteriores se cumple

Persistencia:
  Cada evento se guarda en SQLite. Al reiniciar, se restaura el último vector.
"""

import logging
import threading
from datetime import datetime

logger = logging.getLogger("vector_clock")

# Constantes para la relación causal
ANTES       = "antes_que"
DESPUES     = "despues_que"
CONCURRENTE = "concurrente"
IGUAL       = "igual"


class RelojVectorial:
    """
    Implementación del reloj vectorial con persistencia automática en SQLite.
    Thread-safe: usa un lock en todas las operaciones de escritura.
    """

    def __init__(self, nodo_id: str, todos_nodos: list):
        """
        nodo_id     : nombre de este nodo (debe estar en todos_nodos).
        todos_nodos : lista con los nombres de todos los nodos del sistema.
        """
        if nodo_id not in todos_nodos:
            raise ValueError(f"'{nodo_id}' debe estar en todos_nodos")

        self.nodo_id     = nodo_id
        self.todos_nodos = list(todos_nodos)
        self._vector     = {nodo: 0 for nodo in todos_nodos}   # Iniciar todo en 0
        self._lock       = threading.Lock()
        self._historial  = []
        self._cargar_desde_db()

    # =========================================================================
    # CARGA INICIAL DESDE SQLITE
    # =========================================================================

    def _cargar_desde_db(self):
        """
        Al iniciar, recupera el historial de SQLite y restaura el último vector.
        """
        try:
            from messaging_node import get_historial_vector
            eventos = get_historial_vector(nodo_id=self.nodo_id)
            if eventos:
                self._historial = eventos
                # Restaurar desde el último evento guardado
                for nodo, valor in eventos[-1]["vector"].items():
                    if nodo in self._vector:
                        self._vector[nodo] = valor
                logger.info("[%s] Vector restaurado: %d eventos, V=%s",
                            self.nodo_id, len(eventos), self._vector)
            else:
                logger.info("[%s] Vector iniciando en ceros", self.nodo_id)
        except Exception as e:
            logger.warning("[%s] No se pudo cargar historial Vectorial: %s", self.nodo_id, e)

    # =========================================================================
    # GUARDAR EVENTO EN SQLITE
    # =========================================================================

    def _persistir_evento(self, evento: dict):
        """Guarda un evento en la tabla historial_vector de SQLite."""
        try:
            from messaging_node import guardar_evento_vector
            guardar_evento_vector(evento)
        except Exception as e:
            logger.warning("[%s] No se pudo guardar evento Vectorial: %s", self.nodo_id, e)

    # =========================================================================
    # REGLA 1: EVENTO INTERNO
    # =========================================================================

    def tick(self, descripcion: str = "evento interno") -> dict:
        """
        Llamar cuando ocurre algo en este nodo que no involucra comunicación.
        Regla: V[yo] = V[yo] + 1
        Retorna una copia del vector actualizado.
        """
        with self._lock:
            self._vector[self.nodo_id] += 1
            copia  = dict(self._vector)
            evento = self._construir_evento("interno", copia, descripcion=descripcion)
            self._historial.append(evento)

        self._persistir_evento(evento)
        logger.info("[%s] Tick interno -> V=%s", self.nodo_id, copia)
        return copia

    # =========================================================================
    # REGLA 2: EVENTO DE ENVÍO
    # =========================================================================

    def evento_envio(self, destino: str = "?", descripcion: str = "") -> dict:
        """
        Llamar justo antes de enviar un mensaje.
        Regla: V[yo] = V[yo] + 1
        El vector completo retornado debe adjuntarse al mensaje.
        """
        with self._lock:
            self._vector[self.nodo_id] += 1
            copia  = dict(self._vector)
            evento = self._construir_evento("envio", copia,
                                            nodo_remoto=destino, descripcion=descripcion)
            self._historial.append(evento)

        self._persistir_evento(evento)
        logger.info("[%s] Envío -> %s | V=%s", self.nodo_id, destino, copia)
        return copia

    # =========================================================================
    # REGLA 3: EVENTO DE RECEPCIÓN
    # =========================================================================

    def evento_recepcion(self, vector_remoto: dict,
                         remitente: str = "?",
                         descripcion: str = "") -> dict:
        """
        Llamar al recibir un mensaje con el vector del remitente.

        Pasos:
          1. Para cada posición i: V[i] = max(V[i], V_recibido[i])
          2. Luego: V[yo] = V[yo] + 1
        """
        with self._lock:
            # Paso 1: tomar el máximo en cada posición
            for nodo, valor_remoto in vector_remoto.items():
                if nodo in self._vector and valor_remoto > self._vector[nodo]:
                    self._vector[nodo] = valor_remoto

            # Paso 2: incrementar la posición propia
            self._vector[self.nodo_id] += 1
            copia  = dict(self._vector)
            evento = self._construir_evento("recepcion", copia,
                                            nodo_remoto=remitente,
                                            vector_remoto=vector_remoto,
                                            descripcion=descripcion)
            self._historial.append(evento)

        self._persistir_evento(evento)
        logger.info("[%s] Recepción <- %s | V_remoto=%s -> V_local=%s",
                    self.nodo_id, remitente, vector_remoto, copia)
        return copia

    # =========================================================================
    # CONSULTAS
    # =========================================================================

    def get_vector(self) -> dict:
        """Retorna copia del vector actual."""
        with self._lock:
            return dict(self._vector)

    def get_historial(self) -> list:
        """Retorna copia del historial en memoria."""
        with self._lock:
            return list(self._historial)

    def get_resumen(self) -> dict:
        """Retorna un resumen del estado del reloj para la API."""
        with self._lock:
            return {
                "nodo_id":       self.nodo_id,
                "vector":        dict(self._vector),
                "total_eventos": len(self._historial),
                "todos_nodos":   list(self.todos_nodos),
            }

    def agregar_nodo(self, nuevo_nodo: str):
        """Agrega un nodo nuevo al vector (con valor 0) si no existía."""
        with self._lock:
            if nuevo_nodo not in self._vector:
                self._vector[nuevo_nodo] = 0
                self.todos_nodos.append(nuevo_nodo)
        logger.info("[%s] Nodo agregado al vector: %s", self.nodo_id, nuevo_nodo)

    # =========================================================================
    # COMPARACIÓN DE VECTORES (método estático)
    # =========================================================================

    @staticmethod
    def comparar(va: dict, vb: dict) -> str:
        """
        Determina la relación causal entre dos vectores.

        Retorna uno de: "antes_que", "despues_que", "concurrente", "igual"

        Algoritmo:
          - Si Va[i] <= Vb[i] para todo i  →  Va es 'antes_que' Vb  (o igual)
          - Si Vb[i] <= Va[i] para todo i  →  Va es 'despues_que' Vb (o igual)
          - Si ninguno es <= al otro        →  son 'concurrente'
        """
        todas_las_claves = set(va.keys()) | set(vb.keys())

        # Verificar si Va <= Vb en todas las posiciones
        a_menor_o_igual = all(va.get(k, 0) <= vb.get(k, 0) for k in todas_las_claves)

        # Verificar si Vb <= Va en todas las posiciones
        b_menor_o_igual = all(vb.get(k, 0) <= va.get(k, 0) for k in todas_las_claves)

        if va == vb:
            return IGUAL
        elif a_menor_o_igual:
            return ANTES
        elif b_menor_o_igual:
            return DESPUES
        else:
            return CONCURRENTE

    @staticmethod
    def explicar_relacion(va: dict, vb: dict) -> str:
        """Retorna un texto legible con la relación entre dos vectores."""
        relacion = RelojVectorial.comparar(va, vb)
        if relacion == ANTES:
            return f"A ocurrió ANTES que B (A → B). Va={va} ≤ Vb={vb}"
        elif relacion == DESPUES:
            return f"A ocurrió DESPUÉS que B (B → A). Vb={vb} ≤ Va={va}"
        elif relacion == CONCURRENTE:
            return f"A y B son CONCURRENTES (A ∥ B). Ninguno causó al otro. Va={va}, Vb={vb}"
        else:
            return f"A y B tienen exactamente el mismo vector. Va = Vb = {va}"

    # =========================================================================
    # PRIVADO
    # =========================================================================

    def _construir_evento(self, tipo: str, vector_copia: dict,
                          nodo_remoto=None, vector_remoto=None,
                          descripcion: str = "") -> dict:
        """Construye el dict de un evento. Siempre llamar bajo lock."""
        return {
            "seq":           len(self._historial) + 1,
            "tipo":          tipo,
            "vector":        dict(vector_copia),
            "nodo_id":       self.nodo_id,
            "hora_real":     datetime.now().isoformat(timespec="milliseconds"),
            "nodo_remoto":   nodo_remoto,
            "vector_remoto": vector_remoto,
            "descripcion":   descripcion,
        }
