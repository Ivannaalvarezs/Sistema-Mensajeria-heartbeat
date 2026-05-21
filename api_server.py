"""
api_server.py - Servidor HTTP y dashboard web
Taller de Sistemas Distribuidos - UIS 2026-1

Qué hace este archivo:
  - Define los modelos de entrada con Pydantic (valida los datos de cada petición).
  - Implementa los manejadores de cada ruta de la API REST.
  - Sirve el dashboard web (HTML/JS) en la ruta raíz ("/").

Rutas disponibles:
  GET  /                    → Dashboard web
  GET  /mensajes            → Lista de mensajes recibidos (filtros opcionales)
  GET  /peers               → Lista de peers registrados
  POST /peers               → Registrar un nuevo peer
  DEL  /peers/{name}        → Eliminar un peer
  POST /enviar/tcp          → Enviar mensaje TCP directo (IP + puerto)
  POST /enviar/udp          → Enviar mensaje UDP directo (IP + puerto)
  POST /enviar/peer/tcp     → Enviar TCP usando el nombre del peer registrado
  POST /enviar/peer/udp     → Enviar UDP usando el nombre del peer registrado
  POST /broadcast           → Enviar a todos los peers a la vez
  GET  /heartbeat/estado    → Estado activo/inactivo de los peers
  POST /heartbeat/peer      → Agregar peer al monitoreo de heartbeat
  DEL  /heartbeat/peer/{n}  → Quitar peer del monitoreo
  GET  /lamport             → Valor actual del reloj de Lamport
  GET  /lamport/historial   → Historial completo (desde SQLite)
  POST /lamport/tick        → Forzar un tick manual
  GET  /vector              → Vector actual del reloj vectorial
  GET  /vector/historial    → Historial completo (desde SQLite)
  POST /vector/tick         → Forzar un tick manual
  POST /vector/comparar     → Comparar dos vectores y ver su relación causal

Nota: todos los endpoints que envían mensajes también actualizan
los relojes de Lamport y Vectorial antes del envío.
"""

from __future__ import annotations 
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from messaging_node import ( # funciones de messaging_node.py que se usan en los manejadores de rutas
    broadcast_message, add_peer, get_messages, get_peers, remove_peer,  
    send_tcp_message, send_udp_message,
    get_historial_lamport, get_historial_vector,
)

import main as _main
from vector_clock import RelojVectorial

# MODELOS DE ENTRADA :)

class ModeloPeer(BaseModel): # modelo para agregar un peer a la lista de peers registrados, se usa en la ruta /peers/agregar
    name:     str = Field(..., min_length=1)
    ip:       str
    tcp_port: int = Field(..., ge=1, le=65535)
    udp_port: int = Field(..., ge=1, le=65535)

class ModeloMensaje(BaseModel):# modelo para enviar un mensaje a un peer especifico
    peer_ip: str
    peer_port: int = Field(..., ge=1, le=65535)
    sender:    str = Field(..., min_length=1)
    content:   str = Field(..., min_length=1)

class ModeloMensajePeer(BaseModel): # modelo para enviar un mensaje a un peer especifico usando su nombre registrado para udp o tcp
    peer_name: str
    sender:    str = Field(..., min_length=1)
    content:   str = Field(..., min_length=1)

class ModeloBroadcast(BaseModel): # modelo para enviar un mensaje a todos los peers registrados para udp o tcp
    sender:   str = Field(..., min_length=1)
    content:  str = Field(..., min_length=1)
    protocol: str = Field("tcp", pattern="^(tcp|udp)$")

class ModeloHBPeer(BaseModel): # modelo para agregar un peer al monitoreo de heartbeateart para broadcast de heartbeat
    nombre: str = Field(..., min_length=1)
    ip:     str
    puerto: int = Field(..., ge=1, le=65535)

class ModeloTick(BaseModel): # modelo para hacer tick en lamport o vectorial desde el dashboard para lamport y vector
    descripcion: str = Field("evento manual")

class ModeloCompararVectores(BaseModel): # modelo para comparar dos vectores desde el dashboard, se le pasan los dos vectores como dict y se muestra la relacion entre esas
    va: dict
    vb: dict


# APLICACICON FAST API

app = FastAPI( # metadata de la API
    title="MensajeriaSD",
    description="Sistema distribuido con Heartbeat, Lamport y Vectorial - UIS 2026-1",
    version="3.0.0",
)


# MANEJADORES DE RUTAS


# DASHBOARD

def manejar_dashboard(request: Request) -> HTMLResponse: #Devuelve la página HTML
    return HTMLResponse(content=DASHBOARD_HTML)

# MENSAJES 

def manejar_get_mensajes( #Lee mensajes de SQLite
    protocol: Optional[str] = None,
    sender:   Optional[str] = None,
) -> list:
    return get_messages(protocol=protocol, sender=sender)

# PEERS 

def manejar_listar_peers() -> list: # Lista peers registrados
    return get_peers()

def manejar_crear_peer(peer: ModeloPeer) -> dict: #Agrega un peer nuevo
    add_peer(peer.name, peer.ip, peer.tcp_port, peer.udp_port)
    return {"estado": "ok", "peer": peer.name}

def manejar_eliminar_peer(name: str) -> dict: #Borra un peer
    eliminado = remove_peer(name) 
    if not eliminado:
        raise HTTPException(status_code=404, detail=f"Peer '{name}' no encontrado")
    return {"estado": "ok", "eliminado": name}

# ENVIO DE MENSAJES

def manejar_enviar_tcp(body: ModeloMensaje) -> dict: #Actualiza Lamport + envía TCP
    ts_lamport = _main.reloj_lamport.evento_envio(
        destino=body.peer_ip, descripcion=body.content
    )
    _main.reloj_vectorial.evento_envio(destino=body.peer_ip, descripcion=body.content) 
    exito = send_tcp_message(body.peer_ip, body.peer_port, body.sender,
                             body.content, lamport_ts=ts_lamport)
    if not exito:
        raise HTTPException(status_code=502, detail="Sin ACK del destinatario")
    return {"estado": "ok", "protocolo": "tcp", "lamport_ts": ts_lamport}

def manejar_enviar_udp(body: ModeloMensaje) -> dict: #Actualiza Lamport + envía UDP
    ts_lamport = _main.reloj_lamport.evento_envio(
        destino=body.peer_ip, descripcion=body.content
    )
    _main.reloj_vectorial.evento_envio(destino=body.peer_ip, descripcion=body.content)
    send_udp_message(body.peer_ip, body.peer_port, body.sender,
                     body.content, lamport_ts=ts_lamport)
    return {"estado": "enviado", "protocolo": "udp", "lamport_ts": ts_lamport}

def manejar_enviar_tcp_peer(body: ModeloMensajePeer) -> dict: #Actualiza Lamport + envía TCP usando el nombre del peer registrado para obtener su IP y puerto
    directorio = {p["name"]: p for p in get_peers()}
    if body.peer_name not in directorio:
        raise HTTPException(status_code=404, detail=f"Peer '{body.peer_name}' no encontrado")
    peer = directorio[body.peer_name]
    ts = _main.reloj_lamport.evento_envio(destino=body.peer_name, descripcion=body.content)
    _main.reloj_vectorial.evento_envio(destino=body.peer_name, descripcion=body.content)
    exito = send_tcp_message(peer["ip"], peer["tcp_port"], body.sender,
                             body.content, lamport_ts=ts)
    if not exito:
        raise HTTPException(status_code=502, detail="Sin ACK del destinatario")
    return {"estado": "ok", "protocolo": "tcp", "peer": body.peer_name, "lamport_ts": ts}

def manejar_enviar_udp_peer(body: ModeloMensajePeer) -> dict: #Actualiza Lamport + envía UDP usando el nombre del peer registrado para obtener su IP y puerto
    directorio = {p["name"]: p for p in get_peers()}
    if body.peer_name not in directorio:
        raise HTTPException(status_code=404, detail=f"Peer '{body.peer_name}' no encontrado")
    peer = directorio[body.peer_name]
    ts = _main.reloj_lamport.evento_envio(destino=body.peer_name, descripcion=body.content)
    _main.reloj_vectorial.evento_envio(destino=body.peer_name, descripcion=body.content)
    send_udp_message(peer["ip"], peer["udp_port"], body.sender,
                     body.content, lamport_ts=ts)
    return {"estado": "enviado", "protocolo": "udp", "peer": body.peer_name, "lamport_ts": ts}

def manejar_broadcast(body: ModeloBroadcast) -> dict:#Envía a todos los peers
    ts = _main.reloj_lamport.evento_envio(destino="broadcast", descripcion=body.content)
    _main.reloj_vectorial.evento_envio(destino="broadcast", descripcion=body.content)
    resultados = broadcast_message(body.sender, body.content, body.protocol, lamport_ts=ts)
    return {"estado": "ok", "protocolo": body.protocol, "resultados": resultados, "lamport_ts": ts}

# HEARTBEAT

def manejar_estado_heartbeat() -> dict: #Estado activo/inactivo de nodos
    return _main.heartbeat_manager.get_estado()

def manejar_agregar_peer_hb(body: ModeloHBPeer) -> dict: #Registra peer en heartbeat para monitoreo de vida
    _main.heartbeat_manager.registrar_peer(body.nombre, body.ip, body.puerto)
    return {"estado": "ok", "peer": body.nombre}

def manejar_eliminar_peer_hb(nombre: str) -> dict:# Elimina peer del monitoreo de heartbeat
    _main.heartbeat_manager.eliminar_peer(nombre)
    return {"estado": "ok", "eliminado": nombre}

#  RELOJ DE LAMPORT

def manejar_get_lamport() -> dict: # Valor actual de L
    return _main.reloj_lamport.get_resumen()

def manejar_historial_lamport() -> list:
    """
    Lee el historial desde SQLite no desde memoria.
    Asi siempre muestra el historial completo aunque el nodo se haya reiniciado.
    """
    return get_historial_lamport(nodo_id=_main.MY_NAME)

def manejar_tick_lamport(body: ModeloTick) -> dict: #Fuerza un evento interno en Lamport desde el dashboard
    ts = _main.reloj_lamport.tick(descripcion=body.descripcion)
    return {"estado": "ok", "lamport_ts": ts}

# RELOJ VECTORIAL

def manejar_get_vector() -> dict: #Vector actual 
    return _main.reloj_vectorial.get_resumen()

def manejar_historial_vector() -> list:#Lee historial desde SQLite
    """
    Lee el historial desde SQLite.
    Los campos 'vector' y 'vector_remoto' ya vienen deserializados como dict.
    """
    return get_historial_vector(nodo_id=_main.MY_NAME)

def manejar_tick_vector(body: ModeloTick) -> dict:# Fuerza un evento interno en Vectorial desde el dashboard
    vector = _main.reloj_vectorial.tick(descripcion=body.descripcion)
    return {"estado": "ok", "vector": vector}

def manejar_comparar_vectores(body: ModeloCompararVectores) -> dict: #Compara dos vectores
    relacion    = RelojVectorial.comparar(body.va, body.vb)
    explicacion = RelojVectorial.explicar_relacion(body.va, body.vb)
    return {"va": body.va, "vb": body.vb, "relacion": relacion, "explicacion": explicacion}

# DASHBOARD HTML

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MensajeriaSD</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Segoe UI', system-ui, sans-serif; background: #f1f5f9; color: #1e293b; min-height: 100vh; }
  header { background: #1e293b; color: #f8fafc; padding: 1rem 2rem; display: flex; align-items: center; gap: 1rem; box-shadow: 0 2px 6px #0002; }
  header h1 { font-size: 1.3rem; }
  header .sub { font-size: .8rem; color: #94a3b8; margin-left: auto; }
  main { max-width: 1150px; margin: 2rem auto; padding: 0 1rem; display: grid; gap: 1.5rem; }
  .card { background: #fff; border: 1px solid #e2e8f0; border-radius: .75rem; padding: 1.5rem; box-shadow: 0 1px 3px #0001; }
  .card h2 { font-size: 1rem; font-weight: 700; color: #0f172a; margin-bottom: 1rem; padding-bottom: .5rem; border-bottom: 2px solid #e2e8f0; }
  .card h3 { font-size: .82rem; color: #64748b; margin: .75rem 0 .4rem; text-transform: uppercase; letter-spacing: .04em; }
  .cols-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 1.5rem; }
  .fila { display: flex; flex-wrap: wrap; gap: .5rem; align-items: center; margin-top: .5rem; }
  input, select { border: 1px solid #cbd5e1; border-radius: .4rem; padding: .4rem .7rem; font-size: .875rem; color: #1e293b; background: #f8fafc; }
  input:focus, select:focus { outline: 2px solid #3b82f6; border-color: #3b82f6; }
  button { cursor: pointer; border: none; border-radius: .4rem; padding: .4rem 1rem; font-size: .85rem; font-weight: 600; transition: opacity .15s; }
  button:hover { opacity: .85; }
  .btn-azul   { background: #3b82f6; color: #fff; }
  .btn-gris   { background: #64748b; color: #fff; }
  .btn-verde  { background: #16a34a; color: #fff; }
  .btn-rojo   { background: #dc2626; color: #fff; }
  .btn-morado { background: #7c3aed; color: #fff; }
  .badge { display: inline-block; padding: .15rem .55rem; border-radius: 9999px; font-size: .7rem; font-weight: 700; text-transform: uppercase; }
  .badge-tcp        { background: #dbeafe; color: #1d4ed8; }
  .badge-udp        { background: #ede9fe; color: #6d28d9; }
  .badge-activo     { background: #dcfce7; color: #15803d; }
  .badge-inactivo   { background: #fee2e2; color: #b91c1c; }
  .badge-antes_que  { background: #e0f2fe; color: #0369a1; }
  .badge-concurrente{ background: #fef9c3; color: #a16207; }
  .badge-igual      { background: #f1f5f9; color: #475569; }
  .badge-despues_que{ background: #d1fae5; color: #065f46; }
  .badge-interno    { background: #f1f5f9; color: #475569; }
  .badge-envio      { background: #dbeafe; color: #1d4ed8; }
  .badge-recepcion  { background: #dcfce7; color: #15803d; }
  .tabla-wrap { overflow-x: auto; border: 1px solid #e2e8f0; border-radius: .5rem; margin-top: .5rem; }
  table { width: 100%; border-collapse: collapse; font-size: .82rem; }
  th { background: #f8fafc; color: #64748b; font-weight: 600; text-align: left; padding: .5rem .75rem; border-bottom: 1px solid #e2e8f0; white-space: nowrap; }
  td { padding: .45rem .75rem; border-top: 1px solid #f1f5f9; vertical-align: top; }
  tr:hover td { background: #f8fafc; }
  .ts { color: #94a3b8; font-size: .76rem; }
  .transito { font-size: .75rem; font-weight: 600; color: #7c3aed; }
  .reloj-caja { background: #f8fafc; border: 1px solid #e2e8f0; border-radius: .5rem; padding: 1rem 1.25rem; margin-bottom: .75rem; }
  .reloj-label { font-size: .75rem; color: #64748b; text-transform: uppercase; }
  .reloj-valor { font-size: 2.5rem; font-weight: 800; color: #3b82f6; line-height: 1.1; }
  .reloj-sub { font-size: .75rem; color: #94a3b8; margin-top: .25rem; }
  .vector-chips { display: flex; flex-wrap: wrap; gap: .35rem; margin-top: .35rem; }
  .chip { background: #ede9fe; color: #5b21b6; border-radius: .3rem; padding: .15rem .5rem; font-size: .78rem; font-weight: 600; font-family: monospace; }
  .hb-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px,1fr)); gap: .75rem; }
  .hb-card { border: 1px solid #e2e8f0; border-radius: .5rem; padding: .75rem; display: flex; flex-direction: column; gap: .25rem; }
  .hb-nombre { font-weight: 700; font-size: .95rem; }
  .hb-detalle { font-size: .75rem; color: #64748b; }
  .peer-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(240px,1fr)); gap: .75rem; margin-bottom: .75rem; }
  .peer-card { border: 1px solid #e2e8f0; border-radius: .5rem; padding: .75rem 1rem; display: flex; justify-content: space-between; align-items: center; }
  .peer-nombre { font-weight: 600; }
  .peer-addr { font-size: .75rem; color: #64748b; margin-top: .1rem; }
  .resultado { background: #f0fdf4; border: 1px solid #bbf7d0; border-radius: .4rem; padding: .6rem .9rem; font-size: .85rem; margin-top: .75rem; display: none; }
  #toast { position: fixed; bottom: 1.5rem; right: 1.5rem; background: #1e293b; color: #f8fafc; padding: .75rem 1.25rem; border-radius: .5rem; font-size: .875rem; opacity: 0; transition: opacity .3s; z-index: 999; pointer-events: none; }
  #toast.show  { opacity: 1; }
  #toast.error { background: #dc2626; }
  @media(max-width:680px){ .cols-2 { grid-template-columns:1fr; } }
</style>
</head>
<body>
<header>
  <span></span>
  <h1>MensajeriaSD</h1>
  <span class="sub">Sistemas Distribuidos UIS 2026-1 · v3.0</span>
</header>
<main>

  <!-- Heartbeat -->
  <div class="card">
    <h2> Heartbeat — ¿Quién está vivo?</h2>
    <p style="font-size:.82rem;color:#64748b;margin-bottom:.75rem">Ping UDP cada 3s. Sin respuesta en 10s → INACTIVO.</p>
    <div class="hb-grid" id="hb-grid"><p style="color:#94a3b8;font-size:.85rem">Sin peers en monitoreo</p></div>
    <h3>Agregar nodo al monitoreo</h3>
    <div class="fila">
      <input id="hb-nombre" placeholder="Nombre" style="width:130px">
      <input id="hb-ip"     placeholder="IP" style="width:130px">
      <input id="hb-puerto" placeholder="Puerto HB" type="number" style="width:110px">
      <button class="btn-verde" onclick="agregarPeerHB()">+ Agregar</button>
    </div>
  </div>

  <!-- Relojes -->
  <div class="cols-2">
    <div class="card">
      <h2> Reloj de Lamport</h2>
      <p style="font-size:.82rem;color:#64748b;margin-bottom:.75rem">Un entero L. tick+1 · envio+1 · recepcion=max+1. Persiste en SQLite.</p>
      <div class="reloj-caja">
        <div class="reloj-label">Nodo: <strong id="lmp-nodo">—</strong></div>
        <div class="reloj-valor" id="lmp-valor">0</div>
        <div class="reloj-sub"  id="lmp-eventos">Eventos: 0</div>
      </div>
      <div class="fila">
        <button class="btn-azul"  onclick="tickLamport()">Tick</button>
        <button class="btn-gris"  onclick="verHistorialLamport()">Ver historial</button>
      </div>
    </div>
    <div class="card">
      <h2>Reloj Vectorial</h2>
      <p style="font-size:.82rem;color:#64748b;margin-bottom:.75rem">Un contador por nodo. Detecta concurrencia. Persiste en SQLite.</p>
      <div class="reloj-caja">
        <div class="reloj-label">Nodo: <strong id="vec-nodo">—</strong></div>
        <div class="vector-chips" id="vec-chips"></div>
        <div class="reloj-sub" id="vec-eventos" style="margin-top:.4rem">Eventos: 0</div>
      </div>
      <div class="fila">
        <button class="btn-morado" onclick="tickVector()">Tick</button>
        <button class="btn-gris"   onclick="verHistorialVector()">Ver historial</button>
      </div>
    </div>
  </div>

  <!-- Comparar vectores -->
  <div class="card">
    <h2> Comparar Vectores — ¿Qué pasó primero?</h2>
    <div class="fila">
      <div>
        <div style="font-size:.78rem;color:#64748b;margin-bottom:.25rem">Vector A</div>
        <input id="cmp-va" value='{"nodo1":1,"nodo2":0,"nodo3":0}' style="width:230px">
      </div>
      <div>
        <div style="font-size:.78rem;color:#64748b;margin-bottom:.25rem">Vector B</div>
        <input id="cmp-vb" value='{"nodo1":1,"nodo2":2,"nodo3":0}' style="width:230px">
      </div>
      <button class="btn-morado" onclick="compararVectores()" style="margin-top:1.1rem">Comparar</button>
    </div>
    <div class="resultado" id="cmp-resultado"></div>
  </div>

  <!-- Mensajes -->
  <div class="card">
    <h2> Mensajes recibidos</h2>
    <div class="fila">
      <select id="f-protocol">
        <option value="">Todos</option>
        <option value="tcp">TCP</option>
        <option value="udp">UDP</option>
      </select>
      <input id="f-sender" placeholder="Filtrar por remitente" style="width:180px">
      <button class="btn-azul" onclick="cargarMensajes()">Filtrar</button>
      <button class="btn-gris" onclick="limpiarFiltros()">Limpiar</button>
    </div>
    <div class="tabla-wrap">
      <table>
        <thead>
          <tr>
            <th>#</th>
            <th>Remitente</th>
            <th>Mensaje</th>
            <th>Proto</th>
            <th>L</th>
            <th>Hora envío</th>
            <th>Hora llegada</th>
            <th>Tránsito</th>
          </tr>
        </thead>
        <tbody id="msg-tbody"><tr><td colspan="8" class="ts" style="text-align:center">Cargando...</td></tr></tbody>
      </table>
    </div>
    <div id="msg-count" style="font-size:.78rem;color:#94a3b8;margin-top:.4rem"></div>
  </div>

  <!-- Peers -->
  <div class="card">
    <h2> Peers registrados</h2>
    <div class="peer-grid" id="peers-grid"><p style="color:#94a3b8;font-size:.85rem">Sin peers</p></div>
    <h3>Agregar peer</h3>
    <div class="fila">
      <input id="p-name" placeholder="Nombre" style="width:110px">
      <input id="p-ip"   placeholder="IP" style="width:130px">
      <input id="p-tcp"  placeholder="Puerto TCP" type="number" style="width:105px">
      <input id="p-udp"  placeholder="Puerto UDP" type="number" style="width:105px">
      <button class="btn-verde" onclick="agregarPeer()">+ Agregar</button>
    </div>
  </div>

  <!-- Enviar -->
  <div class="card">
    <h2> Enviar mensaje directo</h2>
    <div class="fila">
      <input id="s-sender"  placeholder="Tu nombre" style="width:120px">
      <input id="s-content" placeholder="Texto del mensaje" style="width:200px">
      <input id="s-ip"      placeholder="IP destino" style="width:130px">
      <input id="s-port"    placeholder="Puerto" type="number" style="width:90px">
      <select id="s-proto"><option value="tcp">TCP</option><option value="udp">UDP</option></select>
      <button class="btn-azul" onclick="enviarMensaje()">Enviar</button>
    </div>
  </div>

  <!-- Broadcast -->
  <div class="card">
    <h2> Broadcast a todos los peers</h2>
    <div class="fila">
      <input id="b-sender"  placeholder="Tu nombre" style="width:120px">
      <input id="b-content" placeholder="Mensaje" style="width:250px">
      <select id="b-proto"><option value="tcp">TCP</option><option value="udp">UDP</option></select>
      <button class="btn-azul" onclick="hacerBroadcast()">Broadcast</button>
    </div>
  </div>

  <!-- Historial -->
  <div class="card" id="seccion-historial" style="display:none">
    <h2 id="historial-titulo">Historial</h2>
    <div class="tabla-wrap">
      <table>
        <thead id="historial-cabecera"></thead>
        <tbody id="historial-cuerpo"></tbody>
      </table>
    </div>
    <div class="fila" style="margin-top:.75rem">
      <button class="btn-gris" onclick="cerrarHistorial()">Cerrar</button>
    </div>
  </div>

</main>
<div id="toast"></div>

<script>
  function esc(s) {
    return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }
  function toast(msg, esError=false) {
    const el = document.getElementById('toast');
    el.textContent = msg;
    el.className = 'show' + (esError ? ' error' : '');
    setTimeout(() => el.className = '', 3000);
  }
  function cerrarHistorial() {
    document.getElementById('seccion-historial').style.display = 'none';
  }

  // Calcular tiempo de transito entre sent_at y received_at
  function calcularTransito(sent_at, received_at) {
    if (!sent_at || !received_at) return '—';
    const diff = new Date(received_at) - new Date(sent_at);
    if (isNaN(diff) || diff < 0) return '—';
    if (diff < 1000) return diff + ' ms';
    return (diff / 1000).toFixed(2) + ' s';
  }

  // ── Heartbeat ──
  async function cargarHeartbeat() {
    const data = await fetch('/heartbeat/estado').then(r => r.json()).catch(()=>({}));
    const grid = document.getElementById('hb-grid');
    const entradas = Object.entries(data);
    if (!entradas.length) { grid.innerHTML = '<p style="color:#94a3b8;font-size:.85rem">Sin peers en monitoreo</p>'; return; }
    grid.innerHTML = entradas.map(([nombre, info]) => `
      <div class="hb-card">
        <div class="hb-nombre">${esc(nombre)}</div>
        <div><span class="badge badge-${info.estado}">${info.estado}</span></div>
        <div class="hb-detalle">Último ping: ${info.ultimo_ping.split('T')[1]}</div>
        <div class="hb-detalle">Sin ping: ${info.segundos_sin_ping}s</div>
      </div>`).join('');
  }
  async function agregarPeerHB() {
    const body = { nombre: document.getElementById('hb-nombre').value.trim(), ip: document.getElementById('hb-ip').value.trim(), puerto: parseInt(document.getElementById('hb-puerto').value) };
    if (!body.nombre || !body.ip || !body.puerto) { toast('Completa todos los campos', true); return; }
    const r = await fetch('/heartbeat/peer', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body) });
    if (r.ok) { toast('Peer agregado'); cargarHeartbeat(); } else toast('Error', true);
  }

  // ── Lamport ──
  async function cargarLamport() {
    const d = await fetch('/lamport').then(r => r.json()).catch(()=>({}));
    document.getElementById('lmp-nodo').textContent   = d.nodo_id || '—';
    document.getElementById('lmp-valor').textContent  = d.tiempo_lamport ?? '—';
    document.getElementById('lmp-eventos').textContent = `Eventos en DB: ${d.total_eventos ?? 0}`;
  }
  async function tickLamport() {
    const r = await fetch('/lamport/tick', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({descripcion:'tick manual'}) });
    const d = await r.json();
    toast(`Lamport tick → L = ${d.lamport_ts}`);
    cargarLamport();
  }
  async function verHistorialLamport() {
    const datos = await fetch('/lamport/historial').then(r => r.json());
    document.getElementById('historial-titulo').textContent = 'Historial — Reloj de Lamport (desde SQLite)';
    document.getElementById('historial-cabecera').innerHTML =
      '<tr><th>#</th><th>Tipo</th><th>L</th><th>Nodo remoto</th><th>L remoto</th><th>Descripción</th><th>Hora real</th></tr>';
    document.getElementById('historial-cuerpo').innerHTML = datos.map(e => `
      <tr>
        <td>${e.seq}</td>
        <td><span class="badge badge-${e.tipo}">${esc(e.tipo)}</span></td>
        <td><strong>${e.tiempo_l}</strong></td>
        <td>${esc(e.nodo_remoto||'—')}</td>
        <td>${e.ts_remoto !== null && e.ts_remoto !== undefined ? e.ts_remoto : '—'}</td>
        <td>${esc(e.descripcion||'')}</td>
        <td class="ts">${e.hora_real}</td>
      </tr>`).join('') || '<tr><td colspan="7" class="ts" style="text-align:center">Sin eventos</td></tr>';
    document.getElementById('seccion-historial').style.display = 'block';
    document.getElementById('seccion-historial').scrollIntoView({behavior:'smooth'});
  }

  // ── Vector ──
  async function cargarVector() {
    const d = await fetch('/vector').then(r => r.json()).catch(()=>({}));
    document.getElementById('vec-nodo').textContent    = d.nodo_id || '—';
    document.getElementById('vec-eventos').textContent = `Eventos en DB: ${d.total_eventos ?? 0}`;
    const chips = Object.entries(d.vector || {}).map(([k,v]) => `<div class="chip">${esc(k)}: ${v}</div>`).join('');
    document.getElementById('vec-chips').innerHTML = chips || '<span style="color:#94a3b8">vacío</span>';
  }
  async function tickVector() {
    const r = await fetch('/vector/tick', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({descripcion:'tick manual'}) });
    const d = await r.json();
    toast(`Vector tick → ${JSON.stringify(d.vector)}`);
    cargarVector();
  }
  async function verHistorialVector() {
    const datos = await fetch('/vector/historial').then(r => r.json());
    document.getElementById('historial-titulo').textContent = 'Historial — Reloj Vectorial (desde SQLite)';
    document.getElementById('historial-cabecera').innerHTML =
      '<tr><th>#</th><th>Tipo</th><th>Vector</th><th>Nodo remoto</th><th>V remoto</th><th>Descripción</th><th>Hora real</th></tr>';
    document.getElementById('historial-cuerpo').innerHTML = datos.map(e => `
      <tr>
        <td>${e.seq}</td>
        <td><span class="badge badge-${e.tipo}">${esc(e.tipo)}</span></td>
        <td><code style="font-size:.75rem">${esc(JSON.stringify(e.vector))}</code></td>
        <td>${esc(e.nodo_remoto||'—')}</td>
        <td><code style="font-size:.75rem">${esc(JSON.stringify(e.vector_remoto||null))}</code></td>
        <td>${esc(e.descripcion||'')}</td>
        <td class="ts">${e.hora_real}</td>
      </tr>`).join('') || '<tr><td colspan="7" class="ts" style="text-align:center">Sin eventos</td></tr>';
    document.getElementById('seccion-historial').style.display = 'block';
    document.getElementById('seccion-historial').scrollIntoView({behavior:'smooth'});
  }

  // ── Comparar ──
  async function compararVectores() {
    let va, vb;
    try { va = JSON.parse(document.getElementById('cmp-va').value); } catch { toast('Vector A invalido', true); return; }
    try { vb = JSON.parse(document.getElementById('cmp-vb').value); } catch { toast('Vector B invalido', true); return; }
    const r = await fetch('/vector/comparar', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({va, vb}) });
    const d = await r.json();
    const div = document.getElementById('cmp-resultado');
    div.style.display = 'block';
    div.innerHTML = `<span class="badge badge-${d.relacion}">${d.relacion.replace('_',' ')}</span> &nbsp; ${esc(d.explicacion)}`;
  }

  // ── Mensajes (con hora envio, llegada y transito) ──
  async function cargarMensajes() {
    const proto  = document.getElementById('f-protocol').value;
    const sender = document.getElementById('f-sender').value.trim();
    let url = '/mensajes?';
    if (proto)  url += `protocol=${encodeURIComponent(proto)}&`;
    if (sender) url += `sender=${encodeURIComponent(sender)}`;
    const msgs = await fetch(url).then(r => r.json()).catch(()=>[]);
    document.getElementById('msg-tbody').innerHTML = msgs.length
      ? msgs.map((m, i) => `
          <tr>
            <td class="ts">${msgs.length - i}</td>
            <td><strong>${esc(m.sender)}</strong></td>
            <td>${esc(m.content)}</td>
            <td><span class="badge badge-${m.protocol}">${m.protocol.toUpperCase()}</span></td>
            <td>${m.lamport_ts !== null && m.lamport_ts !== undefined ? '<strong>L='+m.lamport_ts+'</strong>' : '—'}</td>
            <td class="ts">${m.sent_at ? m.sent_at.replace('T',' ') : '—'}</td>
            <td class="ts">${m.received_at ? m.received_at.replace('T',' ') : (m.timestamp||'—').replace('T',' ')}</td>
            <td class="transito">${calcularTransito(m.sent_at, m.received_at || m.timestamp)}</td>
          </tr>`).join('')
      : '<tr><td colspan="8" class="ts" style="text-align:center">Sin mensajes</td></tr>';
    document.getElementById('msg-count').textContent = `${msgs.length} mensaje(s)`;
  }
  function limpiarFiltros() {
    document.getElementById('f-protocol').value = '';
    document.getElementById('f-sender').value = '';
    cargarMensajes();
  }

  // ── Peers ──
  async function cargarPeers() {
    const peers = await fetch('/peers').then(r => r.json()).catch(()=>[]);
    const grid = document.getElementById('peers-grid');
    grid.innerHTML = peers.length
      ? peers.map(p => `
          <div class="peer-card">
            <div>
              <div class="peer-nombre">${esc(p.name)}</div>
              <div class="peer-addr">${p.ip} · TCP:${p.tcp_port} · UDP:${p.udp_port}</div>
            </div>
            <button class="btn-rojo" style="padding:.2rem .6rem;font-size:.75rem" onclick="eliminarPeer('${esc(p.name)}')">✕</button>
          </div>`).join('')
      : '<p style="color:#94a3b8;font-size:.85rem">Sin peers</p>';
  }
  async function agregarPeer() {
    const body = { name: document.getElementById('p-name').value.trim(), ip: document.getElementById('p-ip').value.trim(), tcp_port: parseInt(document.getElementById('p-tcp').value), udp_port: parseInt(document.getElementById('p-udp').value) };
    if (!body.name || !body.ip || !body.tcp_port || !body.udp_port) { toast('Completa todos los campos', true); return; }
    const r = await fetch('/peers', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body) });
    if (r.ok) { toast('Peer agregado'); cargarPeers(); } else toast('Error', true);
  }
  async function eliminarPeer(nombre) {
    const r = await fetch(`/peers/${encodeURIComponent(nombre)}`, {method:'DELETE'});
    if (r.ok) { toast(`Peer '${nombre}' eliminado`); cargarPeers(); } else toast('Error', true);
  }

  // ── Envio ──
  async function enviarMensaje() {
    const proto = document.getElementById('s-proto').value;
    const body = { peer_ip: document.getElementById('s-ip').value.trim(), peer_port: parseInt(document.getElementById('s-port').value), sender: document.getElementById('s-sender').value.trim(), content: document.getElementById('s-content').value.trim() };
    if (!body.peer_ip || !body.peer_port || !body.sender || !body.content) { toast('Completa todos los campos', true); return; }
    const r = await fetch(`/enviar/${proto}`, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body) });
    const d = await r.json();
    if (r.ok) { toast(`Enviado via ${proto.toUpperCase()} · L=${d.lamport_ts}`); cargarLamport(); cargarVector(); cargarMensajes(); }
    else toast('Error al enviar', true);
  }

  // ── Broadcast ──
  async function hacerBroadcast() {
    const body = { sender: document.getElementById('b-sender').value.trim(), content: document.getElementById('b-content').value.trim(), protocol: document.getElementById('b-proto').value };
    if (!body.sender || !body.content) { toast('Faltan datos', true); return; }
    const r = await fetch('/broadcast', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body) });
    const d = await r.json();
    if (r.ok) { toast(`Broadcast enviado · L=${d.lamport_ts}`); cargarLamport(); cargarVector(); }
    else toast('Error', true);
  }

  // ── Refresh automatico cada 5s ──
  function actualizarTodo() { cargarMensajes(); cargarPeers(); cargarHeartbeat(); cargarLamport(); cargarVector(); }
  actualizarTodo();
  setInterval(actualizarTodo, 5000);
</script>
</body>
</html>"""


# REGISTRO EXPLICITO DE RUTAS

def registrar_rutas(aplicacion: FastAPI):
    aplicacion.add_api_route("/",                  manejar_dashboard,         methods=["GET"],    response_class=HTMLResponse) 
    aplicacion.add_api_route("/mensajes",           manejar_get_mensajes,       methods=["GET"])
    aplicacion.add_api_route("/peers",              manejar_listar_peers,       methods=["GET"])
    aplicacion.add_api_route("/peers",              manejar_crear_peer,         methods=["POST"],   status_code=201)
    aplicacion.add_api_route("/peers/{name}",       manejar_eliminar_peer,      methods=["DELETE"])
    aplicacion.add_api_route("/enviar/tcp",         manejar_enviar_tcp,         methods=["POST"])
    aplicacion.add_api_route("/enviar/udp",         manejar_enviar_udp,         methods=["POST"])
    aplicacion.add_api_route("/enviar/peer/tcp",    manejar_enviar_tcp_peer,    methods=["POST"])
    aplicacion.add_api_route("/enviar/peer/udp",    manejar_enviar_udp_peer,    methods=["POST"])
    aplicacion.add_api_route("/broadcast",          manejar_broadcast,          methods=["POST"])
    aplicacion.add_api_route("/heartbeat/estado",   manejar_estado_heartbeat,   methods=["GET"])
    aplicacion.add_api_route("/heartbeat/peer",     manejar_agregar_peer_hb,    methods=["POST"],   status_code=201)
    aplicacion.add_api_route("/heartbeat/peer/{nombre}", manejar_eliminar_peer_hb, methods=["DELETE"])
    aplicacion.add_api_route("/lamport",            manejar_get_lamport,        methods=["GET"])
    aplicacion.add_api_route("/lamport/historial",  manejar_historial_lamport,  methods=["GET"])
    aplicacion.add_api_route("/lamport/tick",       manejar_tick_lamport,       methods=["POST"])
    aplicacion.add_api_route("/vector",             manejar_get_vector,         methods=["GET"])
    aplicacion.add_api_route("/vector/historial",   manejar_historial_vector,   methods=["GET"])
    aplicacion.add_api_route("/vector/tick",        manejar_tick_vector,        methods=["POST"])
    aplicacion.add_api_route("/vector/comparar",    manejar_comparar_vectores,  methods=["POST"])


registrar_rutas(app)
