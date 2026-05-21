# Sistema de Mensajería P2P con Docker

**Taller de Sistemas Distribuidos — UIS 2026-1**

Sistema de mensajería punto a punto con soporte para TCP, UDP, relojes de Lamport, relojes vectoriales, detección de fallos por heartbeat y una interfaz web hecha con FastAPI. Empaquetado en contenedores Docker y orquestado con Docker Compose.

---

## Estructura del proyecto

```
mensajeria-docker/
├── Dockerfile                    # Imagen del nodo
├── docker-compose.yml            # 3 nodos en red compartida
├── docker-compose.particion.yml  # 5 nodos con partición de red (sección 6)
├── requirements.txt              # Dependencias Python
├── main.py                       # Punto de entrada del nodo
├── messaging_node.py             # Listeners TCP/UDP, envío, base de datos
├── heartbeat.py                  # Detección de fallos (ping UDP cada 3s)
├── lamport_clock.py              # Reloj de Lamport con persistencia
├── vector_clock.py               # Reloj vectorial con persistencia
└── api_server.py                 # API REST + Dashboard web (FastAPI)
```

---

## Requisitos

- Docker 20.10 o superior
- Docker Compose (incluido en Docker Desktop)

Verificar instalación:
```bash
docker --version
docker compose version
```

---

## Cómo correr el sistema

### Opción 1 — 3 nodos básicos (secciones 3 a 5)

```bash
# Construir las imágenes y levantar los 3 nodos
docker compose up --build

# Para correr en segundo plano (detached)
docker compose up --build -d

# Ver logs en tiempo real
docker compose logs -f

# Detener todo
docker compose down
```

Los tres nodos quedan disponibles en:

| Nodo  | Dashboard           | TCP   | UDP   |
|-------|---------------------|-------|-------|
| nodo1 | http://localhost:5003 | 5001  | 5002  |
| nodo2 | http://localhost:6003 | 6001  | 6002  |
| nodo3 | http://localhost:7003 | 7001  | 7002  |

### Opción 2 — Topología con partición de red (sección 6)

```bash
docker compose -f docker-compose.particion.yml up --build
```

Esto levanta 5 nodos:
- **nodo1, nodo2** → solo conectados a `red-norte`
- **nodo3, nodo4** → solo conectados a `red-sur`
- **nodo-puente** → conectado a ambas redes

`nodo1` NO puede comunicarse con `nodo4` directamente. Solo a través de `nodo-puente`.

---

## Variables de entorno configurables

Cada contenedor acepta estas variables de entorno:

| Variable    | Descripción                                | Por defecto |
|-------------|--------------------------------------------|-------------|
| `NODE_NAME` | Nombre del nodo                            | `nodo1`     |
| `TCP_PORT`  | Puerto para recibir mensajes TCP           | `5001`      |
| `UDP_PORT`  | Puerto para recibir mensajes UDP           | `5002`      |
| `API_PORT`  | Puerto del servidor HTTP (dashboard)       | `5003`      |
| `HB_PORT`   | Puerto UDP exclusivo para heartbeats       | `5004`      |
| `PEERS`     | Lista de peers en formato `nombre:puerto`  | `""`        |
| `DATA_DIR`  | Carpeta donde se guarda `mensajes.db`      | `/app/data` |

Ejemplo de `PEERS`:
```
PEERS=nodo2:5001,nodo3:5001
```

---

## Persistencia de datos

Los mensajes se guardan en una base de datos SQLite (`mensajes.db`) dentro de `/app/data/`.  
En `docker-compose.yml` este directorio está montado como un **volumen nombrado**, por lo que los mensajes **sobreviven** al reiniciar el contenedor.

```bash
# Los mensajes persisten con esto:
docker compose down
docker compose up

# Para borrar también los datos:
docker compose down -v
```

---

## Pruebas manuales

### Verificar que los nodos están corriendo

```bash
docker compose ps
```

### Ver logs de un nodo específico

```bash
docker logs -f nodo1
```

### Enviar un mensaje desde la terminal (TCP)

```bash
docker exec -it nodo1 python3 -c "
import socket, json
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('nodo2', 5001))
s.send(json.dumps({'sender': 'nodo1', 'content': 'Hola desde Docker!'}).encode())
print(s.recv(1024).decode())
s.close()
"
```

### Consultar mensajes recibidos por nodo2

```bash
curl http://localhost:6003/mensajes
```

### Consultar estado del heartbeat de nodo1

```bash
curl http://localhost:5003/heartbeat/estado
```

---

## Simulación de fallos (sección 7)

### Caída de un nodo

```bash
# Detener nodo2 (simula caída)
docker stop nodo2

# Observar logs de nodo1 — después de ~10s debería detectar a nodo2 como inactivo
docker logs -f nodo1

# Consultar el estado del heartbeat de nodo1
curl http://localhost:5003/heartbeat/estado

# Reiniciar nodo2
docker start nodo2
```

### Pausa de un nodo (nodo congelado, no caído)

```bash
# Pausar nodo3 — el proceso se congela, no procesa nada
docker pause nodo3

# Reanudar
docker unpause nodo3
```

**Diferencia `stop` vs `pause`:**  
`stop` termina el proceso (el OS libera el puerto). `pause` solo congela los hilos, el proceso sigue vivo. Para el heartbeat ambos se ven igual: el nodo deja de enviar pings.

---

## API REST — endpoints principales

Todos los endpoints aceptan y retornan JSON.

### Mensajes

```bash
# Ver todos los mensajes recibidos
GET /mensajes

# Filtrar por protocolo o remitente
GET /mensajes?protocol=tcp&sender=nodo2
```

### Peers

```bash
# Listar peers registrados
GET /peers

# Registrar un peer manualmente
POST /peers
{"name": "nodo2", "ip": "nodo2", "tcp_port": 5001, "udp_port": 5002}

# Eliminar un peer
DELETE /peers/nodo2
```

### Enviar mensajes

```bash
# TCP directo
POST /enviar/tcp
{"peer_ip": "nodo2", "peer_port": 5001, "sender": "nodo1", "content": "Hola"}

# UDP directo
POST /enviar/udp
{"peer_ip": "nodo2", "peer_port": 5002, "sender": "nodo1", "content": "Hola UDP"}

# TCP usando nombre registrado
POST /enviar/peer/tcp
{"peer_name": "nodo2", "sender": "nodo1", "content": "Hola por nombre"}

# Broadcast a todos los peers
POST /broadcast
{"sender": "nodo1", "content": "Hola a todos", "protocol": "tcp"}
```

### Relojes

```bash
# Estado actual del reloj de Lamport
GET /lamport

# Historial de eventos de Lamport (desde SQLite)
GET /lamport/historial

# Forzar tick manual
POST /lamport/tick
{"descripcion": "evento de prueba"}

# Estado actual del reloj vectorial
GET /vector

# Comparar dos vectores
POST /vector/comparar
{"va": {"nodo1":1, "nodo2":0}, "vb": {"nodo1":1, "nodo2":2}}
```

---

## Inspección de redes Docker

```bash
# Ver las redes creadas
docker network ls

# Ver qué contenedores están en una red
docker network inspect mensajeria-docker_mensajeria-net

# Probar conectividad entre contenedores
docker exec -it nodo1 ping -c 3 nodo2    # Debería funcionar
docker exec -it nodo1 ping -c 3 nodo4    # Falla si están en redes distintas
```

---

## Desarrollo sin reconstruir imagen

Para modificar el código sin hacer `docker build` cada vez, se puede usar un bind mount:

```yaml
# En docker-compose.yml, agregar en el servicio:
volumes:
  - ./:/app                 # Código local montado en el contenedor
  - datos-nodo1:/app/data   # Volumen de datos separado
command: uvicorn api_server:app --host 0.0.0.0 --port 5003 --reload
```

Con `--reload`, uvicorn detecta cambios en los archivos `.py` y reinicia solo.

---

## Notas importantes

- Los nodos se comunican entre sí usando su **nombre de servicio** (ej. `nodo2`) como hostname. Docker resuelve ese nombre a la IP interna del contenedor automáticamente (DNS interno de Docker).
- El puerto **interno** de mensajes siempre es 5001/5002, sin importar el nodo. Los puertos del host (5001, 6001, 7001...) son solo para acceder desde fuera.
- El heartbeat usa el puerto **5004 UDP** separado de los mensajes normales.
- `docker compose down` NO borra los volúmenes. `docker compose down -v` sí los borra.
