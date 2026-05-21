# ============================================================
# Dockerfile - Imagen del nodo de mensajería
# Taller de Sistemas Distribuidos - UIS 2026-1
# ============================================================

# Usamos Python 3.12 sobre Debian Slim (imagen liviana, sin Alpine
# para evitar problemas de compatibilidad con algunas librerías C).
FROM python:3.12-slim

# Directorio de trabajo dentro del contenedor.
# Todos los comandos siguientes se ejecutan desde /app.
WORKDIR /app

# Copiamos requirements.txt ANTES que el resto del código.
# Ventaja: Docker cachea esta capa. Si solo cambia el código Python
# (no las dependencias), el "pip install" no se vuelve a ejecutar
# en el siguiente "docker build", lo que ahorra tiempo.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente al contenedor.
COPY . .

# Crear el directorio de datos para SQLite.
# Este directorio es el punto de montaje del volumen en docker-compose.yml.
# Si no hay volumen, los datos se guardan aquí dentro del contenedor
# (se pierden al borrarlo).
RUN mkdir -p /app/data

# Declarar los puertos que usa la aplicación.
# EXPOSE es solo documentación; los puertos se publican con -p o en Compose.
EXPOSE 5001          # TCP mensajes
EXPOSE 5002/udp      # UDP mensajes
EXPOSE 5003          # API REST y Dashboard
EXPOSE 5004/udp      # Heartbeat

# Variables de entorno con valores por defecto.
# Pueden sobreescribirse desde docker-compose.yml o con -e en docker run.
ENV NODE_NAME=nodo1
ENV TCP_PORT=5001
ENV UDP_PORT=5002
ENV API_PORT=5003
ENV HB_PORT=5004
ENV PEERS=""

# Comando de arranque del contenedor.
CMD ["python", "main.py"]
