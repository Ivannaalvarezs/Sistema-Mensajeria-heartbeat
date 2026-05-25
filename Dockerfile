# ============================================================
# Dockerfile - Imagen del nodo de mensajería
# Taller de Sistemas Distribuidos - UIS 2026-1
# ============================================================

# Usamos Python 3.12 sobre Debian Slim
# para evitar problemas de compatibilidad con algunas librerías)
FROM python:3.12-slim

# Directorio de trabajo dentro del contenedor
# estos comandos se ejecutan desde /app
WORKDIR /app

# Copiamos requirements.txt antes que el resto del código
# para que el Docker cachea esta capa. Si solo cambia el código Python, el "pip install" no se vuelve a ejecutar
# en el siguiente "docker build", lo que ahorra tiempo.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# para copiar el código fuente al contenedor:
COPY . .

# Creamos el directorio de datos para SQLite
RUN mkdir -p /app/data

# Declarar¿mos los puertos que usa la aplicación
# TCP mensajes
EXPOSE 5001         
# UDP mensajes
EXPOSE 5002/udp      
# API REST y Dashboard
EXPOSE 5003          
# Heartbeat
EXPOSE 5004/udp      

# Variables de entorno con valores por defecto.
ENV NODE_NAME=nodo1
ENV TCP_PORT=5001
ENV UDP_PORT=5002
ENV API_PORT=5003
ENV HB_PORT=5004
ENV PEERS=""

# Comando de arranque del contenedor.
CMD ["python", "main.py"]
