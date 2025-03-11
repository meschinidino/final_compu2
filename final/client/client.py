# client.py
import asyncio
import logging
import os
import argparse
import json
from pathlib import Path
from typing import Dict, Any

from dotenv import load_dotenv

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('log_client')

# Cargar variables de entorno
load_dotenv()
HOST = os.getenv("HOST", "127.0.0.1")
PORT = int(os.getenv("PORT", "8888"))


async def send_log_file(file_path: str) -> Dict[str, Any]:
    """
    Envía un archivo de logs al servidor.

    Args:
        file_path: Ruta al archivo a enviar

    Returns:
        Dict con la respuesta del servidor
    """
    try:
        # Verificar que el archivo exista
        path = Path(file_path)
        if not path.exists():
            return {"status": "error", "message": f"El archivo {file_path} no existe"}

        # Preparar metadatos
        file_size = path.stat().st_size
        file_name = path.name
        metadata = {
            "file_name": file_name,
            "file_size": file_size
        }

        # Conectar al servidor
        logger.info(f"Conectando a {HOST}:{PORT}")
        reader, writer = await asyncio.open_connection(HOST, PORT)

        try:
            # Enviar metadatos
            writer.write(json.dumps(metadata).encode() + b"\n")
            await writer.drain()

            # Esperar confirmación del servidor
            response = await reader.readuntil(b"\n")
            if response.decode().strip() != "READY":
                return {"status": "error", "message": "El servidor no está listo para recibir el archivo"}

            # Enviar el archivo
            logger.info(f"Enviando archivo {file_name} ({file_size} bytes)")
            bytes_sent = 0

            with open(path, 'rb') as f:
                while bytes_sent < file_size:
                    chunk = f.read(4096)
                    if not chunk:
                        break
                    writer.write(chunk)
                    await writer.drain()
                    bytes_sent += len(chunk)

                    # Mostrar progreso
                    if file_size > 1024 * 1024:  # Solo mostrar para archivos grandes
                        progress = bytes_sent / file_size * 100
                        logger.info(f"Progreso: {progress:.1f}%")

            # Recibir respuesta del servidor
            response_data = await reader.readuntil(b"\n")
            response = json.loads(response_data.decode().strip())

            logger.info(f"Respuesta del servidor: {response['status']} - {response['message']}")
            return response

        finally:
            writer.close()
            await writer.wait_closed()

    except Exception as e:
        logger.error(f"Error al enviar archivo: {e}")
        return {"status": "error", "message": str(e)}


async def main() -> None:
    """Función principal del cliente."""
    global HOST, PORT

    # Parsear argumentos de línea de comandos
    parser = argparse.ArgumentParser(description='Cliente para enviar archivos de logs')
    parser.add_argument('file', help='Ruta al archivo de logs a enviar')
    parser.add_argument('--host', help='Host del servidor', default=HOST)
    parser.add_argument('--port', help='Puerto del servidor', type=int, default=PORT)

    args = parser.parse_args()

    # Actualizar configuración si se proporcionaron argumentos
    HOST = args.host
    PORT = args.port

    # Enviar archivo
    response = await send_log_file(args.file)

    # Mostrar resultado
    if response["status"] == "success":
        logger.info("Archivo enviado con éxito")
    else:
        logger.error(f"Error al enviar archivo: {response['message']}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Cliente detenido por el usuario")