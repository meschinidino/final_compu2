import asyncio
import logging
import os
import re
from pathlib import Path
from typing import Tuple, Dict, Any
import json
import sqlite3
import multiprocessing as mp

from dotenv import load_dotenv

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('log_server')

# Cargar variables de entorno
load_dotenv()
HOST = os.getenv("HOST", "127.0.0.1")
PORT = int(os.getenv("PORT", "8888"))
STORAGE_PATH = os.getenv("STORAGE_PATH", "logs")
DB_PATH = os.getenv("DB_PATH", "logs.db")

# Asegurar que el directorio de almacenamiento existe
Path(STORAGE_PATH).mkdir(exist_ok=True)


# Configuración de la base de datos
def setup_database() -> None:
    """Configura la base de datos para almacenar resultados del procesamiento."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS log_entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        level TEXT,
        source TEXT,
        message TEXT,
        file_name TEXT,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS log_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_name TEXT,
        entry_count INTEGER,
        error_count INTEGER,
        warning_count INTEGER,
        info_count INTEGER,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    conn.commit()
    conn.close()


# Clase para manejar las conexiones de clientes
class LogServerProtocol:
    def __init__(self, task_queue: mp.Queue) -> None:
        """
        Inicializa el protocolo del servidor de logs.

        Args:
            task_queue: Cola para enviar tareas al procesador de logs
        """
        self.task_queue = task_queue

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """
        Maneja la conexión de un cliente.

        Args:
            reader: StreamReader para leer datos del cliente
            writer: StreamWriter para enviar datos al cliente
        """
        addr = writer.get_extra_info('peername')
        logger.info(f"Nueva conexión de {addr}")

        try:
            # Recibir metadatos del archivo
            metadata_data = await reader.readuntil(b"\n")
            metadata = json.loads(metadata_data.decode().strip())
            file_name = metadata.get("file_name", f"unknown_{addr[0]}_{addr[1]}.log")
            file_size = metadata.get("file_size", 0)

            logger.info(f"Recibiendo archivo {file_name} ({file_size} bytes) de {addr}")

            # Enviar confirmación de recepción de metadatos
            writer.write(b"READY\n")
            await writer.drain()

            # Recibir el contenido del archivo
            file_path = Path(STORAGE_PATH) / file_name
            bytes_received = 0

            with open(file_path, 'wb') as f:
                while bytes_received < file_size:
                    chunk = await reader.read(min(4096, file_size - bytes_received))
                    if not chunk:
                        break
                    f.write(chunk)
                    bytes_received += len(chunk)

            if bytes_received == file_size:
                logger.info(f"Archivo {file_name} recibido completamente.")
                response = {"status": "success", "message": f"Archivo {file_name} recibido correctamente"}

                # Enviar tarea al procesador de logs
                self.task_queue.put({"file_path": str(file_path), "file_name": file_name})
            else:
                logger.warning(f"Archivo {file_name} recibido parcialmente ({bytes_received}/{file_size} bytes).")
                response = {"status": "error", "message": f"Archivo {file_name} recibido parcialmente"}

            # Enviar respuesta al cliente
            writer.write(json.dumps(response).encode() + b"\n")
            await writer.drain()

        except Exception as e:
            logger.error(f"Error al manejar conexión de {addr}: {e}")
            response = {"status": "error", "message": str(e)}
            writer.write(json.dumps(response).encode() + b"\n")
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            logger.info(f"Conexión con {addr} cerrada")


# Procesador de logs (ejecutado en un proceso separado)
def log_processor(task_queue: mp.Queue) -> None:
    """
    Procesa logs de la cola de tareas
    """
    logger = logging.getLogger('log_processor')
    logger.info("Procesador de logs iniciado")

    # Import del modulo de worker
    import importlib.util
    import os

    workers_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "workers", "workers.py")

    spec = importlib.util.spec_from_file_location("workers", workers_path)
    workers_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(workers_module)
    process_log_file = workers_module.process_log_file

    while True:
        try:
            task = task_queue.get()
            if task is None:  # Señal para terminar
                break

            file_path = task["file_path"]
            file_name = task["file_name"]
            logger.info(f"Enviando archivo al worker: {file_path}")

            # Llamar al worker
            process_log_file.delay(file_name)

            logger.info(f"Archivo {file_name} enviado al worker para procesamiento")

        except Exception as e:
            logger.error(f"Error al procesar tarea: {e}")

    logger.info("Procesador de logs finalizado")


# Función principal del servidor
async def main() -> None:
    """Inicia el servidor y el procesador de logs."""
    # Configurar la base de datos
    setup_database()

    # Crear cola de comunicación entre procesos
    task_queue = mp.Queue()

    # Iniciar procesador de logs en un proceso separado
    processor = mp.Process(target=log_processor, args=(task_queue,))
    processor.start()

    # Crear protocolo de servidor
    protocol = LogServerProtocol(task_queue)

    # Iniciar servidor
    server = await asyncio.start_server(
        protocol.handle_client, HOST, PORT
    )

    addr = server.sockets[0].getsockname()
    logger.info(f"Servidor iniciado en {addr}")

    try:
        async with server:
            await server.serve_forever()
    except asyncio.CancelledError:
        logger.info("Servidor detenido")
    finally:
        # Señalizar al procesador que debe terminar
        task_queue.put(None)
        processor.join()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Servidor detenido por el usuario")