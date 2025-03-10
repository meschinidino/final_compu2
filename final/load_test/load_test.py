# load_test.py
import asyncio
import logging
import os
import json
import argparse
import random
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - S%(levelname)s - %(message)s'
)
logger = logging.getLogger('load_test')

# Cargar variables de entorno
load_dotenv()
HOST = os.getenv("HOST", "127.0.0.1")
PORT = int(os.getenv("PORT", "8888"))

# Directorio para generar logs de prueba si es necesario
TEST_DIR = "test_logs"


async def generate_test_log(index: int, size: int = 1000, error_rate: float = 0.2) -> str:
    """
    Genera un archivo de log de prueba con errores aleatorios.

    Args:
        index: Índice del cliente para nombrar el archivo
        size: Número aproximado de líneas
        error_rate: Porcentaje de líneas que serán errores

    Returns:
        Ruta al archivo generado
    """
    # Asegurar que existe el directorio
    test_dir = Path(TEST_DIR)
    test_dir.mkdir(exist_ok=True)

    # Crear nombre de archivo único
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    file_path = test_dir / f"test_log_{timestamp}_client_{index}.log"

    # Posibles niveles de log
    levels = ["INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"]

    # Mensajes de ejemplo para cada nivel
    messages = {
        "INFO": [
            "User login successful - user_id={user_id}",
            "File uploaded - filename=file_{file_id}.txt size={size}MB",
            "Task completed successfully",
            "Scheduled job executed"
        ],
        "DEBUG": [
            "Query executed: SELECT * FROM table_{table_id}",
            "Processing request with ID {request_id}"
        ],
        "WARNING": [
            "Resource usage high: CPU at {cpu}%",
            "Cache miss for key {key}",
            "Deprecated method called from {source}"
        ],
        "ERROR": [
            "Failed to process request - status_code={status}",
            "Connection refused to server{server_id}.example.com",
            "Database connection failed - timeout after {timeout}s",
            "Unhandled exception occurred: {exception_type}"
        ],
        "CRITICAL": [
            "System crash - reboot required",
            "Kernel panic - system halted",
            "Fatal error: Segmentation Fault",
            "Out of memory - application terminated"
        ]
    }

    # Generar el contenido del archivo
    with open(file_path, 'w') as f:
        for i in range(size):
            # Determinar el nivel de log
            if random.random() < error_rate:
                level = random.choice(["ERROR", "CRITICAL"])
            else:
                level = random.choice(["INFO", "DEBUG", "WARNING"])

            # Generar timestamp
            now = datetime.now()
            timestamp = now.strftime("%Y-%m-%d %H:%M:%S")

            # Seleccionar un mensaje aleatorio para el nivel
            message_template = random.choice(messages[level])

            # Rellenar los placeholders
            message = message_template.format(
                user_id=random.randint(1000, 9999),
                file_id=random.randint(1, 100),
                size=random.randint(1, 100),
                table_id=random.randint(1, 20),
                request_id=random.randint(1000, 9999),
                cpu=random.randint(70, 99),
                key=f"key_{random.randint(1, 100)}",
                source=f"module_{random.randint(1, 10)}",
                status=random.choice([400, 401, 403, 404, 500]),
                server_id=random.randint(1, 10),
                timeout=random.randint(1000, 5000),
                exception_type=random.choice(
                    ["NullPointerException", "IndexOutOfBoundsException", "DivisionByZeroError"])
            )

            # Escribir la línea de log
            f.write(f"{timestamp} {level} {message}\n")

    logger.info(f"Generado archivo de prueba: {file_path}")
    return str(file_path)


async def send_log_file(client_id: int, file_path: str, host: str, port: int) -> dict:
    """
    Envía un archivo de logs al servidor.

    Args:
        client_id: ID del cliente simulado
        file_path: Ruta al archivo a enviar
        host: Host del servidor
        port: Puerto del servidor

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

        # Agregar retraso aleatorio para simular clientes reales
        await asyncio.sleep(random.uniform(0.1, 1.0))

        # Conectar al servidor
        logger.info(f"Cliente {client_id}: Conectando a {host}:{port}")
        reader, writer = await asyncio.open_connection(host, port)

        try:
            # Enviar metadatos
            writer.write(json.dumps(metadata).encode() + b"\n")
            await writer.drain()

            # Esperar confirmación del servidor
            response = await reader.readuntil(b"\n")
            if response.decode().strip() != "READY":
                return {"status": "error", "message": "El servidor no está listo para recibir el archivo"}

            # Enviar el archivo
            logger.info(f"Cliente {client_id}: Enviando archivo {file_name} ({file_size} bytes)")
            bytes_sent = 0

            with open(path, 'rb') as f:
                while bytes_sent < file_size:
                    chunk = f.read(4096)
                    if not chunk:
                        break
                    writer.write(chunk)
                    await writer.drain()
                    bytes_sent += len(chunk)

            # Recibir respuesta del servidor
            response_data = await reader.readuntil(b"\n")
            response = json.loads(response_data.decode().strip())

            logger.info(
                f"Cliente {client_id}: Respuesta del servidor: {response['status']} - {response.get('message', '')}")
            return response

        finally:
            writer.close()
            await writer.wait_closed()
            logger.info(f"Cliente {client_id}: Conexión cerrada")

    except Exception as e:
        logger.error(f"Cliente {client_id}: Error al enviar archivo: {e}")
        return {"status": "error", "message": str(e)}


async def run_client(client_id: int, file_path: str, host: str, port: int) -> dict:
    """
    Ejecuta un cliente, generando un archivo si es necesario.

    Args:
        client_id: ID del cliente
        file_path: Ruta al archivo o "generate" para generar uno
        host: Host del servidor
        port: Puerto del servidor

    Returns:
        Dict con los resultados
    """
    start_time = datetime.now()

    # Generar archivo si es necesario
    if file_path == "generate":
        # Variar el tamaño para simular diferentes tipos de logs
        size = random.randint(500, 2000)
        error_rate = random.uniform(0.1, 0.3)
        file_path = await generate_test_log(client_id, size, error_rate)

    # Enviar archivo
    result = await send_log_file(client_id, file_path, host, port)

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    return {
        "client_id": client_id,
        "file_path": file_path,
        "result": result,
        "duration": duration
    }


async def run_load_test(num_clients: int, file_path: str, host: str, port: int) -> None:
    """
    Ejecuta una prueba de carga con múltiples clientes.

    Args:
        num_clients: Número de clientes a simular
        file_path: Ruta al archivo de logs o "generate" para crearlos
        host: Host del servidor
        port: Puerto del servidor
    """
    logger.info(f"Iniciando prueba de carga con {num_clients} clientes simultáneos")

    # Crear tareas para todos los clientes
    tasks = []
    for i in range(num_clients):
        task = run_client(i + 1, file_path, host, port)
        tasks.append(task)

    # Ejecutar todas las tareas concurrentemente
    start_time = datetime.now()
    results = await asyncio.gather(*tasks)
    end_time = datetime.now()

    # Analizar resultados
    successful = sum(1 for r in results if r["result"]["status"] == "success")
    failed = len(results) - successful
    total_duration = (end_time - start_time).total_seconds()

    # Mostrar estadísticas
    logger.info("\n" + "=" * 50)
    logger.info("RESULTADOS DE LA PRUEBA DE CARGA:")
    logger.info(f"Total de clientes: {num_clients}")
    logger.info(f"Conexiones exitosas: {successful}")
    logger.info(f"Conexiones fallidas: {failed}")
    logger.info(f"Duración total: {total_duration:.2f} segundos")

    # Calcular estadísticas de tiempo
    if results:
        durations = [r["duration"] for r in results]
        avg_duration = sum(durations) / len(durations)
        min_duration = min(durations)
        max_duration = max(durations)

        logger.info(f"Tiempo promedio por cliente: {avg_duration:.2f} segundos")
        logger.info(f"Tiempo mínimo: {min_duration:.2f} segundos")
        logger.info(f"Tiempo máximo: {max_duration:.2f} segundos")

    logger.info("=" * 50)


async def main():
    # Parsear argumentos de línea de comandos
    parser = argparse.ArgumentParser(description='Prueba de carga para el servidor de logs')
    parser.add_argument('--clients', type=int, default=5, help='Número de clientes concurrentes')
    parser.add_argument('--file', default="generate", help='Ruta al archivo de logs o "generate" para crear archivos')
    parser.add_argument('--host', default=HOST, help='Host del servidor')
    parser.add_argument('--port', type=int, default=PORT, help='Puerto del servidor')

    args = parser.parse_args()

    # Ejecutar prueba de carga
    await run_load_test(args.clients, args.file, args.host, args.port)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Prueba interrumpida por el usuario")