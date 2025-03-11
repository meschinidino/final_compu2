import os
import time
import logging
import sqlite3
import json
from typing import Dict, List, Any, Optional
import re
from datetime import datetime
from collections import Counter, defaultdict
from celery import Celery
from dotenv import load_dotenv
from celery import chain

# Cargar variables de entorno
load_dotenv()
BROKER_URL = os.getenv("BROKER_URL", "pyamqp://guest@localhost//")
DB_PATH = os.getenv("DB_PATH", "logs.db")
STORAGE_PATH = os.getenv("STORAGE_PATH", "logs")

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('log_worker')

# Configuración de Celery
app = Celery('log_tasks', broker=BROKER_URL)
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)


# python
@app.task
def analyze_log_patterns(file_name: str) -> Dict[str, Any]:
    """
    Analiza patrones en los logs almacenados en la base de datos.

    Args:
        file_name: Nombre del archivo de logs a analizar

    Returns:
        Dict con los resultados del análisis
    """
    logger.info(f"Analizando patrones en logs del archivo: {file_name}")

    try:
        # Conectar a la base de datos
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Crear tabla de análisis si no existe
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS log_analysis (id INTEGER PRIMARY KEY AUTOINCREMENT, file_name TEXT, results TEXT, analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )

        # Obtener entradas de log para el archivo
        cursor.execute(
            "SELECT id, timestamp, level, source, message FROM log_entries WHERE file_name = ?",
            (file_name,)
        )
        entries = cursor.fetchall()
        logger.info(f"Total de entradas de log obtenidas: {len(entries)}")

        # Análisis de patrones
        results = {
            "file_name": file_name,
            "entry_count": len(entries),
            "patterns": {},
            "time_distribution": {},
            "sources": {},
            "common_errors": []
        }

        # Patrones específicos a buscar
        error_patterns = {
            "connection_issues": r"(?i)connection (?:refused|reset|closed|error|failed)",
            "timeouts": r"(?i)timeout|timed out",
            "resources": r"(?i)out of (?:memory|resources)",
            "permissions": r"(?i)permission denied|unauthorized",
            "general_errors": r"(?i)exception|error|fail|crash",
            "system_failures": r"(?i)system (?:crash|halted)|kernel panic|segmentation fault"
        }

        # Contadores para análisis
        pattern_counts = {pattern_name: 0 for pattern_name in error_patterns}
        hour_distribution = defaultdict(int)
        source_counts = Counter()
        error_messages = []

        # Analizar cada entrada
        for entry_id, timestamp, level, source, message in entries:
            # Distribución temporal
            try:
                dt = datetime.fromisoformat(timestamp)
                hour = dt.hour
                hour_distribution[hour] += 1
            except (ValueError, TypeError):
                pass

            # Conteo de fuentes
            if source:
                source_counts[source] += 1

            # Buscar patrones de error
            if level and any(err_term in level.upper() for err_term in ["ERROR", "CRITICAL", "FATAL"]):
                error_messages.append(message)

                logger.debug(f"Checking entry: level={level}, message={message[:50]}...")
                for pattern_name, pattern_str in error_patterns.items():
                    if re.search(pattern_str, message):
                        pattern_counts[pattern_name] += 1

        # Formatear resultados
        results["patterns"] = dict(pattern_counts)
        results["time_distribution"] = dict(hour_distribution)
        results["sources"] = dict(source_counts)

        # Encontrar errores comunes (mensajes duplicados)
        error_counter = Counter(error_messages)
        results["common_errors"] = [
            {"message": msg, "count": count}
            for msg, count in error_counter.most_common(10) if count > 1
        ]

        # Guardar resultados del análisis
        result_json = json.dumps(results)
        cursor.execute(
            "INSERT INTO log_analysis (file_name, results) VALUES (?, ?)",
            (file_name, result_json)
        )
        conn.commit()

        logger.info(f"Análisis de patrones completado para {file_name}")
        return results

    except Exception as e:
        logger.error(f"Error al analizar patrones: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        if 'conn' in locals():
            conn.close()

@app.task
def generate_log_report(file_name: str) -> Dict[str, Any]:
    """Generate log report with improved database structure"""
    logger.info(f"Generando informe para: {file_name}")

    try:
        # Connect to database
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Create normalized tables if they don't exist
        cursor.executescript("""
        CREATE TABLE IF NOT EXISTS log_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT,
            generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            total_entries INTEGER,
            error_count INTEGER,
            warning_count INTEGER,
            info_count INTEGER,
            error_rate TEXT,
            warning_rate TEXT,
            health_status TEXT
        );

        CREATE TABLE IF NOT EXISTS log_patterns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_id INTEGER,
            pattern_name TEXT,
            count INTEGER,
            FOREIGN KEY(report_id) REFERENCES log_reports(id)
        );

        CREATE TABLE IF NOT EXISTS log_common_errors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_id INTEGER,
            message TEXT,
            count INTEGER,
            FOREIGN KEY(report_id) REFERENCES log_reports(id)
        );

        CREATE TABLE IF NOT EXISTS log_time_distribution (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_id INTEGER,
            hour INTEGER,
            count INTEGER,
            FOREIGN KEY(report_id) REFERENCES log_reports(id)
        );
        """)

        # Get basic statistics
        cursor.execute(
            "SELECT entry_count, error_count, warning_count, info_count FROM log_stats WHERE file_name = ?",
            (file_name,)
        )
        stats = cursor.fetchone()

        if not stats:
            return {"status": "error", "message": f"No se encontraron estadísticas para {file_name}"}

        entry_count, error_count, warning_count, info_count = stats
        error_rate = (error_count / entry_count * 100) if entry_count > 0 else 0
        warning_rate = (warning_count / entry_count * 100) if entry_count > 0 else 0
        health_status = "critical" if error_rate > 10 else "warning" if error_rate > 5 else "healthy"

        # Get pattern analysis
        cursor.execute(
            "SELECT results FROM log_analysis WHERE file_name = ? ORDER BY analyzed_at DESC LIMIT 1",
            (file_name,)
        )
        analysis_row = cursor.fetchone()
        pattern_analysis = json.loads(analysis_row[0]) if analysis_row else {}

        # Insert main report record
        cursor.execute(
            """INSERT INTO log_reports 
               (file_name, generated_at, total_entries, error_count, warning_count, 
                info_count, error_rate, warning_rate, health_status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (file_name, datetime.now().isoformat(), entry_count, error_count, warning_count,
             info_count, f"{error_rate:.2f}%", f"{warning_rate:.2f}%", health_status)
        )
        report_id = cursor.lastrowid

        # Insert patterns
        patterns = pattern_analysis.get("patterns", {})
        for pattern_name, count in patterns.items():
            cursor.execute(
                "INSERT INTO log_patterns (report_id, pattern_name, count) VALUES (?, ?, ?)",
                (report_id, pattern_name, count)
            )

        # Insert common errors
        common_errors = pattern_analysis.get("common_errors", [])
        for error in common_errors:
            cursor.execute(
                "INSERT INTO log_common_errors (report_id, message, count) VALUES (?, ?, ?)",
                (report_id, error["message"], error["count"])
            )

        # Insert time distribution
        time_dist = pattern_analysis.get("time_distribution", {})
        for hour, count in time_dist.items():
            cursor.execute(
                "INSERT INTO log_time_distribution (report_id, hour, count) VALUES (?, ?, ?)",
                (report_id, int(hour), count)
            )

        conn.commit()

        # Return data in the same format for API compatibility
        report = {
            "file_name": file_name,
            "generated_at": datetime.now().isoformat(),
            "summary": {
                "total_entries": entry_count,
                "error_count": error_count,
                "warning_count": warning_count,
                "info_count": info_count,
                "error_rate": f"{error_rate:.2f}%",
                "warning_rate": f"{warning_rate:.2f}%",
            },
            "health_status": health_status,
            "patterns": patterns,
            "common_errors": common_errors,
            "time_distribution": pattern_analysis.get("time_distribution", {})
        }

        logger.info(f"Informe generado para {file_name}")
        return report

    except Exception as e:
        logger.error(f"Error al generar informe: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        if 'conn' in locals():
            conn.close()


# Tarea de limpieza de datos
@app.task
def clean_log_data(file_name: str) -> Dict[str, Any]:
    """
    Limpia entradas de logs irrelevantes o corruptas.

    Args:
        file_name: Nombre del archivo de logs a limpiar

    Returns:
        Dict con resultados de la limpieza
    """
    logger.info(f"Limpiando datos de logs para: {file_name}")

    try:
        # Conectar a la base de datos
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Contar entradas antes de la limpieza
        cursor.execute(
            "SELECT COUNT(*) FROM log_entries WHERE file_name = ?",
            (file_name,)
        )
        initial_count = cursor.fetchone()[0]

        # Identificar entradas corruptas o irrelevantes
        # Criterios de limpieza:
        # 1. Mensajes vacíos
        # 2. Entradas sin timestamp válido
        # 3. Mensajes duplicados
        cursor.execute(
            """
            DELETE FROM log_entries 
            WHERE file_name = ? AND (
                message IS NULL OR
                trim(message) = '' OR
                timestamp IS NULL OR
                trim(timestamp) = ''
            )
            """,
            (file_name,)
        )

        # Eliminar duplicados exactos (mantener la primera ocurrencia)
        cursor.execute(
            """
            DELETE FROM log_entries 
            WHERE id NOT IN (
                SELECT MIN(id) 
                FROM log_entries 
                WHERE file_name = ? 
                GROUP BY timestamp, level, source, message
            ) AND file_name = ?
            """,
            (file_name, file_name)
        )

        conn.commit()

        # Contar entradas después de la limpieza
        cursor.execute(
            "SELECT COUNT(*) FROM log_entries WHERE file_name = ?",
            (file_name,)
        )
        final_count = cursor.fetchone()[0]

        # Resultados de la limpieza
        removed_count = initial_count - final_count
        results = {
            "file_name": file_name,
            "initial_count": initial_count,
            "final_count": final_count,
            "removed_count": removed_count,
            "removal_percentage": f"{(removed_count / initial_count * 100):.2f}%" if initial_count > 0 else "0%"
        }

        # Actualizar estadísticas en la tabla log_stats
        cursor.execute(
            """
            UPDATE log_stats SET entry_count = ?
            WHERE file_name = ?
            """,
            (final_count, file_name)
        )
        conn.commit()

        logger.info(f"Limpieza de datos completada para {file_name}. Entradas eliminadas: {removed_count}")
        return results

    except Exception as e:
        logger.error(f"Error al limpiar datos: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        if 'conn' in locals():
            conn.close()


# Función para procesamiento completo
@app.task(name='workers.process_log_file')
def process_log_file(file_name: str) -> Dict[str, Any]:
    """
    Realiza el procesamiento completo de un archivo de logs.

    Args:
        file_name: Nombre del archivo de logs a procesar

    Returns:
        Dict con resultados del procesamiento
    """
    logger.info(f"Iniciando procesamiento completo para: {file_name}")

    try:
        file_path = os.path.join(STORAGE_PATH, file_name)

        # Patrón para extraer información de logs
        log_pattern = re.compile(
            r'(?P<timestamp>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:\.\d+)?)\s+'
            r'(?P<level>[A-Z]+)\s+'
            r'(?:(?P<source>[^:]+):\s+)?'
            r'(?P<message>.*)'
        )

        # Estadísticas iniciales
        stats = {
            "entry_count": 0,
            "error_count": 0,
            "warning_count": 0,
            "info_count": 0,
            "file_name": file_name
        }

        # Conectar a la base de datos
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Procesar línea por línea el archivo de logs
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                stats["entry_count"] += 1

                # Extraer información usando regex
                try:
                    match = log_pattern.match(line)
                    if match:
                        timestamp = match.group('timestamp')
                        level = match.group('level')
                        source = match.group('source') or "unknown"
                        message = match.group('message')
                    else:
                        # Fallback para líneas que no coinciden con el patrón
                        timestamp = ""
                        level = "UNKNOWN"
                        source = "unknown"
                        message = line

                    # Actualizar estadísticas según el nivel
                    level_upper = level.upper()
                    if "ERROR" in level_upper:
                        stats["error_count"] += 1
                    elif "WARN" in level_upper:
                        stats["warning_count"] += 1
                    elif "INFO" in level_upper:
                        stats["info_count"] += 1

                    # Guardar entrada en la base de datos
                    cursor.execute(
                        "INSERT INTO log_entries (timestamp, level, source, message, file_name) VALUES (?, ?, ?, ?, ?)",
                        (timestamp, level, source, message, file_name)
                    )
                except Exception as e:
                    logger.error(f"Error al procesar línea: {e}")

        # Guardar estadísticas en la base de datos
        cursor.execute(
            "INSERT INTO log_stats (file_name, entry_count, error_count, warning_count, info_count) VALUES (?, ?, ?, ?, ?)",
            (file_name, stats["entry_count"], stats["error_count"], stats["warning_count"], stats["info_count"])
        )
        conn.commit()
        conn.close()

        # Create chain of tasks that execute in order
        workflow = chain(
            clean_log_data.si(file_name),
            analyze_log_patterns.si(file_name),
            generate_log_report.si(file_name)
        )

        # Start the workflow asynchronously
        result = workflow.apply_async()

        return {
            "file_name": file_name,
            "status": "processing",
            "task_id": result.id
        }

    except Exception as e:
        logger.error(f"Error en el procesamiento completo: {e}")
        return {
            "file_name": file_name,
            "status": "error",
            "message": str(e)
        }


if __name__ == "__main__":
    logger.info("Módulo de workers cargado. Utiliza Celery para iniciar los workers.")