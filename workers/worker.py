# workers.py
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

# Cargar variables de entorno
load_dotenv()
BROKER_URL = os.getenv("BROKER_URL", "pyamqp://guest@localhost//")
DB_PATH = os.getenv("DB_PATH", "logs.db")

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

        # Obtener entradas de log para el archivo
        cursor.execute(
            "SELECT id, timestamp, level, source, message FROM log_entries WHERE file_name = ?",
            (file_name,)
        )
        entries = cursor.fetchall()

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
        error_patterns = [
            r"(?i)exception|error|fail|crash",
            r"(?i)timeout|timed out",
            r"(?i)connection (?:refused|reset|closed)",
            r"(?i)out of (?:memory|resources)",
            r"(?i)permission denied|unauthorized",
        ]

        # Contadores para análisis
        pattern_counts = {pattern: 0 for pattern in error_patterns}
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
            if level and "ERROR" in level.upper():
                error_messages.append(message)

                for pattern in error_patterns:
                    if re.search(pattern, message):
                        pattern_counts[pattern] += 1

        # Formatear resultados
        results["patterns"] = {
            "connection_issues": pattern_counts[r"(?i)connection (?:refused|reset|closed)"],
            "timeouts": pattern_counts[r"(?i)timeout|timed out"],
            "resources": pattern_counts[r"(?i)out of (?:memory|resources)"],
            "permissions": pattern_counts[r"(?i)permission denied|unauthorized"],
            "general_errors": pattern_counts[r"(?i)exception|error|fail|crash"]
        }

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
            "CREATE TABLE IF NOT EXISTS log_analysis (id INTEGER PRIMARY KEY AUTOINCREMENT, file_name TEXT, results TEXT, analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
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
    """
    Genera un informe estadístico basado en los logs procesados.

    Args:
        file_name: Nombre del archivo de logs a analizar

    Returns:
        Dict con el informe estadístico
    """
    logger.info(f"Generando informe para: {file_name}")

    try:
        # Conectar a la base de datos
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Obtener estadísticas básicas
        cursor.execute(
            "SELECT entry_count, error_count, warning_count, info_count FROM log_stats WHERE file_name = ?",
            (file_name,)
        )
        stats = cursor.fetchone()

        if not stats:
            return {"status": "error", "message": f"No se encontraron estadísticas para {file_name}"}

        entry_count, error_count, warning_count, info_count = stats

        # Calcular métricas adicionales
        error_rate = (error_count / entry_count * 100) if entry_count > 0 else 0
        warning_rate = (warning_count / entry_count * 100) if entry_count > 0 else 0

        # Crear informe
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
            "health_status": "critical" if error_rate > 10 else "warning" if error_rate > 5 else "healthy"
        }

        # Obtener análisis de patrones si existe
        cursor.execute(
            "SELECT results FROM log_analysis WHERE file_name = ? ORDER BY analyzed_at DESC LIMIT 1",
            (file_name,)
        )
        analysis_row = cursor.fetchone()

        if analysis_row:
            pattern_analysis = json.loads(analysis_row[0])
            # Añadir datos del análisis de patrones al informe
            report["patterns"] = pattern_analysis.get("patterns", {})
            report["common_errors"] = pattern_analysis.get("common_errors", [])
            report["time_distribution"] = pattern_analysis.get("time_distribution", {})

        # Guardar informe en la base de datos
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS log_reports (id INTEGER PRIMARY KEY AUTOINCREMENT, file_name TEXT, report TEXT, generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        cursor.execute(
            "INSERT INTO log_reports (file_name, report) VALUES (?, ?)",
            (file_name, json.dumps(report))
        )
        conn.commit()

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
@app.task
def process_log_file(file_name: str) -> Dict[str, Any]:
    """
    Realiza el procesamiento completo de un archivo de logs.

    Args:
        file_name: Nombre del archivo de logs a procesar

    Returns:
        Dict con resultados del procesamiento
    """
    logger.info(f"Iniciando procesamiento completo para: {file_name}")

    # Ejecutar tareas en secuencia
    try:
        # 1. Limpiar datos
        clean_results = clean_log_data.delay(file_name).get()

        # 2. Analizar patrones
        analysis_results = analyze_log_patterns.delay(file_name).get()

        # 3. Generar informe
        report_results = generate_log_report.delay(file_name).get()

        # Resultados combinados
        results = {
            "file_name": file_name,
            "processed_at": datetime.now().isoformat(),
            "clean_results": clean_results,
            "analysis_results": analysis_results,
            "report_results": report_results,
            "status": "success"
        }

        logger.info(f"Procesamiento completo finalizado para {file_name}")
        return results

    except Exception as e:
        logger.error(f"Error en el procesamiento completo: {e}")
        return {
            "file_name": file_name,
            "status": "error",
            "message": str(e)
        }


if __name__ == "__main__":
    logger.info("Módulo de workers cargado. Utiliza Celery para iniciar los workers.")