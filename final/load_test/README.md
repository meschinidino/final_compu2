# Prueba de Carga para el Servidor de Logs

Este script de prueba de carga permite evaluar el rendimiento de un servidor de logs simulando múltiples clientes concurrentes que envían archivos de logs. Además, permite generar archivos de logs aleatorios con diferentes tamaños y tasas de error.

## Características
- Simula múltiples clientes enviando archivos de logs al servidor.
- Genera archivos de log con diferentes niveles de mensajes (INFO, DEBUG, WARNING, ERROR, CRITICAL).
- Mide el tiempo de respuesta y la tasa de éxito de las conexiones.
- Permite especificar un archivo de log existente o generar nuevos archivos aleatorios.

## Requisitos
- Python 3.7 o superior
- Dependencias instaladas (pueden instalarse con `pip install -r requirements.txt` si se proporciona un archivo de requisitos).
- Un servidor de logs en ejecución que pueda recibir los archivos.

## Uso

### Generar y enviar 10 archivos de log aleatorios
```sh
python load_test.py --clients 10
```

### Especificar un archivo de log existente para todos los clientes
```sh
python load_test.py --clients 5 --file tu_archivo.log
```

### Configurar el host y puerto del servidor
```sh
python load_test.py --clients 8 --host 192.168.1.100 --port 9000
```

## Parámetros
| Parámetro       | Descripción |
|----------------|------------|
| `--clients`    | Número de clientes concurrentes a simular. |
| `--file`       | Ruta a un archivo de logs existente o `generate` para crear nuevos archivos. |
| `--host`       | Dirección del servidor de logs (por defecto `127.0.0.1`). |
| `--port`       | Puerto del servidor de logs (por defecto `8888`). |

## Ejemplo de Salida
Al ejecutar una prueba con 5 clientes generando archivos aleatorios, podrías obtener una salida como esta:
```
2025-03-10 12:34:56 - load_test - INFO - Iniciando prueba de carga con 5 clientes simultáneos
2025-03-10 12:34:57 - load_test - INFO - Generado archivo de prueba: test_logs/test_log_20250310-123456_client_1.log
...
2025-03-10 12:35:10 - load_test - INFO - RESULTADOS DE LA PRUEBA DE CARGA:
Total de clientes: 5
Conexiones exitosas: 4
Conexiones fallidas: 1
Duración total: 12.34 segundos
Tiempo promedio por cliente: 2.47 segundos
Tiempo mínimo: 1.89 segundos
Tiempo máximo: 3.21 segundos
```
