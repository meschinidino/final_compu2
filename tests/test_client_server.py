import asyncio
import pytest
from server import main as server_main


@pytest.mark.asyncio
async def test_client_server():
    server_task = asyncio.create_task(server_main())
    await asyncio.sleep(1)  # Esperar que el servidor inicie

    reader, writer = await asyncio.open_connection('127.0.0.1', 8888)
    message = "Test Message"
    writer.write(message.encode())
    await writer.drain()

    response = await reader.read(1024)
    assert response.decode() == f"Servidor recibió: {message}"

    writer.close()
    await writer.wait_closed()
    server_task.cancel()