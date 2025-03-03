import asyncio
import os
from dotenv import load_dotenv

HOST = os.getenv("HOST")
PORT = int(os.getenv("PORT"))

async def handle_client(reader, writer):
    data = await reader.read(1024)
    message = data.decode()
    addr = writer.get_extra_info('peername')
    print(f"Recibido de {addr}: {message}")

    response = f"Servidor recibió: {message}"
    writer.write(response.encode())
    await writer.drain()
    writer.close()

async def main():
    server = await asyncio.start_server(handle_client, HOST, PORT)
    addr = server.sockets[0].getsockname()
    print(f"Servidor en {addr}")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())