import asyncio
import os
import sys
from dotenv import load_dotenv


HOST = os.getenv("HOST")
PORT = int(os.getenv("PORT"))

data_to_send = " ".join(sys.argv[1:])

async def send_data():
    reader, writer = await asyncio.open_connection(HOST, PORT)
    writer.write(data_to_send.encode())
    await writer.drain()

    response = await reader.read(1024)
    print(f"Respuesta del servidor: {response.decode()}")
    writer.close()
    await writer.wait_closed()

if __name__ == "__main__":
    asyncio.run(send_data())