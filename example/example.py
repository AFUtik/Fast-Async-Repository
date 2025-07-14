import asyncio

from example_entity import UserExample
from example_repository import UserRepository
from src.database.connection import DBConnector

import time

async def main():
    connector: DBConnector = DBConnector()
    await connector.create_pool()

    start = time.perf_counter()
    async with connector.get_connection() as conn:
        for _ in range(5000):
            await UserRepository.update(conn, UserExample(45, "6453"))
    end = time.perf_counter()
    print(f"Время выполнения: {end - start:.4f} секунд")

    await connector.close()

if __name__ == "__main__":
    asyncio.run(main())