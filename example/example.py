import asyncio
import time

from example_repository import UserRepository
from src.database.connection import DBConnector


async def main():
    connector: DBConnector = DBConnector()
    await connector.create_pool()

    start = time.perf_counter()
    async with connector.get_connection() as conn:
        for _ in range(5000):
            date = await UserRepository.find_user(conn, 45)
    print(date.created_at)
    end = time.perf_counter()
    print(f"Время выполнения: {end - start:.4f} секунд")

    await connector.close()

if __name__ == "__main__":
    asyncio.run(main())