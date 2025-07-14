import asyncio

from example_entity import UserExample
from example_repository import UserRepository
from src.database.connection import DBConnector

async def main():
    db = DBConnector()
    await db.create_pool()
    await db.create_tables_if_not_exists()

    async with db.get_connection() as conn:
        if not await UserRepository.exists_by_id(conn, 1):
            new_user = UserExample(id=1, tag='SomeTag')

            await UserRepository.insert(conn, new_user)

        user: UserExample = await UserRepository.find_user(conn, 1)
        print(user.id)

    await db.close()

if __name__ == "__main__":
    asyncio.run(main())