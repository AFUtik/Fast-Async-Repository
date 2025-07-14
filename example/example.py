import asyncio

from example_entity import UserExample
from example_repository import UserRepository
from src.database.connection import DBConnector

from src.config import config

async def main():
    db = DBConnector()
    await db.create_pool()
    await db.create_tables_if_not_exists()

    async with db.get_connection() as conn:
        new_user = UserExample(id=65, tag='Someh5Tag934164')
        print(new_user.created_at)
        if not await UserRepository.exists_by_id(conn, 65):


            await UserRepository.insert(conn, new_user)

        user: UserExample = await UserRepository.find_user(conn, 65)
        print(user.created_at)

    await db.close()

if __name__ == "__main__":
    asyncio.run(main())