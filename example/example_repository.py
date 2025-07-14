import asyncpg

from src.database.repository import Repository, repository
from src.database.connection import to_sql
from src.database.repository import query, select, bindparam

from example_entity import UserExample

@repository(UserExample)
class UserRepository(Repository):
    @classmethod
    @query(to_sql(select(UserExample).where(UserExample.id == bindparam('id'))))
    def find_user(cls, conn: asyncpg.Connection, id: int): pass