from src.database.cache import LRUCache, TTLCache
from src.database.repository import Repository, repository, cache_entity
from src.database.connection import to_sql
from src.database.repository import query, select, bindparam

from example_entity import UserExample

@repository(UserExample)
class UserRepository(Repository):
    @classmethod
    @cache_entity(to_sql(select(UserExample).where(UserExample.id == bindparam('id'))), TTLCache())
    def find_user(cls, connection, id: int): pass