from src.database.cache import LRUCache, TTLCache
from src.database.repository import Repository, repository, cache_entity
from src.database.connection import StmtGenerator
from src.database.repository import query

from example_entity import UserExample

@repository(UserExample)
class UserRepository(Repository):
    @classmethod
    @cache_entity(StmtGenerator(model=UserExample).select().where('id').sql(), LRUCache())
    def find_user(cls, connection, id: int): pass