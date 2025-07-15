from src.database.cache import LRUCache, TTLCache
from src.database.repository import Repository, repository, query_lru, query_ttl
from src.database.connection import StmtGenerator
from src.database.repository import query

from example_entity import UserExample, TimestampEntity

@repository(UserExample)
class UserRepository(Repository):
    @classmethod # Decorator to make method static
    @query_lru(
        model=UserExample,
        sql=StmtGenerator(model=UserExample).select().where('id').sql(), # Sql query compiled to string.
        input_id=True,
        cache_key="id", # CACHE KEY - Name of Entity's identifier
        cache_capacity=256 # Optional Parameter, you can create cache with custom capacity.
    )
    def find_user(cls, connection, id: int): pass

    @classmethod
    @query_ttl(
        model=TimestampEntity,
        sql=StmtGenerator(model=UserExample).select('created_at').where('id').sql(),
        cache_capacity=256,
        cache_expire=60.0
    )
    def fetch_date_by_user_id(cls, id: int): pass