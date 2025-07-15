from dataclasses import astuple
from typing import Set, List

import asyncpg
from asyncpg import Connection

from src.asyncrepository.connection import StmtGenerator
from src.asyncrepository.expections import DatabaseError
from src.asyncrepository.entity import BaseEntity
from src.asyncrepository.cache import LRUCache, TTLCache

# Class Decorator - Used to statically generate common queries for the concrete repository.
def repository(model: BaseEntity.__class__ = None):
    def decorator(cls):
        if model is None: return cls

        cls.__model__ = model

        stmt = StmtGenerator(model=model)

        cls.__find_all_query__     = stmt.select().sql()
        cls.__find_by_id_query__   = stmt.select().where(model.__key__).sql()
        cls.__insert_query__       = stmt.insert(model.__key__).sql()
        cls.__delete_by_id_query__ = stmt.delete().where(model.__key__).sql()
        cls.__update_query__       = stmt.update_all(exceptions=model.__key__).where(model.__key__).sql()
        cls.__count_query__        = stmt.count().sql()
        return cls
    return decorator

# (Brief) Takes entity from the cache if exists. Checks whether entity is old by comparing current time
#         and time that was inserted in hash table.
# (Usage) Use if entity should not update some timeout that specified in the cache.
#
# (Params)
#   sql (string) - SQL Query
#   cache (LRUCache or LFUCache) - Cache class
#
def query_ttl(
        model: BaseEntity.__class__,
        sql: str,
        cache_capacity: int = 256,
        cache_expire: float = 60.0
):
    def decorator(func):
        cache = TTLCache(cache_capacity, cache_expire)
        async def wrapper(cls, conn: Connection, *args, **kwargs):
            try:
                cached: BaseEntity = cache.get(args)
                if cached is not None: return cached

                row = await conn.fetchrow(sql, *args)
                if row is None: return None

                entity = model(**row)

                cache[args] = entity
                return entity
            except asyncpg.PostgresError as e:
                raise DatabaseError() from e
        return wrapper
    return decorator

# (Brief) Takes entity from the cache if exists. Checks whether entity is old by searching in hash set [Demands the only identifier from the entity].
# (Usage) If you delete or change entities frequently, it can be not effective.
#
# (Params)
#   model (class of Model)
#   sql (string) - SQL Query
#   cache_key (name of entity's identifier)
#   cache (Cache class)
#
def query_lru(
        model: BaseEntity.__class__,
        sql: str,
        input_id: bool = False,
        cache_key: str="",
        cache_capacity: int = 256,
):
    def decorator(func):
        cache = LRUCache(cache_capacity)
        async def wrapper(cls, conn: Connection, *args, **kwargs):
            try:
                cached: BaseEntity = cache.get(args)
                if cached is not None and (getattr(cached, cache_key) if not input_id else args): return cached

                row = await conn.fetchrow(sql, *args)
                if row is None: return None

                entity = model(**row)

                cls.__cache_id__.add(getattr(entity, cache_key) if not input_id else args)
                cache[args] = entity
                return entity
            except asyncpg.PostgresError as e:
                raise DatabaseError() from e
        return wrapper
    return decorator

def query_all_ttl(
        model: BaseEntity.__class__,
        sql: str,
        cache_capacity: int = 256,
        cache_expire: float = 60.0
):
    def decorator(func):
        cache = TTLCache(cache_capacity, cache_expire)
        async def wrapper(cls, conn: Connection, *args, **kwargs):
            try:
                cached: List[BaseEntity] = cache.get(args)
                if cached is not None: return cached

                entities = [model(**x) for x in await conn.fetch(sql, *args)]
                cache[args] = entities
                return entities
            except asyncpg.PostgresError as e:
                raise DatabaseError() from e
        return wrapper
    return decorator

def query_all_lru(
        model: BaseEntity.__class__,
        sql: str,
        input_id: bool = False,
        cache_key: str = "",
        cache_capacity: int = 256
):
    def decorator(func):
        cache = LRUCache(cache_capacity)
        async def wrapper(cls, conn: Connection, *args, **kwargs):
            try:
                cached: List[BaseEntity] = cache.get(args)
                if cached is not None:
                    if all(getattr(entity, cache_key) if not input_id else args in cls.__cache_id__ for entity in cached):
                        return cached

                entities = [model(**x) for x in await conn.fetch(sql, *args)]
                for entity in entities:
                    cls.__cache_id__.add(getattr(entity, cache_key) if not input_id else args)
                cache[args] = entities
                return entities
            except asyncpg.PostgresError as e:
                raise DatabaseError() from e
        return wrapper
    return decorator

# (Brief) Fetches the entity from a query. Does not use cache.
def query(model: BaseEntity.__class__,sql: str):
    def decorator(func):
        async def wrapper(cls, conn: Connection, *args, **kwargs):
            try:
                return model(*await conn.fetchrow(sql, *args))
            except asyncpg.PostgresError as e:
                raise DatabaseError() from e
        return wrapper
    return decorator

# (Brief) Fetches list of entities from a query. Does not use cache.
def query_all(sql: str):
    def decorator(func):
        async def  wrapper(cls, conn: Connection, *args, **kwargs):
            try:
                return [cls.__model__(**x) for x in await conn.fetch(sql, *args)]
            except asyncpg.PostgresError as e:
                raise DatabaseError() from e
        return wrapper
    return decorator

# Method Decorator - Executes SQL queries without returning anything.
def execute(sql: str):
    def decorator(func):
        async def wrapper(cls, conn: Connection, *args, **kwargs):
            try:
                async with conn.transaction():
                        return conn.execute(sql, *args)
            except asyncpg.PostgresError as e:
                raise DatabaseError() from e
        return wrapper
    return decorator

class Repository:
    # Static fields
    __cache_id__: Set[tuple] = set() # primary key(id)
    __model__: BaseEntity.__class__

    # Default queries
    __find_all_query__:     str
    __find_by_id_query__:   str
    __exists_by_id_query__: str
    __insert_query__:       str
    __delete_by_id_query__: str
    __update_query__:       str
    __count_query__:        str

    @classmethod
    async def find_all(cls, conn: Connection) -> List[BaseEntity]:
        try:
            return [cls.__model__(**x) for x in await conn.fetch(cls.__find_all_query__)]
        except asyncpg.PostgresError as e: raise DatabaseError() from e

    @classmethod
    async def find_by_id(cls, conn: Connection, *id: int | str | tuple) -> BaseEntity | None:
        try:
            row = await conn.fetchrow(cls.__find_by_id_query__, *id)
            return cls.__model__(**row) if row else None
        except asyncpg.PostgresError as e: raise DatabaseError() from e

    @classmethod
    async def exists_by_id(cls, conn: Connection, id: int | str) -> bool:
        try:
            return await conn.fetchrow(cls.__find_by_id_query__, id) is not None
        except asyncpg.PostgresError as e: raise DatabaseError() from e

    @classmethod
    async def insert(cls, conn: Connection, entity: BaseEntity) -> None:
        try:
            async with conn.transaction():
                await conn.execute(cls.__insert_query__, *astuple(entity))
        except asyncpg.PostgresError as e: raise DatabaseError() from e

    @classmethod
    async def delete_by_id(cls, conn: Connection, *id) -> None:
        try:
            async with conn.transaction():
                cls.__cache_id__.discard(id)
                await conn.execute(cls.__delete_by_id_query__, id)
        except asyncpg.PostgresError as e: raise DatabaseError() from e

    @classmethod
    async def update(cls, conn: Connection, entity: BaseEntity) -> None:
        try:
            async with conn.transaction():
                cls.__cache_id__.discard(entity.primary_key())
                await conn.execute(cls.__update_query__, *astuple(entity))
        except asyncpg.PostgresError as e: raise DatabaseError() from e

    @classmethod
    async def count(cls, conn: Connection) -> int:
        try:
            return await conn.fetchval(cls.__count_query__)
        except asyncpg.PostgresError as e: raise DatabaseError() from e