from dataclasses import dataclass, astuple
from functools import singledispatch
from typing import Any, LiteralString, Callable, Set, Coroutine

import asyncpg
from asyncpg import Connection, Record

from src.database.connection import StmtGenerator

from src.expections import DatabaseError
from src.database.entity import *

from src.database.cache import LRUCache, LFUCache, TTLCache

# Class Decorator - Used to statically generate common queries for the concrete repository.
def repository(model: BaseEntity.__class__):
    def decorator(cls):
        cls.__model__ = model

        stmt = StmtGenerator(model=model)

        cls.__find_all_query__     = stmt.select().sql()
        cls.__find_by_id_query__   = stmt.select().where(*model.__pk_attrs__).sql()
        cls.__insert_query__       = stmt.insert(*model.__fields__).sql()
        cls.__delete_by_id_query__ = stmt.delete().where(*model.__pk_attrs__).sql()
        cls.__update_query__       = stmt.where(*model.__pk_attrs__).update_all(exceptions=model.__pk_attrs__).sql()
        cls.__count_query__        = stmt.count().sql()

        print(cls.__update_query__)
        return cls
    return decorator

@singledispatch
def cache_entity(sql, cache):
    def decorator(func):
        async def wrapper(cls, *args, **kwargs):
            return func(cls, *args, **kwargs)
        return wrapper
    return decorator

@singledispatch
def cache_entities(sql, cache):
    def decorator(func):
        async def wrapper(cls, *args, **kwargs):
            return func(cls, *args, **kwargs)
        return wrapper
    return decorator

# (Brief) Takes entity from the cache if exists. Checks whether entity is old by comparing current time
#         and time that was inserted in hash table.
# (Usage) Use if entity should not update some timeout that specified in the cache.
#
# (Params)
#   sql (string) - SQL Query
#   cache (LRUCache or LFUCache) - Cache class
#
@cache_entity.register
def _(sql: str, cache: TTLCache):
    def decorator(func):
        async def wrapper(cls, conn: Connection, *args, **kwargs):
            try:
                cached: BaseEntity = cache.get(args)
                if cached is not None: return cached

                row = await conn.fetchrow(sql, *args)
                if row is None: return None

                entity = cls.__model__(**row)

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
#   sql (string) - SQL Query
#   cache (LRUCache or LFUCache) - Cache class
#
@cache_entity.register
def _(sql: str, cache: LRUCache):
    def decorator(func):
        async def wrapper(cls, conn: Connection, *args, **kwargs):
            try:
                cached: BaseEntity = cache.get(args)
                if cached is not None and cached.primary_key() in cls.__cache_id__: return cached

                row = await conn.fetchrow(sql, *args)
                if row is None: return None

                entity = cls.__model__(**row)

                cls.__cache_id__.add(entity.primary_key())
                cache[args] = entity
                return entity
            except asyncpg.PostgresError as e:
                raise DatabaseError() from e
        return wrapper
    return decorator

@cache_entities.register
def _(sql: str, cache: TTLCache = TTLCache()):
    def decorator(func):
        async def wrapper(cls, conn: Connection, *args, **kwargs):
            try:
                cached: List[BaseEntity] = cache.get(args)
                if cached is not None: return cached

                entities = [cls.__model__(**x) for x in await conn.fetch(sql, *args)]
                cache[args] = entities
                return entities
            except asyncpg.PostgresError as e:
                raise DatabaseError() from e
        return wrapper
    return decorator

@cache_entities.register
def _(sql: str, cache: LRUCache):
    def decorator(func):
        async def wrapper(cls, conn: Connection, *args, **kwargs):
            try:
                cached: List[BaseEntity] = cache.get(args)
                if cached is not None:
                    if all(entity.__pk_values__ in cls.__cache_id__ for entity in cached):
                        return cached

                entities = [cls.__model__(**x) for x in await conn.fetch(sql, *args)]
                for entity in entities:
                    cls.__cache_id__.add(entity.primary_key())
                cache[args] = entities
                return entities
            except asyncpg.PostgresError as e:
                raise DatabaseError() from e
        return wrapper
    return decorator

# (Brief) Fetches the entity from a query. Does not use cache.
def query(sql: str):
    def decorator(func):
        async def wrapper(cls, conn: Connection, *args, **kwargs):
            try:
                return cls.__model__(**await conn.fetchrow(sql, *args))
            except asyncpg.PostgresError as e:
                raise DatabaseError() from e
        return wrapper
    return decorator

# (Brief) Fetches list of entities from a query. Does not use cache.
def query_all(sql: str):
    def decorator(func):
        async def  wrapper(cls, conn: Connection, *args, **kwargs):
            try:
                return [cls.__model__(**x) for x in await conn.fetch(sql, *args) if x is not None]
            except asyncpg.PostgresError as e:
                raise DatabaseError() from e
        return wrapper
    return decorator

# Method Decorator - Executes SQL queries without returning anything.
def execute(sql: str, transaction: bool = True):
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
            row: Record = await conn.fetchrow(cls.__find_by_id_query__, *id)
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