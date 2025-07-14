from dataclasses import dataclass, astuple
from functools import singledispatch
from typing import Any, LiteralString, Callable, Set, Coroutine

import asyncpg
from asyncpg import Connection

from sqlalchemy import select, insert, delete, update, bindparam, func as sqlfunc
from src.database.connection import StmtGenerator, to_sql

from src.expections import DatabaseError
from src.database.entity import *

from src.database.cache import LRUCache, LFUCache, TTLCache

# Class Decorator - Used to statically generate common queries for the concrete repository.
def repository(model: BaseEntity.__class__):
    def decorator(cls):
        model.__pk_attrs__ = tuple(col.key for col in model.__mapper__.primary_key)
        cls._model = model

        table = model.__table__

        cls._find_all_query = to_sql(select(table))
        cls._find_by_id_query = to_sql(select(table).where(
            *[col == bindparam(col.name) for col in table.primary_key.columns]
        ))
        cls._exists_by_id_query = to_sql(select(sqlfunc.exists(select(table).where(
            *[col == bindparam(col.name) for col in table.primary_key.columns]
        ).scalar_subquery())))
        cls._insert_query = to_sql(insert(table))
        cls._delete_by_id_query = to_sql(delete(table).where(
            *[col == bindparam(col.name) for col in table.primary_key.columns]
        ))
        cls._update_query = to_sql(update(table))
        cls._count_query = to_sql(select(sqlfunc.count()).select_from(table))
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

                data = cls._model(**await conn.fetchrow(sql, *args))
                cache[args] = data
                return data
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
                if cached is not None and cached.primary_key() in cls._cached_id: return cached

                entity = cls._model(**await conn.fetchrow(sql, *args))
                cls._cached_id.add(entity.primary_key())
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

                entities = [cls._model(**x) for x in await conn.fetch(sql, *args)]
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
                    if all(entity.__pk_values__ in cls._cached_id for entity in cached):
                        return cached

                entities = [cls._model(**x) for x in await conn.fetch(sql, *args)]
                for entity in entities:
                    cls._cached_id.add(entity.primary_key())
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
                return cls._model(**await conn.fetchrow(sql, *args))
            except asyncpg.PostgresError as e:
                raise DatabaseError() from e
        return wrapper
    return decorator

# (Brief) Fetches list of entities from a query. Does not use cache.
def query_all(sql: str):
    def decorator(func):
        async def  wrapper(cls, conn: Connection, *args, **kwargs):
            try:
                return [cls._model(**x) for x in await conn.fetch(sql, *args)]
            except asyncpg.PostgresError as e:
                raise DatabaseError() from e
        return wrapper
    return decorator

# Method Decorator - Executes SQL queries without returning anything.
def execute(sql: str, transaction: bool = True):
    def decorator(func):
        async def wrapper(cls, conn: Connection, *args, **kwargs):
            try:
                if transaction:
                    async with conn.transaction():
                        return conn.execute(sql, *args)
                else:
                    return conn.execute(sql, *args)
            except asyncpg.PostgresError as e:
                raise DatabaseError() from e
        return wrapper
    return decorator

class Repository:
    # Static fields
    _model: BaseEntity.__class__

    # Default queries
    _find_all_query:       LiteralString
    _find_by_id_query:     LiteralString
    _exists_by_id_query:   LiteralString
    _insert_query:         LiteralString
    _delete_by_id_query:   LiteralString
    _update_query:         LiteralString
    _count_query:          LiteralString

    _cached_id: Set[tuple] = set() # primary key(id)

    @classmethod
    async def find_all(cls, conn: Connection) -> List[BaseEntity]:
        try:
            return [cls._model(**x) for x in await conn.fetch(cls._find_all_query)]
        except asyncpg.PostgresError as e: raise DatabaseError() from e

    @classmethod
    async def find_by_id(cls, conn: Connection, *id: int | str | tuple) -> BaseEntity | None:
        try:
            return cls._model(**await conn.fetchrow(cls._find_by_id_query, *id))
        except asyncpg.PostgresError as e: raise DatabaseError() from e

    @classmethod
    async def exists_by_id(cls, conn: Connection, id: int | str) -> bool:
        try:
            return await conn.fetchval(cls._exists_by_id_query, id)
        except asyncpg.PostgresError as e: raise DatabaseError() from e

    @classmethod
    async def insert(cls, conn: Connection, entity: BaseEntity) -> None:
        try:
            async with conn.transaction():
                await conn.execute(cls._insert_query, *tuple(getattr(entity, col.name) for col in entity.__table__.columns))
        except asyncpg.PostgresError as e: raise DatabaseError() from e

    @classmethod
    async def delete_by_id(cls, conn: Connection, id) -> None:
        try:
            async with conn.transaction():
                cls._cached_id.discard(id)
                await conn.execute(cls._delete_by_id_query, id)
        except asyncpg.PostgresError as e: raise DatabaseError() from e

    @classmethod
    async def update(cls, conn: Connection, entity: BaseEntity) -> None:
        try:
            async with conn.transaction():
                cls._cached_id.discard(entity.primary_key())
                await conn.execute(cls._update_query, *entity.__dict__.values())
        except asyncpg.PostgresError as e: raise DatabaseError() from e

    @classmethod
    async def count(cls, conn: Connection) -> int:
        try:
            return await conn.fetchval(cls._count_query)
        except asyncpg.PostgresError as e: raise DatabaseError() from e