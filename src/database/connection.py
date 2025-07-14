from typing import List, Any, LiteralString, Dict
from contextlib import asynccontextmanager

import asyncpg as pg

from src.config import config

from sqlalchemy.dialects import postgresql

db_config = config.database
conn_string = f"postgresql://{db_config.username}:{db_config.password}@{db_config.host}:{db_config.port}/{db_config.name}"

class DBConnector:
    def __init__(self):
        self.pool: pg.Pool = None

    async def create_pool(self) -> None:
        self.pool = await pg.create_pool(conn_string)

    @asynccontextmanager
    async def get_connection(self):
        async with self.pool.acquire() as conn:
            yield conn

    # USE ONLY FOR DEVELOPING YOUR DATABASE. THIS WILL DELETE ALL DATA IN TABLES #
    async def reset_tables(self):
        async with self.get_connection() as conn:
            async with conn.transaction():
                await conn.execute(db_config.delete_schema)
                await conn.execute(db_config.schema)

    async def create_tables_if_not_exists(self):
        async with self.get_connection() as conn:
            async with conn.transaction():
                await conn.execute(db_config.schema)

    async def close(self):
        await self.pool.close()

# (Brief) Custom Statement Generator - Adapted statement generator with placeholders for asyncpg queries.
# (Usage) Can be used as fast generator for queries.
#
class StmtGenerator:
    def __init__(self, model):
        self.sql_parts: List[str] = []
        self.model = model

    def select(self, *args: str) -> "StmtGenerator":
        if not args: args = ("*", )

        self.sql_parts.append(f"SELECT {','.join(args)} FROM {self.model.__table_name__} ")
        return self

    def where(self, *args: str) -> "StmtGenerator":
        self.sql_parts.append(f"WHERE {' and '.join(f'{x}=${i}' for i, x in enumerate(args, 1))} ")
        return self

    def order_by(self,
                 asc: List[str] | tuple = (),
                 desc: List[str] | tuple = (),
                 nulls_last: List[str] | tuple = (),
                 nulls_first: List[str] | tuple = ()
    ) -> "StmtGenerator":
        self.sql_parts.append(f"ORDER BY {','.join(
            [f'{x} DESC' for x in desc] + 
            [f'{x} ASC' for x in asc] + 
            [f'{x} NULLS LAST' for x in nulls_last] +
            [f'{x} NULLS FIRST' for x in nulls_first]
        )}")
        return self

    def insert(self, *args: str) -> "StmtGenerator":
        self.sql_parts.append(f"INSERT INTO {self.model.__table_name__} ({','.join(args)}) VALUES ({','.join([f'${i}' for i in range(1, len(args)+1)])}) ")
        return self

    def delete(self) -> "StmtGenerator":
        self.sql_parts.insert(0, f"DELETE FROM {self.model.__table_name__} ")
        return self

    def update(self, *args: str) -> "StmtGenerator":
        self.sql_parts.insert(0, f"UPDATE {self.model.__table_name__} SET {','.join([f'{x}=${i}' for i, x in enumerate(args, 1)])} ")
        return self

    def update_all(self, exceptions:  tuple[Any, ...] | List[str]) -> "StmtGenerator":
        self.sql_parts.insert(0, f"UPDATE {self.model.__table_name__} SET {','.join([f'{x}=${i}' for i, x in enumerate(self.model.__fields__, 1) if x not in exceptions])} ")
        return self

    def group_by(self, *args: str):
        self.sql_parts.append(f"GROUP BY {[','.join(args)]}")
        return self

    def limit(self, lim: int):
        self.sql_parts.append(f"LIMIT {lim}")
        return self

    def count(self):
        self.sql_parts.append(f'SELECT COUNT(*) FROM {self.model.__table_name__}')
        return self

    def sql(self) -> str:
        final_string: str = ''.join(self.sql_parts)
        self.sql_parts.clear()
        return final_string

    async def value(self, conn: pg.Connection, *args: Any):
        return await conn.fetchval(''.join(self.sql_parts), args)

    async def first(self, conn: pg.Connection, *args: Any):
        return self.model(**await conn.fetchrow(''.join(self.sql_parts), *args))

    async def all(self, conn: pg.Connection, *args: Any):
        return [self.model(**x) for x in conn.fetch(''.join(self.sql_parts), *args)]

class StmtExt:
    @staticmethod
    def sum(param: str = '*', as_param: str = "sum") -> str:
        return f"SUM({param}) AS {as_param}"

    @staticmethod
    def max(param:str, as_param:str = "max") -> str:
        return f"MAX({param}) AS {as_param}"

    @staticmethod
    def min(param: str, as_param: str = "min") -> str:
        return f"MIN({param}) AS {as_param}"

    @staticmethod
    def avg(param: str = '*', as_param: str = "avg") -> str:
        return f"AVG({param}) AS {as_param}"

    @staticmethod
    def count(param: str = '*', as_param: str = "count") -> str:
        return f"COUNT({param}) AS {as_param}"

    @staticmethod
    def and_op(*params: str, braces: bool = False) -> str:
        sql = ' AND '.join(params)
        return f"({sql})" if braces else sql

    @staticmethod
    def or_op(*params: str, braces: bool = False) -> str:
        sql = ' OR '.join(params)
        return f"({sql})" if braces else sql
