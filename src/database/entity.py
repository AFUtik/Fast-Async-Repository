from typing import List, Optional, Dict
from datetime import datetime

from typing import TypeVar, Generic

T = TypeVar('T')

class Timestamp: pass

class Default(Generic[T]):
    def __init__(self, item):
        self.item = item

    def __repr__(self):
        return f"PrimaryKey[{self.item}]"

    @classmethod
    def __class_getitem__(cls, item):
        return cls(item)

class PrimaryKey(Generic[T]):
    def __init__(self, item):
        self.item = item

    def __repr__(self):
        return f"PrimaryKey[{self.item}]"

    @classmethod
    def __class_getitem__(cls, item):
        return cls(item)

class Varchar(Generic[T]):
    def __init__(self, item: int):
        self.item = item

    def __repr__(self):
        return f"PrimaryKey[{self.item}]"

    @classmethod
    def __class_getitem__(cls, item: int):
        return cls(item)

class BaseEntity:
    __fields__:    tuple[str] = ()
    __pk_attrs__:  tuple[str] = ()
    __pk_values__: tuple = ()

    __table_name__: str = ""

    def primary_key(self) -> tuple:
        if not self.__pk_values__: self.__pk_values__ = tuple(getattr(self, attr) for attr in self.__pk_attrs__)
        return self.__pk_values__

def entity(table_name: str):
    def decorator(cls):
        cls.__pk_attrs__ = tuple(x for x in cls.__annotations__ if isinstance(cls.__annotations__[x], PrimaryKey))
        cls.__fields__   = cls.__annotations__.keys()
        cls.__table_name__ = table_name

        return cls
    return decorator
