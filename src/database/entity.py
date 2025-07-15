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
    __key__: str = ""

def entity(table_name: str, nullable: bool = False):
    def decorator(cls):
        if nullable:
            for name in cls.__annotations__:
                if not hasattr(cls, name):
                    setattr(cls, name, None)

        for x in cls.__annotations__:
            if isinstance(cls.__annotations__[x], PrimaryKey):
                cls.__key__ = x
                break
        cls.__fields__   = cls.__annotations__.keys()
        cls.__table_name__ = table_name

        return cls
    return decorator
