from typing import List, Optional, Dict
from datetime import datetime

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import Column, Integer, DateTime, String, text

class BaseEntity(DeclarativeBase):
    __pk_attrs__:  tuple[str] = ()
    __pk_values__: tuple= ()

    def primary_key(self) -> tuple:
        if not self.__pk_values__: self.__pk_values__ = tuple(getattr(self, attr) for attr in self.__pk_attrs__)
        return self.__pk_values__