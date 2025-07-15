from dataclasses import dataclass

from src.database.entity import entity, BaseEntity, PrimaryKey, Varchar, Timestamp, Default

from datetime import datetime

@dataclass(slots=True)
@entity(table_name="users")
class UserExample(BaseEntity):
    id: PrimaryKey[int]
    tag: Varchar[255]
    created_at: Default[Timestamp] = datetime.now()

@dataclass(slots=True)
@entity(table_name="users", nullable=True)
class TimestampEntity:
    created_at: str