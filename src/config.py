import os
import sys
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def read_file(path: str) -> str:
    with open(path, 'r') as f:
        return f.read()

@dataclass(frozen=True)
class DatabaseConfig:
    password: int = os.getenv("DB_PASSWORD") or sys.exit("Environment variable DB_PASSWORD is required")
    username: str = os.getenv("DB_USERNAME") or sys.exit("Environment variable DB_USERNAME is required")
    name: str = os.getenv("DB_NAME", "postgres")
    port: int = os.getenv("DB_PORT", 5432)
    host: str = os.getenv("DB_HOST") or sys.exit("Environment variable DB_HOST is required")
    ssl = os.getenv("DB_SSL_MODE", "disable")

    schema: str = read_file(os.path.join(BASE_DIR, "../resources/sql/schema.sql"))
    delete_schema: str = read_file(os.path.join(BASE_DIR, "../resources/sql/schema.sql"))

@dataclass(frozen=True)
class Config:
    database: DatabaseConfig = DatabaseConfig()

config: Config = Config()