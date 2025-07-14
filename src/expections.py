from dataclasses import dataclass

@dataclass
class AdvancedError(Exception):
    name: str
    message: str
    info: str

class DatabaseError(AdvancedError):
    def __init__(self):
        super().__init__("DatabaseError", "Error in Database Query", "some info")