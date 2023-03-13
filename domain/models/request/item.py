from pydantic import BaseModel


class Item(BaseModel):
    Nombre: str
    Nombre1: str
    Nombre2: str
    Apellido1: str
    Apellido2: str
    algorithm: int
