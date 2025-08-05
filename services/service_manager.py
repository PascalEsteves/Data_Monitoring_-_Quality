from typing import Optional
import importlib

class Service_Manager:

    def __init__(self):
        pass

    @classmethod
    def get_connection(cls, configuration_type:str)->Optional[str]:
        
        type_connection = configuration_type
        module = importlib.import_module(f"services.{type_connection.lower()}_connection")
        connection_class = getattr(module, f"{type_connection.capitalize()}Connection")

        return connection_class()