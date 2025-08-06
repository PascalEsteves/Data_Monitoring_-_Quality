from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import StaticPool
from urllib.parse import quote_plus
from contextlib import contextmanager
from sqlalchemy.sql import text
from typing import Dict
import pandas as pd
from environments.environments import Environments


class Config:
    
    """
    Configuration class responsible for building the database connection string.
    """

    def __init__(self, engine:str="postgres", active_directory:bool=False):
        self.engine = engine
        self.active_directory = active_directory

    def get_connection_string(self)->str:
        """
        Constructs and returns the appropriate connection string based on the selected engine.
        """
        db_configs:Dict = Environments.get_db_configuration()
        username = db_configs.get("username")
        password = db_configs.get("password")
        db_host = db_configs.get("host")
        db_name = db_configs.get("database")
        port = db_configs.get("port")

        if self.engine=="postgres":
            return f"postgresql://{username}:{password}@{db_host}:{port}/{db_name}"
        
        elif self.engine in ["sqlserver","mssql"]:
            connection_string =(
                f"Driver={{ODBC Driver 17 for SQL Server}};"
                f"Server={db_host};"
                f"DATABASE={db_name};ApplicationIntent=ReadOnly;"
            )
            if self.active_directory:
                connection_string+= f"UUID={username};Authentication=ActiveDirectoryInteractive;"
            else:
                connection_string+= f"UUID={username};PWD={password}"
            params = quote_plus(connection_string)

            return f"mssql+pyodbc://?odbc_connect={params}"
        else:
            return f"sqlite:///{db_name}.db"
        

class DbConnection(Config):
    
    """
    Database class that handles database engine creation, sessions, and basic query utilities.
    """

    def __init__(self, engine = "postgres", active_directory = False):
        super().__init__(engine, active_directory)
        self._connnection_string = self.get_connection_string()
        self._db = create_engine(self._connnection_string, pool_pre_ping=True, poolclass=StaticPool)
        self._session = sessionmaker(bind=self._db)
    
    @contextmanager
    def session_scope(self)->Session:
        """
        Context manager for handling database sessions, ensuring commit/rollback and close.
        """
        session = self._session()
        try:
            yield session
        except Exception as e:
            session.rollback()
            print(f"Session rollback due to : {e}")
            raise 
        finally:
            session.close()

    def get_all_from_model(self, model: object) -> pd.DataFrame:
        """
        Retrieves all records from a given SQLAlchemy model and returns them as a Pandas DataFrame.
        """
        with self.session_scope() as session:
            results = session.query(model).all()
            data = [{c.name: getattr(row, c.name) for c in model.__table__.columns} for row in results]
            return pd.DataFrame(data)
        
    def filter_model_by_field(self, model: object, field_name: str, value) -> pd.DataFrame:
        """
        Retrieves records from a model filtered by a given field and value. Returns a Pandas DataFrame.
        """
        with self.session_scope() as session:
            field = getattr(model, field_name, None)
            if field is None:
                raise AttributeError(f"Model {model.__name__} has no field {field_name}")

            results = session.query(model).filter(field == value).all()
            data = [{c.name: getattr(row, c.name) for c in model.__table__.columns} for row in results]
            return pd.DataFrame(data)