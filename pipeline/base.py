from abc import ABC, abstractmethod
from typing import List
from pipeline.pipeline import Pipeline

class ValidationAction(ABC):

    def __init__(self, params:List, pipeline:Pipeline):
        self.params = params
        self.pipeline = pipeline
        self.df = pipeline.df

    @abstractmethod
    def run(self):
        pass

    def report(self, result):
        self.pipeline.report(result)
    
    def validate_column_exist(self, column:str):
        if column not in self.df.columns:
            raise ValueError(f"Missing columns: {column}")