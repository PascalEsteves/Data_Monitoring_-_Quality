import yaml
import pandas as pd
from datetime import datetime
from pathlib import Path
import xml.etree.ElementTree as ET
import json
from typing import Dict, Optional, Union, List, Tuple
import numpy as np
from logging import Logger
from utils.fileloader import FileLoader
from services.service_manager import Service_Manager
from pipeline.pipeline import Pipeline
import jsonschema

class UnsupportedFileTypeError(Exception):
    """Custom exception raised when an unsupported file type is provided."""
    pass

class ValidationProcessor(FileLoader):

    def __init__(self, schema_path: Union[Dict,Path,str], logs:Logger):
       
        self.logs = logs
        
        if isinstance(schema_path, (str, Path)):
            self.schema = self._load_schema(Path(schema_path))
        else:
            self.schema = schema_path
        #self._validate_schema()
        self._mode = self.schema.get("storage").get("type")=='local' 
        self.connector = Service_Manager.get_connection(self.schema.get("storage").get("type"))
        super().__init__(logs=logs, storage_connector=self.connector, local_mode=self._mode)
    
    def _validate_schema(self):
        self.logs.info(f"Initializing validation with schema")
        with open("configs/yaml_config.json", "r") as f:
            data_schema = json.load(f)
        try:
            jsonschema.validate(instance=self.schema, schema=data_schema)
            self.logs.info("YAML is valid according to the schema.")
        except jsonschema.ValidationError as e:
            self.logs.info(f"YAML is not valid: {e.message}")

    def _load_schema(self, schema_path: Path) -> Dict:
        """
        Loads schema from a YAML file.

        Args:
            schema_path (Path): Path to the schema file.

        Returns:
            Dict: Parsed schema.

        Raises:
            FileNotFoundError: If the schema file is not found.
        """
        self.logs.info(f"Loading schema from: {schema_path}")
        if not schema_path.exists():
            self.logs.error(f"Schema not found: {schema_path}")
            raise FileNotFoundError(f"Schema not found: {schema_path}")
        with open(schema_path, "r") as f:
            return yaml.safe_load(f)

    def run(self):
        """                                                                                                                                                                                                                                                              
        Function to get validations and data
        """
        file = self.schema.get("storage").get("type")
        data = self.load_file(filepath=self.schema.get("storage").get(file))
        pipeline = Pipeline(df=data)
        results = pipeline.run(actions_config=self.schema.get("actions"))

        with open("results/results.json", "w") as f:
            json.dump(results,f,ensure_ascii=True, indent=2)
        
        self.logs.info("Results saved in : results/results.json")

