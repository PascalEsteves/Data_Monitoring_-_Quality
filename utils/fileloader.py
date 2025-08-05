from typing import Any, Dict, List
from pathlib import Path
from logging import Logger
from io import BytesIO
import json
import pandas as pd
import xml.etree.ElementTree as ET
from typing import Optional

class UnsupportedFileTypeError(Exception):
    """Custom exception raised when an unsupported file type is provided."""
    pass


class FileLoader:
    """Class to handle loading of various file formats from a multiple storage."""

    SUPPORTED_FILES: Dict = {
        ".json", ".csv", ".xml", ".txt", ".xlsx", ".parquet", ".yaml"
    }

    def __init__(self, storage_connector:Optional, logs:Logger, local_mode: bool = False):

        self.logs = logs
        self.connector = storage_connector
        self.local_mode = local_mode

    def _detect_extention(self, filepath:str) -> str:
        """
        Detects and validates the file extension.

        Returns:
            str: The validated file extension.

        Raises:
            UnsupportedFileTypeError: If the file extension is not supported.
        """
        extention = Path(filepath).suffix.lower()
        self.logs.debug(f"Detected extension: {extention}")
        if extention not in self.SUPPORTED_FILES:
            self.logs.error(f"Unsupported extension: {extention}")
            raise UnsupportedFileTypeError(f"Unsupported extension: {extention}")
        return extention

    def _load_json(self, data: object) -> Any:
        """
        Loads JSON data.

        Args:
            data (object): Raw binary data of the JSON file.

        Returns:
            Any: Parsed JSON object.
        """
        self.logs.debug("Loading JSON file.")
        lines: list = data.decode("utf-8").strip().split("\n")

        return [json.loads(line) for line in lines]

    def _load_csv(self, data: object) -> pd.DataFrame:
        """
        Loads CSV data into a pandas DataFrame.

        Args:
            data (object): Raw binary data of the CSV file.

        Returns:
            pd.DataFrame: Loaded data.
        """
        self.logs.debug("Loading CSV file.")
        return pd.read_csv(BytesIO(data))

    def _load_excel(self, data: object) -> pd.DataFrame:
        """
        Loads Excel data into a pandas DataFrame.

        Args:
            data (object): Raw binary data of the Excel file.

        Returns:
            pd.DataFrame: Loaded data.
        """
        self.logs.debug("Loading Excel file.")
        return pd.read_excel(BytesIO(data))

    def _load_parquet(self, data: object) -> pd.DataFrame:
        """
        Loads Parquet data into a pandas DataFrame.

        Args:
            data (object): Raw binary data of the Parquet file.

        Returns:
            pd.DataFrame: Loaded data.
        """
        self.logs.debug("Loading Parquet file.")
        return pd.read_parquet(BytesIO(data), engine="pyarrow")

    def _load_xml(self, data: object) -> ET.Element:
        """
        Loads XML data and parses it into an ElementTree.

        Args:
            data (object): Raw binary data of the XML file.

        Returns:
            ET.Element: Parsed XML root element.
        """
        self.logs.debug("Loading XML file.")
        return ET.fromstring(BytesIO(data))
    
    def _load_from_local(self, filepath: str) -> bytes:
        """
        Loads a file from the local filesystem as binary.

        Args:
            filepath (str): Path to the local file.

        Returns:
            bytes: Raw binary data of the file.
        """
        self.logs.debug(f"Loading file from local path: {filepath}")
        with open(filepath, 'rb') as file:
            data = file.read()
        self.logs.debug(f"File {filepath} loaded successfully from local.")
        return data

    def load_file(self, filepath:str) -> Any:
        """
        Downloads and loads the file from blob storage based on its extension.

        Returns:
            Any: Parsed file content.

        Raises:
            Exception: If file download or parsing fails.
        """
        self.logs.info(f"Downloading file from blob: {filepath}")
        extention_type = {
            ".json": self._load_json,
            ".csv": self._load_csv,
            ".xml": self._load_xml,
            ".xlsx": self._load_excel,
            ".parquet": self._load_parquet
        }
        extention:str = self._detect_extention(filepath=filepath)
        try:
            if self.local_mode:
                data = self._load_from_local(filepath)
            else:
                self.logs.info(f"Downloading file from blob: {filepath}")
                data = self.connector.load_file(filepath)
                self.logs.info(f"File {filepath} downloaded successfully.")
            return extention_type.get(extention)(data)
        except Exception as e:
            self.logs.exception(f"Error loading file: {filepath}")
            raise e