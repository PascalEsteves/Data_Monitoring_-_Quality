from validation_processor import ValidationProcessor
from utils.fileloader import FileLoader
from logs.logs import LoggerFactory
import logging
import argparse

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Parser to add schema path")
    parser.add_argument("-p", "--path", type=str, required=True, help="Path to config file")
    args = parser.parse_args()

    logger = LoggerFactory()
    logs = logger.get_logger()
    Val_checks = ValidationProcessor(schema_path=args.path, logs=logs)
    results = Val_checks.run()