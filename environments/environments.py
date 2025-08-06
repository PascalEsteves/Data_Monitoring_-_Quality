import os 
from dotenv import load_dotenv
from typing import Dict

load_dotenv()

class Environments:

    db_configurations: Dict = {
        "port" : os.getenv("db_port"),
        "db_name" : os.getenv("db_name"),
        "username" : os.getenv("db_username"),
        "password": os.getenv("db_password"),
        "host": os.getenv("db_host")
    }

    @classmethod
    def get_db_configuration(self):
        return self.db_configurations

    @classmethod
    def get_s3_bucket(self):
        return os.getenv("S3_BUCKET")
    
    @classmethod
    def get_s3_access_key(self):
        return os.getenv("S3_ACCESS_KEY")
    
    @classmethod
    def get_s3_secret_key(self):
        return os.getenv("S3_SECRET_KEY")

    @classmethod
    def get_s3_region(self):
        return os.getenv("S3_REGION")

    @classmethod
    def get_blob_user(self):
        return os.getenv("BLOBUSER")
    
    @classmethod
    def get_blob_secret(self):
        return os.getenv("SECRET")
    
    @classmethod
    def get_blob_container(self):
        return os.getenv("CONTAINER")