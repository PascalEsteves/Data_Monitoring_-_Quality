import os 
from dotenv import load_dotenv

load_dotenv()

class Environments:

    @classmethod
    def get_blob_user(self):
        return os.getenv("BLOBUSER")
    
    @classmethod
    def get_blob_secret(self):
        return os.getenv("SECRET")
    
    @classmethod
    def get_blob_container(self):
        return os.getenv("CONTAINER")