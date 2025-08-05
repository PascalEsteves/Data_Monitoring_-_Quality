from datetime import datetime
from azure.storage.blob import BlobServiceClient,generate_blob_sas, BlobSasPermissions
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
import os
from typing import List
from environments.environments import Environments

load_dotenv()

class BlobConnection:

    __connection__ = "Blob_Connection"

    def __init__(self):
        self.container:str = Environments.get_blob_container()
        self.client:object = self._build_client()

    def _build_client(self)->BlobServiceClient:

        """Create Blob Storage Connection"""

        self.user:str = Environments.get_blob_user()
        self.sercret:str = Environments.get_blob_secret()

        url = f"https://{self.user}.blob.core.windows.net"
        return BlobServiceClient(account_url = url, credential= self.sercret)
    

    def upload_file(self, blob_name:str, file_path:str, container:str=None)->None:

        """
        Function to upload file to a blob container

        :params container: Container where to upload the file
        :params blob_name: Path inside container where to upload the file
        :params file_path: Local path of the file to upload to blob
        """
        container:str = container if container else self.container
        blob:object = self.client.get_blob_clent(container= container, blob=blob_name)

        with open(file_path, "rb") as f:
            blob.upload_blob(f, overwrite= True, max_concurrency=4)
    
    def download_file(self, blob_name:str, file_path:str, container:str = None):
        """
        Function to download file from blob to local machine
        :params container: Container where is required file
        :params blob_name: Path inside container where of the file
        :params file_path: Local path to download the file 
        """
        container = container if container else self.container
        blob:object = self.client.get_blob_client(container= container, blob= blob_name)
        
        with open(file_path, "wb") as f:

            blob_data = blob.download_blob()
            f.write(blob_data.readall())

    def read_file(self, filepath:str):

        """
        Function to download file to memory from blob to local machine
        :params filepath: Local path to download the file 
        """

        try:
            data: object = self.client.get_blob_client(
                    container=self.container,
                    blob=filepath
                ).download_blob().readall()
            
            self.logs.info(f"File {filepath} downloaded successfully.")
        except Exception as e:
            self.logs.exception(f"Error loading file: {filepath}")
            raise e
        
        return data
    
    def get_public_link(self, blob_name:str, container:str=None)->str:

        """
        Function generate the sas link from file 
        :params container: Container where is required file
        :params blob_name: Path inside container where of the file
        """
        container = container if container else self.container
        file:object = self.client.get_blob_client(container=container, blob=blob_name)
        
        token:str = generate_blob_sas(
            account_name= self.user,
            account_key = self.sercret,
            container_name = container,
            blob_name = blob_name,
            permission = BlobSasPermissions(read=True, tag=False),
            expiry= datetime.utcnow() + relativedelta(months=3)
        )

        return file.url + "?" + token
    
    def get_list_files(self, container:str=None, folder:str=None)->List:


        """
        Function to return all list of files inside a container
        :params container: Container to list
        :params folder: Folder name to segment the list if needed
        """
        container = container if container else self.container
        client_container: object = self.client.get_container_client(container=container)
        
        if folder:
            return [x.name for x in client_container.list_blobs(name_starts_with=folder)]
        
        return [x.name for x in client_container.list_blobs()]
