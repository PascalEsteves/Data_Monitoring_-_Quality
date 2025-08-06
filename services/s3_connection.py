import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from typing import List
from environments.environments import Environments

load_dotenv()

class S3Connection:

    __connection__ = "S3_Connection"

    def __init__(self):
        self.bucket: str = Environments.get_s3_bucket()
        self.client = self._build_client()

    def _build_client(self):
        """Create S3 Client Connection"""
        self.access_key = Environments.get_s3_access_key()
        self.secret_key = Environments.get_s3_secret_key()
        self.region = Environments.get_s3_region()

        return boto3.client(
            's3',
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region
        )

    def upload_file(self, s3_key: str, file_path: str, bucket: str = None) -> None:
        """
        Upload a file to an S3 bucket.
        :params bucket: Target S3 bucket.
        :params s3_key: Path (key) inside the S3 bucket.
        :params file_path: Local file path to upload.
        """
        bucket = bucket if bucket else self.bucket
        try:
            self.client.upload_file(file_path, bucket, s3_key)
        except ClientError as e:
            print(f"Upload failed: {e}")
            raise e

    def download_file(self, s3_key: str, file_path: str, bucket: str = None) -> None:
        """
        Download a file from an S3 bucket to local machine.
        :params bucket: Source S3 bucket.
        :params s3_key: Path (key) inside S3 bucket.
        :params file_path: Local path to save the file.
        """
        bucket = bucket if bucket else self.bucket
        try:
            self.client.download_file(bucket, s3_key, file_path)
        except ClientError as e:
            print(f"Download failed: {e}")
            raise e

    def read_file(self, s3_key: str, bucket: str = None) -> bytes:
        """
        Read file from S3 directly into memory.
        :params s3_key: Path (key) inside S3 bucket.
        :return: File content as bytes.
        """
        bucket = bucket if bucket else self.bucket
        try:
            response = self.client.get_object(Bucket=bucket, Key=s3_key)
            data = response['Body'].read()
            print(f"File {s3_key} downloaded successfully.")
            return data
        except ClientError as e:
            print(f"Error loading file: {s3_key}")
            raise e

    def get_public_link(self, s3_key: str, bucket: str = None, expiry_hours: int = 72) -> str:
        """
        Generate a pre-signed URL for a file.
        :params s3_key: Path (key) inside S3 bucket.
        :params expiry_hours: Validity time for the link.
        :return: Pre-signed URL string.
        """
        bucket = bucket if bucket else self.bucket
        try:
            url = self.client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket, 'Key': s3_key},
                ExpiresIn=expiry_hours * 3600
            )
            return url
        except ClientError as e:
            print(f"Failed to generate presigned URL: {e}")
            raise e

    def get_list_files(self, bucket: str = None, prefix: str = "") -> List[str]:
        """
        List files in an S3 bucket or inside a folder (prefix).
        :params bucket: S3 bucket name.
        :params prefix: Folder path to filter files.
        :return: List of file keys.
        """
        bucket = bucket if bucket else self.bucket
        try:
            response = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            if 'Contents' in response:
                return [item['Key'] for item in response['Contents']]
            else:
                return []
        except ClientError as e:
            print(f"Failed to list files: {e}")
            raise e
