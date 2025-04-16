# Copyright (c) 2025 Stephen G. Pope
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.



import os

# Retrieve the API key from environment variables
API_KEY = os.environ.get('API_KEY')
if not API_KEY:
    raise ValueError("API_KEY environment variable is not set")

# MinIO (S3-Compatible) environment variables
S3_BUCKET_NAME = os.environ.get('MINIO_BUCKET_NAME', '')
S3_REGION = os.environ.get('MINIO_REGION', '')
S3_ENDPOINT_URL = os.environ.get('MINIO_ENDPOINT_URL', '')
S3_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY', '')
S3_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY', '')

def validate_env_vars(provider):
    """ Validate the necessary environment variables for the selected storage provider """
    required_vars = {
        'S3': ['S3_BUCKET_NAME', 'S3_REGION', 'S3_ENDPOINT_URL', 'S3_ACCESS_KEY', 'S3_SECRET_KEY']
    }
    
    missing_vars = [var for var in required_vars[provider] if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"Missing environment variables for {provider} storage: {', '.join(missing_vars)}")

class CloudStorageProvider:
    """ Abstract CloudStorageProvider class to define the upload_file method """
    def upload_file(self, file_path: str) -> str:
        raise NotImplementedError("upload_file must be implemented by subclasses")

class S3CompatibleProvider(CloudStorageProvider):
    """ MinIO-compatible storage provider """
    def __init__(self):
        self.bucket_name = os.getenv('S3_BUCKET_NAME')
        self.region = os.getenv('S3_REGION')
        self.endpoint_url = os.getenv('S3_ENDPOINT_URL')
        self.access_key = os.getenv('S3_ACCESS_KEY')
        self.secret_key = os.getenv('S3_SECRET_KEY')

    def upload_file(self, file_path: str) -> str:
        from services.s3_toolkit import upload_to_s3
        return upload_to_s3(
            file_path,
            self.bucket_name,
            self.region,
            self.endpoint_url,
            self.access_key,
            self.secret_key
        )

def get_storage_provider() -> CloudStorageProvider:
    """ Return the appropriate storage provider (MinIO in this case) """
    validate_env_vars('S3')
    return S3CompatibleProvider()
