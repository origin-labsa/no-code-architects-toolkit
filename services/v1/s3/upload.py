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
import boto3
import logging
import requests
from urllib.parse import urlparse, unquote, quote
import uuid

logger = logging.getLogger(__name__)

def get_s3_client():
    """Create and return a MinIO-compatible S3 client."""
    endpoint_url = os.getenv('S3_ENDPOINT_URL')
    access_key = os.getenv('S3_ACCESS_KEY')
    secret_key = os.getenv('S3_SECRET_KEY')
    region = os.environ.get('S3_REGION', 'us-east-1')

    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )

    return session.client('s3', endpoint_url=endpoint_url, config=boto3.session.Config(signature_version='s3v4'))

def get_filename_from_url(url):
    """Extract filename from URL or generate one."""
    path = urlparse(url).path
    filename = os.path.basename(unquote(path))
    return filename or f"{uuid.uuid4()}"

def stream_upload_to_s3(file_url, custom_filename=None, make_public=False):
    """
    Streams a file from a remote URL to MinIO via multipart upload.
    Returns public or signed URL based on `make_public`.
    """
    try:
        bucket_name = os.environ.get('S3_BUCKET_NAME')
        endpoint_url = os.getenv('S3_ENDPOINT_URL')
        s3_client = get_s3_client()

        filename = custom_filename or get_filename_from_url(file_url)
        acl = 'public-read' if make_public else 'private'

        logger.info(f"Starting upload to MinIO: {filename} -> bucket={bucket_name}")

        multipart_upload = s3_client.create_multipart_upload(
            Bucket=bucket_name,
            Key=filename,
            ACL=acl
        )
        upload_id = multipart_upload['UploadId']

        response = requests.get(file_url, stream=True)
        response.raise_for_status()

        chunk_size = 5 * 1024 * 1024
        parts = []
        part_number = 1
        buffer = bytearray()

        for chunk in response.iter_content(chunk_size=1024 * 1024):
            buffer.extend(chunk)

            if len(buffer) >= chunk_size:
                logger.info(f"Uploading part {part_number}")
                part = s3_client.upload_part(
                    Bucket=bucket_name,
                    Key=filename,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=buffer
                )
                parts.append({'PartNumber': part_number, 'ETag': part['ETag']})
                part_number += 1
                buffer = bytearray()

        if buffer:
            logger.info(f"Uploading final part {part_number}")
            part = s3_client.upload_part(
                Bucket=bucket_name,
                Key=filename,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=buffer
            )
            parts.append({'PartNumber': part_number, 'ETag': part['ETag']})

        logger.info("Completing multipart upload")
        s3_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=filename,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )

        if make_public:
            encoded_filename = quote(filename)
            file_url = f"{endpoint_url}/{bucket_name}/{encoded_filename}"
        else:
            file_url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': filename},
                ExpiresIn=3600
            )

        return {
            'file_url': file_url,
            'filename': filename,
            'bucket': bucket_name,
            'public': make_public
        }

    except Exception as e:
        logger.error(f"Error uploading to MinIO: {e}")
        raise
