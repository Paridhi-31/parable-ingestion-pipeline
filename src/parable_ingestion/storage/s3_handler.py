import boto3
import os
from loguru import logger
from dotenv import load_dotenv
import time
import mimetypes

load_dotenv()

class S3Handler:
    def __init__(self):
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION")
        )
        self.bucket = os.getenv("AWS_S3_BUCKET_NAME")


    def upload_file(self, local_file_path, s3_folder):
            original_name = os.path.basename(local_file_path)
            ext = os.path.splitext(local_file_path)[1].lower() # Get .webp, .epub, etc.
            
            # 1. Standardize naming logic
            if ".epub" in original_name:
                standard_name = original_name.split('.')[0] + ".epub"
                content_type = "application/epub+zip"
            elif ext == ".webp":
                standard_name = original_name # Keep .webp extension
                content_type = "image/webp"
            else:
                # Fallback for other images/files
                standard_name = original_name
                content_type = mimetypes.guess_type(local_file_path)[0] or "application/octet-stream"
            
            # 2. Add timestamp
            timestamp = int(time.time() * 1000)
            final_name = f"{timestamp}-{standard_name}"
            s3_key = f"{s3_folder}/{final_name}"

            try:
                self.s3.upload_file(
                    local_file_path, 
                    self.bucket, 
                    s3_key,
                    ExtraArgs={'ContentType': content_type}
                )
                # Use the actual region variable to build the URL
                region = os.getenv("AWS_REGION")
                return f"https://{self.bucket}.s3.{region}.amazonaws.com/{s3_key}"
            except Exception as e:
                logger.error(f"S3 Upload failed: {e}")
                raise