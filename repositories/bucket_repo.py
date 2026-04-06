import boto3
import json
from botocore.exceptions import ClientError
from botocore.config import Config
import os
from dotenv import load_dotenv
load_dotenv() 

class S3ContactRepository:
    def __init__(self, json_repo=None):
        self.json_repo = json_repo
        self.bucket_configs = {} # Keyed by library_id
        self.active_config = None
        self.active_s3_client = None
        self.refresh_configs()
    
    def refresh_configs(self):
        self.bucket_configs = {} 
        
        if self.json_repo:
            libraries = self.json_repo.get_libraries()
            for lib in libraries:
                lib_id = lib.get("id")
                if not lib_id: continue
                
                url = lib.get("url")
                ids = lib.get("identifiers", {})
                
                bucket_name = ids.get("bucket_name")
                if not bucket_name and url:
                    try: bucket_name = url.rstrip("/").split("/")[-1]
                    except: pass
                
                if bucket_name:
                    self.bucket_configs[lib_id] = {
                        "bucket_name": bucket_name,
                        "endpoint_url": url,
                        "access_key": ids.get("aws_access_key_id"),
                        "secret_key": ids.get("aws_secret_access_key"),
                        "url_expiration": ids.get("url_expiration", 3600),
                        "library_id": lib_id
                    }
        
        # Fallback from env vars (assigned id 0 for fallback)
        env_bucket = os.environ.get("BUCKET_NAME")
        if env_bucket:
            self.bucket_configs[0] = {
                "bucket_name": env_bucket,
                "endpoint_url": os.environ.get("AWS_ENDPOINT_URL"),
                "access_key": os.environ.get("AWS_ACCESS_KEY_ID"),
                "secret_key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
                "url_expiration": int(os.environ.get("URL_EXPIRATION", 3600)),
                "library_id": 0
            }
        
        if self.bucket_configs:
            self.set_active_by_id(list(self.bucket_configs.keys())[0])

    def set_active_by_id(self, lib_id):
        config = self.bucket_configs.get(lib_id)
        if not config:
            raise ValueError(f"Library configuration '{lib_id}' not found.")
        
        self.active_config = config
        
        # Robust endpoint extraction
        url = config.get("endpoint_url", "")
        bucket_name = config["bucket_name"]
        endpoint = url
        if bucket_name and bucket_name in url:
            # We want the part before /bucket_name/
            # Example: https://host.com/my-bucket/ -> https://host.com/
            parts = url.split(f"/{bucket_name}")
            if len(parts) > 1:
                endpoint = parts[0]
        
        # Ensure endpoint ends with / if it doesn't already (some S3 clients prefer it)
        # but boto3 usually handles it.
        
        self.active_s3_client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=config.get("access_key"),
            aws_secret_access_key=config.get("secret_key"),
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
            region_name="us-east-1"
        )

    def get_temporary_link(self, path: str, bucket_name: str = None, library_id: int = None):
        if library_id is not None:
            try: library_id = int(library_id)
            except: pass

        config = None
        if library_id in self.bucket_configs:
            config = self.bucket_configs[library_id]
        elif bucket_name:
            for cfg in self.bucket_configs.values():
                if cfg["bucket_name"] == bucket_name:
                    config = cfg
                    break
        
        if not config:
            if self.active_config:
                config = self.active_config
            else:
                return None
            
        try:
            client = self.active_s3_client
            if config != self.active_config:
                # Localized client creation with robust endpoint
                url = config.get("endpoint_url", "")
                b_name = config["bucket_name"]
                endpoint = url
                if b_name and b_name in url:
                    parts = url.split(f"/{b_name}")
                    if len(parts) > 1:
                        endpoint = parts[0]

                client = boto3.client(
                    's3',
                    endpoint_url=endpoint,
                    aws_access_key_id=config.get("access_key"),
                    aws_secret_access_key=config.get("secret_key"),
                    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
                    region_name="us-east-1"
                )
            
            url = client.generate_presigned_url(
                "get_object",
                Params={"Bucket": config["bucket_name"], "Key": path},
                ExpiresIn=int(config.get("url_expiration", 3600)) 
            )
            return url
        except Exception as e:
            logger.error(f"Error generating presigned URL for {path}: {e}")
            return None
