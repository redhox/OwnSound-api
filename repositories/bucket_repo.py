import boto3
import json
from botocore.exceptions import ClientError
from botocore.config import Config
import os
from dotenv import load_dotenv
import logging
load_dotenv() 

logger = logging.getLogger(__name__)

class S3ContactRepository:
    def __init__(self, repo=None):
        self.repo = repo
        self.bucket_configs = {} # Keyed by library_id
        self.active_config = None
        self.active_s3_client = None
        self.refresh_configs()
    
    def refresh_configs(self):
        self.bucket_configs = {} 
        
        if self.repo:
            libraries = self.repo.get_libraries()
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

    def ensure_public_policy(self, client, bucket_name):
        """Vérifie et applique la politique d'accès public pour le préfixe 'public/'."""
        import json
        
        target_resource = f"arn:aws:s3:::{bucket_name}/public/*"
        
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "PublicReadPrefix",
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Action": ["s3:GetObject"],
                    "Resource": [target_resource]
                }
            ]
        }

        try:
            # On tente d'abord de récupérer la politique actuelle
            try:
                response = client.get_bucket_policy(Bucket=bucket_name)
                current_policy = json.loads(response['Policy'])
                
                # Vérifier si notre statement est déjà présent
                is_configured = False
                for statement in current_policy.get('Statement', []):
                    if statement.get('Sid') == "PublicReadPrefix" and target_resource in statement.get('Resource', []):
                        is_configured = True
                        break
                
                if is_configured:
                    logger.info(f"Public policy already configured for bucket {bucket_name}")
                    return
                
                # Si une politique existe mais n'a pas notre statement, on l'ajoute
                current_policy['Statement'].append(policy['Statement'][0])
                policy_to_apply = current_policy
            except ClientError as e:
                # Si aucune politique n'existe (Error 404/NoSuchBucketPolicy)
                policy_to_apply = policy

            # Appliquer la politique
            client.put_bucket_policy(
                Bucket=bucket_name,
                Policy=json.dumps(policy_to_apply)
            )
            logger.info(f"Successfully applied public read policy to {bucket_name}/public/*")
            
        except Exception as e:
            logger.warning(f"Could not ensure bucket policy for {bucket_name}: {e}")

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
            parts = url.split(f"/{bucket_name}")
            if len(parts) > 1:
                endpoint = parts[0]
        
        self.active_s3_client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=config.get("access_key"),
            aws_secret_access_key=config.get("secret_key"),
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
            region_name="us-east-1"
        )
        
        # S'assurer que la politique publique est appliquée
        self.ensure_public_policy(self.active_s3_client, bucket_name)

    def check_public_access(self, bucket_name, path, endpoint_url):
        """Tente d'accéder à un fichier sans signature pour voir s'il est public."""
        import requests
        # Construire l'URL publique probable
        public_url = f"{endpoint_url.rstrip('/')}/{bucket_name}/{path.lstrip('/')}"
        try:
            # On utilise HEAD pour ne pas télécharger tout le fichier, juste vérifier l'accès
            response = requests.head(public_url, timeout=2)
            return response.status_code == 200
        except:
            return False

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

        # Si un bucket_host (URL publique) est configuré, on vérifie si on peut l'utiliser directement
        bucket_host = config.get("bucket_host")
        if bucket_host:
            # On pourrait mettre en cache le résultat du test de visibilité publique
            # pour éviter de faire un ping HTTP à chaque fois
            return f"{bucket_host.rstrip('/')}/{path.lstrip('/')}"

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
