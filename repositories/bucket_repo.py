import boto3
import json
from botocore.exceptions import ClientError
import os
from dotenv import load_dotenv
load_dotenv() 

class S3ContactRepository:
    def __init__(self):
        self.bucket_name = os.environ["BUCKET_NAME"]
        self.s3 = boto3.client(
            's3',
            endpoint_url=os.environ["AWS_ENDPOINT_URL"],
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
        )
    
    def _get_object_key(self, contact_id):
        return f'contacts/{contact_id}.json'

    def save_contact(self, contact_id, contact_data):
        key = self._get_object_key(contact_id)
        try:
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(contact_data),
                ContentType='application/json'
            )
        except ClientError as e:
            raise RuntimeError(f"Error saving contact: {e}")

    def get_contact(self, contact_id):
        key = self._get_object_key(contact_id)
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            return json.loads(response['Body'].read().decode('utf-8'))
        except self.s3.exceptions.NoSuchKey:
            return None
        except ClientError as e:
            raise RuntimeError(f"Error reading contact: {e}")

    def delete_contact(self, contact_id):
        key = self._get_object_key(contact_id)
        try:
            self.s3.delete_object(Bucket=self.bucket_name, Key=key)
        except ClientError as e:
            raise RuntimeError(f"Error deleting contact: {e}")

    def list_contacts(self):
        try:
            response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix='contacts/')
            contacts = []
            for obj in response.get('Contents', []):
                key = obj['Key']
                contact_id = key.split('/')[-1].replace('.json', '')
                contacts.append(contact_id)
            return contacts
        except ClientError as e:
            raise RuntimeError(f"Error listing contacts: {e}")

    def get_temporary_link(self,request: str):
        key = request
        print(f"[DEBUG] S3 key avant head_object: '{key}'")
        self.s3.head_object(Bucket=self.bucket_name, Key=key)

        url = self.s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket_name, "Key": key},
            ExpiresIn=int(os.environ["URL_EXPIRATION"]) 
        )
        return  url
