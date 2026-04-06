from repositories.sqlite_repo import SqliteRepository
from repositories.bucket_repo import S3ContactRepository

repo = SqliteRepository()
bucketS3 = S3ContactRepository(repo=repo)
