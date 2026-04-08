import os
from repositories.sqlite_repo import SqliteRepository
from repositories.postgres_repo import PostgresRepository
from repositories.bucket_repo import S3ContactRepository

db_type = os.getenv("DATABASE_TYPE", "sqlite").lower()

if db_type == "postgres":
    pg_host = os.getenv("PGHOST", "localhost")
    pg_port = os.getenv("PGPORT", "5432")
    pg_db = os.getenv("PGDATABASE", "musique")
    pg_user = os.getenv("PGUSER", "postgres")
    pg_pass = os.getenv("PGPASSWORD", "postgres")
    dsn = f"dbname={pg_db} user={pg_user} password={pg_pass} host={pg_host} port={pg_port}"
    repo = PostgresRepository(dsn)
else:
    repo = SqliteRepository()

bucketS3 = S3ContactRepository(repo=repo)
