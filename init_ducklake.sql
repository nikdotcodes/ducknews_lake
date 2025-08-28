INSTALL ducklake;
INSTALL postgres;
INSTALL httpfs;
LOAD ducklake;
LOAD postgres;
LOAD httpfs;

-- Create Postgres secret for metadata
CREATE OR REPLACE SECRET postgres_meta (
  TYPE postgres,
  HOST '${PG_HOST}',
  PORT 5432,
  DATABASE '${PG_DB}',
  USER '${PG_USER}',
  PASSWORD '${PG_PASSWORD}'
);

-- Create MinIO secret for data storage
CREATE OR REPLACE SECRET minio_secret (
  TYPE s3,
  PROVIDER config,
  KEY_ID '${S3_ACCESS_KEY_ID}',
  SECRET '${S3_SECRET_ACCESS_KEY}',
  REGION '${S3_REGION}',
  ENDPOINT '${S3_ENDPOINT}',
  URL_STYLE '${S3_URL_STYLE}',
  USE_SSL ${S3_USE_SSL}
);

-- Attach DuckLake to Postgres catalog and MinIO for storage

ATTACH 'ducklake:postgres:dbname=${PG_DB} host=${PG_HOST} user=${PG_USER} password=${PG_PASSWORD}' AS demo (
  DATA_PATH 's3://${S3_BUCKET}/'
);

ATTACH 'ducklake:postgres:dbname=${PG_DB} host=${PG_HOST} user=${PG_USER} password=${PG_PASSWORD}' AS raw (
  DATA_PATH 's3://${RAW_BUCKET}/'
);

ATTACH 'ducklake:postgres:dbname=${PG_DB} host=${PG_HOST} user=${PG_USER} password=${PG_PASSWORD}' AS bronze (
  DATA_PATH 's3://${BRONZE_BUCKET}/'
);

ATTACH 'ducklake:postgres:dbname=${PG_DB} host=${PG_HOST} user=${PG_USER} password=${PG_PASSWORD}' AS silver (
  DATA_PATH 's3://${SILVER_BUCKET}/'
);

ATTACH 'ducklake:postgres:dbname=${PG_DB} host=${PG_HOST} user=${PG_USER} password=${PG_PASSWORD}' AS gold (
  DATA_PATH 's3://${GOLD_BUCKET}/'
);

USE demo;

-- Creating demo table to test DuckLake
DROP TABLE IF EXISTS demo_table;
CREATE TABLE IF NOT EXISTS demo_table (
    id INTEGER,
    name STRING
);

INSERT INTO demo_table VALUES (1, 'Alice'), (2, 'Bob');