from dotenv import load_dotenv
import duckdb
import os

print("🚀 Starting Bronze pipeline...")
print("🔎 Loading environment variables...")

load_dotenv()

# Detect environment
running_in_docker = os.path.exists("/.dockerenv")
# Setting up MinIO/S3 connection
S3_ENDPOINT = (
    os.getenv("S3_ENDPOINT_DOCKER")
    if running_in_docker
    else os.getenv("S3_ENDPOINT_LOCAL")
)
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY_ID")
S3_SECRET_KEY = os.getenv("S3_SECRET_ACCESS_KEY")
S3_REGION = os.getenv("S3_REGION")
S3_URL_STYLE = os.getenv("S3_URL_STYLE")
S3_USE_SSL = os.getenv("S3_USE_SSL")

PG_HOST = (
    os.getenv("PG_HOST_DOCKER") if running_in_docker else os.getenv("PG_HOST_LOCAL")
)
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

BUCKET_PREFIX = os.getenv("BUCKET_PREFIX")
RAW_BUCKET = os.getenv("RAW_BUCKET")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET")


def load_raw_to_bronze():

    preamble_sql = f"""
        INSTALL httpfs;
        LOAD httpfs;
        CREATE OR REPLACE SECRET minio_secret (
          TYPE s3,
          PROVIDER config,
          KEY_ID '{S3_ACCESS_KEY}',
          SECRET '{S3_SECRET_KEY}',
          REGION '{S3_REGION}',
          ENDPOINT '{S3_ENDPOINT}',
          URL_STYLE '{S3_URL_STYLE}',
          USE_SSL {S3_USE_SSL.lower()}
        );"""

    attach_bronze_sql = f"""
        ATTACH 'ducklake:postgres:dbname={PG_DB} host={PG_HOST} user={PG_USER} password={PG_PASSWORD}' AS {BRONZE_BUCKET} (
            DATA_PATH 's3://{BRONZE_BUCKET}/'
        );"""

    use_bucket_sql = f"use {BRONZE_BUCKET};"

    table_exists_sql = """
       SELECT EXISTS (
           SELECT 1
           FROM information_schema.tables
           WHERE table_name = 'trust_category_fake_news'
       ) 
       """

    select_statement = f"""
       SELECT filename,
              crawled::DATETIME as crawldate,
           thread.site,
              thread.site_categories,
              thread.site_type,
              thread.country,
              thread.performance_score,
              thread.domain_rank,
              uuid,
              url,
              author,
              published,
              title,
              text,
           language,
           sentiment,
           categories,
           topics,
           ai_allow,
           has_canonical,
           breaking,
           entities,
           syndication,
           trust
       FROM read_json('s3://{RAW_BUCKET}/{BUCKET_PREFIX}/trust_category_fake_news*/*.json', ignore_errors=true) \
       """

    create_table_sql = f"""
        CREATE TABLE trust_category_fake_news AS
        {select_statement} WITH NO DATA
        ;
    """

    set_partition_sql = """
        ALTER TABLE trust_category_fake_news SET PARTITIONED BY (year(crawldate), month(crawldate), day(crawldate)); \
        """

    load_table_sql = f"""
        INSERT INTO trust_category_fake_news
        {select_statement}
        WHERE uuid NOT IN (
            SELECT uuid FROM trust_category_fake_news
        )
        LIMIT 1000
        ;
    """

    with duckdb.connect() as con:
        print("🔧Configuring DuckDB connection")
        con.execute(preamble_sql)
        print("📌Attaching Bronze DuckLake")
        con.execute(attach_bronze_sql)
        con.execute(use_bucket_sql)
        print("🔍Checking for existing table...")
        exists = con.execute(table_exists_sql).fetchone()[0]
        if not exists:
            print("🏗️Table does not exist, creating...")
            con.execute(create_table_sql)
            con.execute(set_partition_sql)
        print("🚰Loading data into table...")
        con.execute(load_table_sql)
