import duckdb
import os
from pathlib import Path

# Detect environment
running_in_docker = os.path.exists("/.dockerenv")

# Pick correct hostnames
pg_host = (
    os.environ["PG_HOST_DOCKER"] if running_in_docker else os.environ["PG_HOST_LOCAL"]
)
s3_endpoint = (
    os.environ["S3_ENDPOINT_DOCKER"] if running_in_docker else os.environ["S3_ENDPOINT_LOCAL"]
)

# Path to SQL file
if running_in_docker:
    init_sql_path = Path("/app/init_ducklake.sql")
else:
    init_sql_path = Path(__file__).resolve().parent.parent / "init_ducklake.sql"

if not init_sql_path.exists():
    raise FileNotFoundError(f"❌ Could not find {init_sql_path}")

print(f"🚀 Connecting to DuckDB... using PG host={pg_host}, S3 endpoint={s3_endpoint}")
con = duckdb.connect()

# Read and replace placeholders
with open(init_sql_path, "r") as f:
    sql_script = f.read()

# Replace variables manually for ones that differ between local and Docker
sql_script = sql_script.replace("${PG_HOST}", pg_host)
sql_script = sql_script.replace("${S3_ENDPOINT}", s3_endpoint)
sql_script = os.path.expandvars(sql_script)

print("⚙️  Running DuckLake initialization...")
con.execute(sql_script)

print("✅ DuckLake is initialized and ready!")
print("📊 Demo table contents:")
print(con.execute("SELECT * FROM demo.demo_table").fetchall())
