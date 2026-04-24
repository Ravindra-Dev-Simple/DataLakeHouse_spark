"""
================================================================
 config.py — Centralized environment-based configuration
================================================================
 Saare environment variables ek hi jagah. Har module yahan se
 config padhta hai. Hardcoded values nahi — sab override-able.
================================================================
"""
import os


# ----------------------------------------------------------------
# KAFKA CONFIGURATION
# ----------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    "my-cluster-kafka-bootstrap.lakehouse-ingest.svc.cluster.local:9092"
)
KAFKA_TOPIC             = os.environ.get("KAFKA_TOPIC", "finacle-transactions")
KAFKA_SECURITY_PROTOCOL = os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
KAFKA_SASL_MECHANISM    = os.environ.get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
KAFKA_USERNAME          = os.environ.get("KAFKA_USERNAME", "app-user")
KAFKA_PASSWORD          = os.environ.get("KAFKA_PASSWORD", "bwDqbYGrgC2AKOMuthoUu7Ckkj8tNjtB")


# ----------------------------------------------------------------
# MINIO / S3 CONFIGURATION
# ----------------------------------------------------------------
S3_ENDPOINT   = os.environ.get(
    "S3_ENDPOINT",
    "http://minio-api.lakehouse-data.svc.cluster.local:9000"
)
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "MyStr0ngP@ssw0rd123")
S3_REGION     = os.environ.get("S3_REGION", "us-east-1")


# ----------------------------------------------------------------
# ICEBERG / NESSIE CONFIGURATION
# ----------------------------------------------------------------
ICEBERG_WAREHOUSE = os.environ.get(
    "ICEBERG_WAREHOUSE",
    "s3a://lakehouse-warehouse/warehouse"
)
NESSIE_URI        = os.environ.get(
    "NESSIE_URI",
    "http://nessie.lakehouse-catalog.svc:19120/api/v2"
)
NESSIE_REF        = os.environ.get("NESSIE_REF", "main")


# ----------------------------------------------------------------
# TRINO CONFIGURATION
# ----------------------------------------------------------------
TRINO_HOST = os.environ.get(
    "TRINO_HOST",
    "trino.lakehouse-catalog.svc.cluster.local"
)
TRINO_PORT        = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_USER        = os.environ.get("TRINO_USER", "admin")
TRINO_CATALOG     = os.environ.get("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA      = os.environ.get("TRINO_SCHEMA", "bronze")
TRINO_HTTP_SCHEME = os.environ.get("TRINO_HTTP_SCHEME", "http")


# ----------------------------------------------------------------
# APPLICATION CONSTANTS
# ----------------------------------------------------------------
BRONZE_TABLES = [
    "finacle_transactions",
    "finacle_customers",
    "aml_alerts",
    "cibil_bureau",
    "npa_report",
]


def print_config():
    """Startup pe current configuration print karo (debugging ke liye)."""
    print("=" * 72)
    print(" CONFIGURATION ".center(72, "="))
    print("=" * 72)
    print(f"  KAFKA_BOOTSTRAP_SERVERS : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"  KAFKA_TOPIC             : {KAFKA_TOPIC}")
    print(f"  S3_ENDPOINT             : {S3_ENDPOINT}")
    print(f"  ICEBERG_WAREHOUSE       : {ICEBERG_WAREHOUSE}")
    print(f"  NESSIE_URI              : {NESSIE_URI}")
    print(f"  TRINO_HOST              : {TRINO_HOST}:{TRINO_PORT}")
    print(f"  TRINO_CATALOG           : {TRINO_CATALOG}")
    print(f"  TRINO_SCHEMA            : {TRINO_SCHEMA}")
    print("=" * 72)