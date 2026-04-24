# ================================================================
# Dockerfile — Banking Lakehouse Application
# ================================================================
# Multi-stage build:
#   Stage 1: Python + Spark + Iceberg jars
#   Stage 2: Final lean runtime image
#
# Usage:
#   docker build -t lakehouse-app:1.0 .
#   docker run --rm lakehouse-app:1.0 --mode trino --section counts
#   docker run --rm lakehouse-app:1.0 --mode kafka --topic finacle-transactions
# ================================================================

FROM apache/spark:3.5.4-python3

USER root

# ---- Metadata ----
LABEL maintainer="Data Engineering Team"
LABEL description="Banking Lakehouse — containerized Kafka consumer + Trino query client"
LABEL version="1.0"

# ---- Environment defaults (override at runtime) ----
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    AWS_REGION=us-east-1 \
    KAFKA_BOOTSTRAP_SERVERS=my-cluster-kafka-bootstrap.lakehouse-ingest.svc.cluster.local:9092 \
    KAFKA_TOPIC=finacle-transactions \
    KAFKA_USERNAME=app-user \
    S3_ENDPOINT=http://minio-api.lakehouse-data.svc.cluster.local:9000 \
    S3_ACCESS_KEY=minioadmin \
    ICEBERG_WAREHOUSE=s3a://lakehouse-warehouse/warehouse \
    NESSIE_URI=http://nessie.lakehouse-catalog.svc:19120/api/v2 \
    TRINO_HOST=trino.lakehouse-catalog.svc.cluster.local \
    TRINO_PORT=8080 \
    TRINO_USER=admin \
    TRINO_CATALOG=iceberg \
    TRINO_SCHEMA=bronze \
    TRINO_HTTP_SCHEME=http

# ---- Download Iceberg / Nessie / Kafka jars at build time ----
# This avoids runtime downloads — faster startup, works offline
WORKDIR /opt/spark/jars
RUN set -eux; \
    for jar in \
        "org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar" \
        "org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar" \
        "org/apache/iceberg/iceberg-nessie/1.7.1/iceberg-nessie-1.7.1.jar" \
        "org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.92.1/nessie-spark-extensions-3.5_2.12-0.92.1.jar" \
        "org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.4/spark-sql-kafka-0-10_2.12-3.5.4.jar" \
        "org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.4/spark-token-provider-kafka-0-10_2.12-3.5.4.jar" \
        "org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar" \
        "org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" \
    ; do \
        filename=$(basename "$jar"); \
        curl -fsSL -o "$filename" "https://repo1.maven.org/maven2/$jar"; \
    done

# ---- Install Python dependencies ----
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---- Copy application code ----
COPY app/ /app/app/

# ---- Create entrypoint script ----
# Routes kafka mode → spark-submit, trino mode → python
RUN cat > /usr/local/bin/entrypoint.sh <<'EOF'
#!/bin/bash
set -e

# Parse --mode flag to decide execution strategy
MODE=""
for arg in "$@"; do
    if [ "$prev" = "--mode" ]; then
        MODE="$arg"
        break
    fi
    prev="$arg"
done

cd /app

if [ "$MODE" = "kafka" ]; then
    echo "==> Running kafka mode via spark-submit"
    exec /opt/spark/bin/spark-submit \
        --conf spark.driver.extraJavaOptions=-Daws.region=${AWS_REGION} \
        --conf spark.executor.extraJavaOptions=-Daws.region=${AWS_REGION} \
        -m app.index "$@"
elif [ "$MODE" = "trino" ]; then
    echo "==> Running trino mode"
    exec python -m app.index "$@"
else
    echo "==> Dispatching (mode not pre-detected): python -m app.index"
    exec python -m app.index "$@"
fi
EOF

RUN chmod +x /usr/local/bin/entrypoint.sh && \
    chown -R 185:185 /app

# ---- Run as non-root user (Spark's default UID) ----
USER 185

# ---- Entrypoint and default command ----
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["--mode", "trino"]