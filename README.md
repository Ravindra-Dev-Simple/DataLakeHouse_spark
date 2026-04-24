# Banking Lakehouse — Containerized Pipeline

A modular, containerized Python application that handles both:

1. **Kafka → Iceberg ingestion** (PySpark, Nessie catalog, MinIO storage)
2. **Trino-based analytics queries** on Bronze layer tables

Designed for deployment on OpenShift / Kubernetes.

---

## Architecture

```
 Kafka ──→ Spark (kafka mode)  ──→  Nessie catalog
             │                          │
             ▼                          │
         MinIO (Parquet)  ◄─────────────┘
             ▲
             │
          Trino  ◄──  Python client (trino mode)
             ▲
             │
          Metabase
```

---

## Project Structure

```
lakehouse-app/
├── app/
│   ├── __init__.py
│   ├── index.py              # CLI entrypoint
│   ├── config.py             # env-based config
│   ├── kafka_consumer.py     # Kafka → Iceberg
│   ├── trino_client.py       # Trino analytics
│   └── utils.py              # logging helpers
├── requirements.txt
├── Dockerfile
├── .dockerignore
└── README.md
```

---

## Quick start

### 1. Build the image

```bash
docker build -t lakehouse-app:1.0 .
```

### 2. Run in Trino query mode (lightweight)

```bash
docker run --rm \
  -e TRINO_HOST=trino.lakehouse-catalog.svc.cluster.local \
  -e TRINO_PORT=8080 \
  lakehouse-app:1.0 --mode trino --section counts
```

### 3. Run in Kafka ingestion mode

```bash
docker run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=my-cluster-kafka-bootstrap:9092 \
  -e KAFKA_PASSWORD='your-secret' \
  -e S3_SECRET_KEY='your-minio-secret' \
  lakehouse-app:1.0 --mode kafka --topic finacle-transactions
```

---

## Environment variables

| Variable                  | Default                                          | Purpose                    |
|---------------------------|--------------------------------------------------|----------------------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `my-cluster-kafka-bootstrap...:9092`             | Kafka endpoint             |
| `KAFKA_TOPIC`             | `finacle-transactions`                           | Default topic              |
| `KAFKA_USERNAME`          | `app-user`                                       | SASL username              |
| `KAFKA_PASSWORD`          | (change)                                         | SASL password              |
| `S3_ENDPOINT`             | `http://minio-api...:9000`                       | MinIO endpoint             |
| `S3_ACCESS_KEY`           | `minioadmin`                                     | MinIO access key           |
| `S3_SECRET_KEY`           | (change)                                         | MinIO secret key           |
| `NESSIE_URI`              | `http://nessie...:19120/api/v2`                  | Nessie catalog URL         |
| `NESSIE_REF`              | `main`                                           | Nessie branch              |
| `ICEBERG_WAREHOUSE`       | `s3a://lakehouse-warehouse/warehouse`            | Iceberg root path          |
| `TRINO_HOST`              | `trino.lakehouse-catalog.svc.cluster.local`      | Trino coordinator host     |
| `TRINO_PORT`              | `8080`                                           | Trino port                 |
| `TRINO_CATALOG`           | `iceberg`                                        | Default Trino catalog      |
| `TRINO_SCHEMA`            | `bronze`                                         | Default Trino schema       |

---

## CLI usage

Run `--help` to see all options:

```bash
docker run --rm lakehouse-app:1.0 --help
```

### Kafka mode options

```
--mode kafka                              # batch mode (default)
--mode kafka --topic <topic>              # specific topic
--mode kafka --consume-mode streaming     # continuous streaming
```

### Trino mode options

```
--mode trino                              # run all query sections
--mode trino --section counts             # row count discovery
--mode trino --section txn                # transaction analytics
--mode trino --section risk               # AML / NPA / CIBIL
```

### Utility

```
--show-config                             # print resolved config & exit
```

---

## Build and push to Docker Hub

```bash
# Build with tag
docker build -t yourname/lakehouse-app:1.0 .

# Login
docker login

# Push
docker push yourname/lakehouse-app:1.0
```

---

## OpenShift deployment

### Push to internal registry (recommended)

```bash
oc registry login

# Tag for OpenShift internal registry
docker tag lakehouse-app:1.0 \
  default-route-openshift-image-registry.apps.YOUR-CLUSTER/lakehouse-catalog/lakehouse-app:1.0

docker push \
  default-route-openshift-image-registry.apps.YOUR-CLUSTER/lakehouse-catalog/lakehouse-app:1.0
```

### Or use OpenShift BuildConfig (no local Docker needed)

```bash
# Create a BuildConfig
oc new-build \
  --strategy docker \
  --binary \
  --name lakehouse-app \
  -n lakehouse-catalog

# Build from current directory
oc start-build lakehouse-app \
  --from-dir . \
  -n lakehouse-catalog \
  --follow
```

### Deploy as a Job

**trino-job.yaml:**

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: lakehouse-query-counts
  namespace: lakehouse-catalog
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: app
        image: image-registry.openshift-image-registry.svc:5000/lakehouse-catalog/lakehouse-app:1.0
        args: ["--mode", "trino", "--section", "counts"]
```

**kafka-job.yaml:**

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: lakehouse-ingest-transactions
  namespace: lakehouse-ingest
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: app
        image: image-registry.openshift-image-registry.svc:5000/lakehouse-ingest/lakehouse-app:1.0
        args: ["--mode", "kafka", "--topic", "finacle-transactions"]
        env:
          - name: KAFKA_PASSWORD
            valueFrom:
              secretKeyRef:
                name: kafka-creds
                key: password
          - name: S3_SECRET_KEY
            valueFrom:
              secretKeyRef:
                name: minio-creds
                key: secret-key
        resources:
          requests:
            memory: "2Gi"
            cpu: "1"
          limits:
            memory: "4Gi"
            cpu: "2"
```

```bash
oc apply -f trino-job.yaml
oc logs -f job/lakehouse-query-counts -n lakehouse-catalog
oc delete job lakehouse-query-counts -n lakehouse-catalog
```

### Create secrets (one-time)

```bash
oc create secret generic kafka-creds \
  --from-literal=password='your-kafka-password' \
  -n lakehouse-ingest

oc create secret generic minio-creds \
  --from-literal=secret-key='MyStr0ngP@ssw0rd123' \
  -n lakehouse-ingest
```

---

## Local development

```bash
# Install deps locally
pip install -r requirements.txt

# Run Trino mode
python -m app.index --mode trino --section counts

# Run Kafka mode (requires spark-submit setup)
spark-submit \
  --conf spark.driver.extraJavaOptions=-Daws.region=us-east-1 \
  --conf spark.executor.extraJavaOptions=-Daws.region=us-east-1 \
  -m app.index --mode kafka --topic finacle-transactions
```

---

## Troubleshooting

### Trino mode — "bronze schema not found"

Kafka consumer hasn't registered any tables yet. Run ingestion first:

```bash
docker run --rm lakehouse-app:1.0 --mode kafka --topic finacle-transactions
```

### Kafka mode — "Unable to load region"

Make sure `AWS_REGION=us-east-1` is set. The Dockerfile sets it by default but env can override.

### "Connection refused" to Trino

Verify the service is reachable from your pod's namespace:

```bash
oc exec <your-pod> -- curl -s http://$TRINO_HOST:$TRINO_PORT/v1/info
```

### Spark consumer crashes — missing jars

The Dockerfile pre-downloads all required jars. If you rebuild and break something, confirm with:

```bash
docker run --rm --entrypoint ls lakehouse-app:1.0 /opt/spark/jars | grep -iE "iceberg|kafka|nessie"
```

---

## License

Proprietary — Banking internal.