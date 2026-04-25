#!/bin/bash
set -e

MODE=""
prev=""
for arg in "$@"; do
    if [ "$prev" = "--mode" ]; then
        MODE="$arg"
        break
    fi
    prev="$arg"
done

cd /app

if [ "$MODE" = "kafka" ]; then
    echo "==> Running kafka consume mode via spark-submit"
    exec /opt/spark/bin/spark-submit \
        --conf spark.driver.extraJavaOptions=-Daws.region=${AWS_REGION} \
        --conf spark.executor.extraJavaOptions=-Daws.region=${AWS_REGION} \
        /app/app/index.py "$@"      
elif [ "$MODE" = "produce" ]; then
    echo "==> Running produce mode (Kafka bulk producer)"
    exec python3 -m app.index "$@"
elif [ "$MODE" = "trino" ]; then
    echo "==> Running trino query mode"
    exec python3 -m app.index "$@"
else
    echo "==> Default: python3 -m app.index"
    exec python3 -m app.index "$@"
fi
