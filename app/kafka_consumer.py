"""
================================================================
 kafka_consumer.py — Kafka → Iceberg (Nessie + MinIO)
================================================================
 PySpark Structured Streaming (or batch) reads messages from Kafka,
 parses JSON, writes to Iceberg Bronze tables in MinIO,
 catalog registered with Nessie.
================================================================
"""
from datetime import datetime
from typing import Optional

from app import config
from app.utils import get_logger, print_section

log = get_logger("consumer")


# ----------------------------------------------------------------
# SCHEMAS — one per Kafka topic
# ----------------------------------------------------------------
def _build_schemas():
    """Build Iceberg schemas lazily (PySpark import happens only here)."""
    from pyspark.sql.types import StructType, StructField, StringType

    SCHEMA_TRANSACTIONS = StructType([
        StructField("txn_id", StringType()),
        StructField("account_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("txn_type", StringType()),
        StructField("amount", StringType()),
        StructField("currency", StringType()),
        StructField("txn_date", StringType()),
        StructField("value_date", StringType()),
        StructField("branch_code", StringType()),
        StructField("channel", StringType()),
        StructField("narration", StringType()),
        StructField("balance_after", StringType()),
        StructField("status", StringType()),
        StructField("ref_number", StringType()),
    ])

    SCHEMA_CUSTOMERS = StructType([
        StructField("customer_id", StringType()),
        StructField("name", StringType()),
        StructField("pan", StringType()),
        StructField("aadhaar_masked", StringType()),
        StructField("mobile", StringType()),
        StructField("email", StringType()),
        StructField("dob", StringType()),
        StructField("gender", StringType()),
        StructField("kyc_status", StringType()),
        StructField("kyc_date", StringType()),
        StructField("risk_category", StringType()),
        StructField("branch_code", StringType()),
        StructField("account_type", StringType()),
        StructField("account_number", StringType()),
        StructField("account_open_date", StringType()),
        StructField("occupation", StringType()),
        StructField("annual_income", StringType()),
        StructField("address_city", StringType()),
        StructField("address_state", StringType()),
        StructField("nominee_name", StringType()),
    ])

    SCHEMA_AML_ALERTS = StructType([
        StructField("alert_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("customer_name", StringType()),
        StructField("alert_date", StringType()),
        StructField("alert_type", StringType()),
        StructField("risk_score", StringType()),
        StructField("rule_triggered", StringType()),
        StructField("transaction_ids", StringType()),
        StructField("total_amount", StringType()),
        StructField("currency", StringType()),
        StructField("description", StringType()),
        StructField("status", StringType()),
        StructField("assigned_to", StringType()),
        StructField("priority", StringType()),
        StructField("due_date", StringType()),
        StructField("resolution", StringType()),
        StructField("resolution_date", StringType()),
        StructField("sar_filed", StringType()),
        StructField("sar_reference", StringType()),
    ])

    SCHEMA_CIBIL = StructType([
        StructField("report_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("customer_name", StringType()),
        StructField("pan", StringType()),
        StructField("cibil_score", StringType()),
        StructField("score_date", StringType()),
        StructField("total_accounts", StringType()),
        StructField("active_accounts", StringType()),
        StructField("closed_accounts", StringType()),
        StructField("overdue_accounts", StringType()),
        StructField("total_outstanding", StringType()),
        StructField("secured_outstanding", StringType()),
        StructField("unsecured_outstanding", StringType()),
        StructField("credit_utilization_pct", StringType()),
        StructField("enquiry_count_6m", StringType()),
        StructField("enquiry_count_12m", StringType()),
        StructField("dpd_30_count", StringType()),
        StructField("dpd_60_count", StringType()),
        StructField("dpd_90_count", StringType()),
        StructField("written_off_amount", StringType()),
        StructField("suit_filed_count", StringType()),
        StructField("wilful_defaulter", StringType()),
        StructField("report_pull_date", StringType()),
        StructField("report_source", StringType()),
    ])

    SCHEMA_NPA = StructType([
        StructField("report_date", StringType()),
        StructField("loan_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("customer_name", StringType()),
        StructField("loan_type", StringType()),
        StructField("branch_code", StringType()),
        StructField("sanctioned_amount", StringType()),
        StructField("outstanding_principal", StringType()),
        StructField("outstanding_interest", StringType()),
        StructField("total_outstanding", StringType()),
        StructField("dpd", StringType()),
        StructField("asset_classification", StringType()),
        StructField("npa_status", StringType()),
        StructField("npa_date", StringType()),
        StructField("provision_required_pct", StringType()),
        StructField("provision_amount", StringType()),
        StructField("collateral_type", StringType()),
        StructField("collateral_value", StringType()),
        StructField("net_npa_exposure", StringType()),
        StructField("recovery_action", StringType()),
        StructField("recovery_amount", StringType()),
        StructField("last_review_date", StringType()),
        StructField("next_review_date", StringType()),
        StructField("relationship_manager", StringType()),
        StructField("remarks", StringType()),
    ])

    # Topic name → (schema, full iceberg table path)
    return {
        "finacle-transactions": (SCHEMA_TRANSACTIONS, "lakehouse.bronze.finacle_transactions")
    #     "finacle-customers":    (SCHEMA_CUSTOMERS,    "lakehouse.bronze.finacle_customers"),
    #     "aml-alerts":           (SCHEMA_AML_ALERTS,   "lakehouse.bronze.aml_alerts"),
    #     "cibil-bureau":         (SCHEMA_CIBIL,        "lakehouse.bronze.cibil_bureau"),
    #     "npa-report":           (SCHEMA_NPA,          "lakehouse.bronze.npa_report"),
    }


# ----------------------------------------------------------------
# KAFKA AUTH OPTIONS
# ----------------------------------------------------------------
def _get_kafka_options() -> dict:
    """Kafka source options (SASL/SCRAM)."""
    jaas_config = (
        f'org.apache.kafka.common.security.scram.ScramLoginModule required '
        f'username="{config.KAFKA_USERNAME}" password="{config.KAFKA_PASSWORD}";'
    )
    return {
        "kafka.bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "kafka.security.protocol": config.KAFKA_SECURITY_PROTOCOL,
        "kafka.sasl.mechanism":    config.KAFKA_SASL_MECHANISM,
        "kafka.sasl.jaas.config":  jaas_config,
        "startingOffsets":         "earliest",
        "failOnDataLoss":          "false",
    }


# ----------------------------------------------------------------
# SPARK SESSION BUILDER
# ----------------------------------------------------------------
def _create_spark_session():
    """Create Spark session with Iceberg + Nessie + MinIO wiring."""
    from pyspark.sql import SparkSession

    log.info("Creating Spark session")
    log.info("  Nessie:    %s", config.NESSIE_URI)
    log.info("  MinIO:     %s", config.S3_ENDPOINT)
    log.info("  Warehouse: %s", config.ICEBERG_WAREHOUSE)

    spark = (
        SparkSession.builder
        .appName("LakehouseKafkaConsumer")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Iceberg catalog backed by Nessie
        .config("spark.sql.catalog.lakehouse",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.catalog-impl",
                "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.lakehouse.uri",       config.NESSIE_URI)
        .config("spark.sql.catalog.lakehouse.ref",       config.NESSIE_REF)
        .config("spark.sql.catalog.lakehouse.warehouse", config.ICEBERG_WAREHOUSE)
        # S3 FileIO for MinIO
        .config("spark.sql.catalog.lakehouse.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint",          config.S3_ENDPOINT)
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.sql.catalog.lakehouse.s3.access-key-id",     config.S3_ACCESS_KEY)
        .config("spark.sql.catalog.lakehouse.s3.secret-access-key", config.S3_SECRET_KEY)
        .config("spark.sql.catalog.lakehouse.s3.region",            config.S3_REGION)
        # Hadoop S3A (fallback)
        .config("spark.hadoop.fs.s3a.endpoint",          config.S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        config.S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",        config.S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.defaultCatalog", "lakehouse")
        .getOrCreate()
    )

    # Ensure namespaces exist
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")

    log.info("Spark session ready")
    return spark


# ----------------------------------------------------------------
# PROCESSING FUNCTIONS
# ----------------------------------------------------------------
def _process_batch(spark, kafka_options, topic, schema, iceberg_table) -> int:
    """One topic ka batch — read all available, APPEND to Iceberg+Nessie."""
    from pyspark.sql.functions import col, from_json, current_timestamp, lit

    log.info("Processing: %s → %s", topic, iceberg_table)

    # ---- Read from Kafka ----
    df_raw = (
        spark.read
        .format("kafka")
        .options(**kafka_options)
        .option("subscribe", topic)
        .load()
    )

    msg_count = df_raw.count()
    if msg_count == 0:
        log.warning("No messages in topic '%s'", topic)
        return 0

    log.info("Found %d messages in Kafka", msg_count)

    # ---- Parse JSON ----
    df_parsed = (
        df_raw
        .selectExpr("CAST(key AS STRING) as kafka_key",
                    "CAST(value AS STRING) as json_value")
        .select(
            col("kafka_key"),
            from_json(col("json_value"), schema).alias("data")
        )
        .select("data.*")
        .withColumn("_ingestion_ts", current_timestamp())
        .withColumn("_source", lit("kafka"))
    )

    row_count = df_parsed.count()
    log.info("Parsed %d rows", row_count)

    # ---- Write to Iceberg (with Nessie registration) ----
    # Check if table already exists in Nessie catalog
    try:
        table_exists = spark.catalog.tableExists(iceberg_table)
        log.info("Table existence check: %s = %s", iceberg_table, table_exists)
    except Exception as e:
        log.warning("Could not check table existence: %s", e)
        table_exists = False

    if not table_exists:
        # First time — CREATE the table (registers in Nessie)
        log.info("Creating new Iceberg table: %s", iceberg_table)
        (
            df_parsed.writeTo(iceberg_table)
            .tableProperty("format-version", "2")
            .tableProperty("write.format.default", "parquet")
            .tableProperty("write.parquet.compression-codec", "snappy")
            .create()
        )
        log.info("✓ Table created and registered in Nessie")
    else:
        # Subsequent runs — APPEND (preserves history)
        log.info("Appending to existing table: %s", iceberg_table)
        df_parsed.writeTo(iceberg_table).append()
        log.info("✓ Appended to existing Nessie-registered table")

    log.info("✓ Written %d rows to %s", row_count, iceberg_table)

    # ---- Verify Nessie registration ----
    try:
        verify_count = spark.table(iceberg_table).count()
        log.info("✓ Nessie verified — table has %d total rows", verify_count)
    except Exception as e:
        log.error("✗ Nessie verification FAILED: %s", e)
        log.error("   Files in MinIO but NOT registered in Nessie!")

    return row_count


def _process_streaming(spark, kafka_options, topic, schema, iceberg_table,
                       checkpoint_dir: str):
    """One topic ka continuous stream."""
    import os
    from pyspark.sql.functions import col, from_json, current_timestamp, lit

    log.info("Starting stream: %s → %s", topic, iceberg_table)

    df_stream = (
        spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .option("subscribe", topic)
        .load()
    )

    df_parsed = (
        df_stream
        .selectExpr("CAST(key AS STRING) as kafka_key",
                    "CAST(value AS STRING) as json_value")
        .select(
            col("kafka_key"),
            from_json(col("json_value"), schema).alias("data")
        )
        .select("data.*")
        .withColumn("_ingestion_ts", current_timestamp())
        .withColumn("_source", lit("kafka"))
    )

    checkpoint = os.path.join(checkpoint_dir, topic)
    query = (
        df_parsed.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .option("fanout-enabled", "true")
        .toTable(iceberg_table)
    )
    return query


# ----------------------------------------------------------------
# PUBLIC ENTRY POINT
# ----------------------------------------------------------------
def run(mode: str = "batch",
        topic: Optional[str] = None,
        checkpoint_dir: str = "/tmp/kafka-iceberg-checkpoints") -> int:
    """
    Main entrypoint for kafka → iceberg consumer.

    Args:
        mode: "batch" or "streaming"
        topic: specific topic name (None = all configured topics)
        checkpoint_dir: for streaming mode
    Returns:
        0 on success, non-zero on failure
    """
    print_section(f"KAFKA CONSUMER — {mode.upper()} MODE")
    log.info("Started at: %s", datetime.now().isoformat())

    try:
        topics_config = _build_schemas()
    except ImportError as e:
        log.error("PySpark not installed: %s", e)
        log.error("Install: pip install pyspark==3.5.4")
        return 1

    # Default topic from env (single topic mode)
    if topic is None and config.KAFKA_TOPIC:
        topic = config.KAFKA_TOPIC

    if topic:
        if topic not in topics_config:
            log.error("Unknown topic: %s", topic)
            log.error("Available: %s", list(topics_config.keys()))
            return 1
        topics_config = {topic: topics_config[topic]}

    spark = _create_spark_session()
    kafka_options = _get_kafka_options()

    if mode == "batch":
        total = 0
        for tp, (schema, table) in topics_config.items():
            try:
                total += _process_batch(spark, kafka_options, tp, schema, table)
            except Exception as e:
                log.error("Topic %s failed: %s", tp, e)

        print_section(f"BATCH COMPLETE — {total} total rows", char="=")

        log.info("Verifying...")
        for tp, (_, table) in topics_config.items():
            try:
                count = spark.table(table).count()
                log.info("  %s: %d rows", table, count)
            except Exception:
                log.warning("  %s: table not created", table)

        spark.stop()
        return 0

    elif mode == "streaming":
        queries = []
        for tp, (schema, table) in topics_config.items():
            try:
                q = _process_streaming(spark, kafka_options, tp, schema,
                                       table, checkpoint_dir)
                queries.append(q)
                log.info("Started streaming for %s", tp)
            except Exception as e:
                log.error("Stream start failed for %s: %s", tp, e)

        print_section(f"STREAMING — {len(queries)} active", char="=")
        log.info("Press Ctrl+C to stop")

        try:
            for q in queries:
                q.awaitTermination()
        except KeyboardInterrupt:
            log.info("Stopping streams...")
            for q in queries:
                q.stop()

        spark.stop()
        return 0

    else:
        log.error("Invalid mode: %s (use 'batch' or 'streaming')", mode)
        return 1