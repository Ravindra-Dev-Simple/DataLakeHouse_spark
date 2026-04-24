"""
================================================================
 trino_transactions.py — Query Bronze transactions via Trino
================================================================
 Transactions-only client. Runs the same queries that power your
 Metabase dashboard, but from Python for local inspection.

 Only reads iceberg.bronze.finacle_transactions — no joins to
 customers/AML/NPA/CIBIL (those topics aren't generated yet).

 Entry point: run(section=None)
   section = "kpi" | "volume" | "pattern" | "risk" | None (all)

 Example (from main.py):
   from app import trino_transactions
   trino_transactions.run(section="kpi")
================================================================
"""
from contextlib import contextmanager
from datetime import datetime
from typing import Optional

from app import config
from app.utils import get_logger, print_section, print_subsection

log = get_logger("trino-txn")

# The one table this module queries
TXN_TABLE = "iceberg.bronze.finacle_transactions"


# ----------------------------------------------------------------
# CONNECTION (identical pattern to trino_client.py)
# ----------------------------------------------------------------
@contextmanager
def _connect():
    """Context-managed Trino connection."""
    try:
        from trino.dbapi import connect
    except ImportError:
        log.error("trino package missing. Install: pip install trino")
        raise

    log.info("Trino: %s://%s:%d (catalog=%s, schema=%s)",
             config.TRINO_HTTP_SCHEME, config.TRINO_HOST, config.TRINO_PORT,
             config.TRINO_CATALOG, config.TRINO_SCHEMA)

    conn = connect(
        host=config.TRINO_HOST,
        port=config.TRINO_PORT,
        user=config.TRINO_USER,
        catalog=config.TRINO_CATALOG,
        schema=config.TRINO_SCHEMA,
        http_scheme=config.TRINO_HTTP_SCHEME,
    )
    try:
        yield conn
    finally:
        conn.close()


def _query(conn, sql: str):
    """Run SQL → pandas DataFrame."""
    import pandas as pd
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=cols)


def _show(df, title: str, max_rows: int = 25):
    """Pretty-print DataFrame."""
    from tabulate import tabulate
    print_subsection(title)
    if df is None or df.empty:
        print("(no rows)")
        return
    print(tabulate(df.head(max_rows), headers="keys", tablefmt="psql",
                   showindex=False))
    if len(df) > max_rows:
        print(f"... ({len(df) - max_rows} more rows)")


def _run_and_show(conn, sql: str, title: str, max_rows: int = 25):
    """Query + print + graceful error handling."""
    try:
        from trino.exceptions import TrinoQueryError, TrinoUserError
        try:
            df = _query(conn, sql)
            _show(df, title, max_rows=max_rows)
            return df
        except (TrinoUserError, TrinoQueryError) as e:
            msg = e.message if hasattr(e, "message") else str(e)
            log.warning("Query failed [%s]: %s", title, msg[:200])
            return None
    except Exception as e:
        log.error("Unexpected error [%s]: %s", title, e)
        return None


def _table_exists(conn, schema: str, table: str) -> bool:
    """Check if table exists before querying it."""
    try:
        df = _query(conn, f"SHOW TABLES FROM iceberg.{schema} LIKE '{table}'")
        return df is not None and not df.empty
    except Exception:
        return False


# ----------------------------------------------------------------
# SECTION: kpi — summary numbers (for dashboard big-number cards)
# ----------------------------------------------------------------
def _section_kpi(conn):
    print_section("SECTION 1: KPI — Summary numbers")

    _run_and_show(conn, f"""
        SELECT
            COUNT(*)                                          AS total_transactions,
            COUNT(DISTINCT customer_id)                       AS active_customers,
            COUNT(DISTINCT account_id)                        AS unique_accounts,
            COUNT(DISTINCT branch_code)                       AS unique_branches,
            ROUND(SUM(TRY_CAST(amount AS DECIMAL(18,2))), 2)  AS total_volume_inr,
            ROUND(AVG(TRY_CAST(amount AS DECIMAL(18,2))), 2)  AS avg_amount_inr,
            MIN(TRY_CAST(txn_date AS TIMESTAMP))              AS earliest_txn,
            MAX(TRY_CAST(txn_date AS TIMESTAMP))              AS latest_txn,
            MAX(_ingestion_ts)                                AS last_ingested
        FROM {TXN_TABLE}
    """, "Overall KPIs")


# ----------------------------------------------------------------
# SECTION: volume — daily & hourly trends
# ----------------------------------------------------------------
def _section_volume(conn):
    print_section("SECTION 2: Volume — Trends over time")

    _run_and_show(conn, f"""
        SELECT
            DATE(TRY_CAST(txn_date AS TIMESTAMP))             AS txn_day,
            COUNT(*)                                          AS txn_count,
            ROUND(SUM(TRY_CAST(amount AS DECIMAL(18,2))), 2)  AS total_amount,
            COUNT(DISTINCT customer_id)                       AS active_customers
        FROM {TXN_TABLE}
        WHERE TRY_CAST(txn_date AS TIMESTAMP) IS NOT NULL
          AND TRY_CAST(amount   AS DECIMAL(18,2)) IS NOT NULL
        GROUP BY DATE(TRY_CAST(txn_date AS TIMESTAMP))
        ORDER BY txn_day DESC
        LIMIT 30
    """, "Daily transaction volume (last 30 days)")

    _run_and_show(conn, f"""
        SELECT
            HOUR(TRY_CAST(txn_date AS TIMESTAMP))             AS txn_hour,
            COUNT(*)                                          AS txn_count,
            ROUND(AVG(TRY_CAST(amount AS DECIMAL(18,2))), 2)  AS avg_amount,
            ROUND(SUM(TRY_CAST(amount AS DECIMAL(18,2))), 2)  AS total_amount
        FROM {TXN_TABLE}
        WHERE TRY_CAST(txn_date AS TIMESTAMP) IS NOT NULL
          AND TRY_CAST(amount   AS DECIMAL(18,2)) IS NOT NULL
        GROUP BY HOUR(TRY_CAST(txn_date AS TIMESTAMP))
        ORDER BY txn_hour
    """, "Hourly transaction pattern (0-23h)")


# ----------------------------------------------------------------
# SECTION: pattern — distribution by channel/type/branch
# ----------------------------------------------------------------
def _section_pattern(conn):
    print_section("SECTION 3: Pattern — Distribution breakdowns")

    _run_and_show(conn, f"""
        SELECT
            UPPER(TRIM(channel))                              AS channel,
            COUNT(*)                                          AS txn_count,
            ROUND(SUM(TRY_CAST(amount AS DECIMAL(18,2))), 2)  AS total_amount,
            ROUND(AVG(TRY_CAST(amount AS DECIMAL(18,2))), 2)  AS avg_amount,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
        FROM {TXN_TABLE}
        WHERE TRY_CAST(amount AS DECIMAL(18,2)) IS NOT NULL
          AND channel IS NOT NULL
        GROUP BY UPPER(TRIM(channel))
        ORDER BY txn_count DESC
    """, "Channel distribution")

    _run_and_show(conn, f"""
        SELECT
            UPPER(TRIM(txn_type))                             AS txn_type,
            COUNT(*)                                          AS txn_count,
            ROUND(SUM(TRY_CAST(amount AS DECIMAL(18,2))), 2)  AS total_amount,
            ROUND(MIN(TRY_CAST(amount AS DECIMAL(18,2))), 2)  AS min_amount,
            ROUND(MAX(TRY_CAST(amount AS DECIMAL(18,2))), 2)  AS max_amount
        FROM {TXN_TABLE}
        WHERE TRY_CAST(amount AS DECIMAL(18,2)) IS NOT NULL
          AND txn_type IS NOT NULL
        GROUP BY UPPER(TRIM(txn_type))
        ORDER BY total_amount DESC
    """, "Transaction type breakdown")

    _run_and_show(conn, f"""
        SELECT
            branch_code,
            COUNT(*)                                          AS txn_count,
            ROUND(SUM(TRY_CAST(amount AS DECIMAL(18,2))), 2)  AS total_amount,
            COUNT(DISTINCT customer_id)                       AS unique_customers
        FROM {TXN_TABLE}
        WHERE TRY_CAST(amount AS DECIMAL(18,2)) IS NOT NULL
          AND branch_code IS NOT NULL
        GROUP BY branch_code
        ORDER BY total_amount DESC
        LIMIT 15
    """, "Top 15 branches by volume")


# ----------------------------------------------------------------
# SECTION: risk — AML signals computable from transactions alone
# ----------------------------------------------------------------
def _section_risk(conn):
    print_section("SECTION 4: Risk — AML signals (no external joins)")

    _run_and_show(conn, f"""
        SELECT
            txn_id,
            customer_id,
            branch_code,
            TRY_CAST(amount AS DECIMAL(18,2))                 AS amount,
            narration,
            txn_date
        FROM {TXN_TABLE}
        WHERE UPPER(TRIM(channel)) = 'CASH'
          AND TRY_CAST(amount AS DECIMAL(18,2)) >= 1000000
        ORDER BY TRY_CAST(amount AS DECIMAL(18,2)) DESC
        LIMIT 20
    """, "High-value cash transactions (>= INR 10L) — CTR candidates")

    _run_and_show(conn, f"""
        SELECT
            customer_id,
            DATE(TRY_CAST(txn_date AS TIMESTAMP))             AS txn_day,
            COUNT(*)                                          AS cash_txn_count,
            ROUND(SUM(TRY_CAST(amount AS DECIMAL(18,2))), 2)  AS total_cash
        FROM {TXN_TABLE}
        WHERE UPPER(TRIM(channel)) = 'CASH'
          AND TRY_CAST(amount   AS DECIMAL(18,2)) BETWEEN 900000 AND 999999
          AND TRY_CAST(txn_date AS TIMESTAMP) IS NOT NULL
        GROUP BY customer_id, DATE(TRY_CAST(txn_date AS TIMESTAMP))
        HAVING COUNT(*) >= 2
        ORDER BY cash_txn_count DESC, total_cash DESC
        LIMIT 20
    """, "Structuring suspects (multiple 9L-9.99L cash txns/day)")

    _run_and_show(conn, f"""
        SELECT
            customer_id,
            COUNT(*)                                          AS txn_count,
            ROUND(SUM(TRY_CAST(amount AS DECIMAL(18,2))), 2)  AS total_amount,
            COUNT(DISTINCT UPPER(TRIM(channel)))              AS channels_used,
            COUNT(DISTINCT branch_code)                       AS branches_used,
            MIN(txn_date)                                     AS first_txn,
            MAX(txn_date)                                     AS last_txn
        FROM {TXN_TABLE}
        WHERE TRY_CAST(amount AS DECIMAL(18,2)) IS NOT NULL
          AND customer_id IS NOT NULL
        GROUP BY customer_id
        HAVING COUNT(*) >= 20
        ORDER BY txn_count DESC
        LIMIT 15
    """, "High-velocity customers (>= 20 transactions)")


# ----------------------------------------------------------------
# PUBLIC ENTRY POINT
# ----------------------------------------------------------------
_SECTIONS = {
    "kpi":     _section_kpi,
    "volume":  _section_volume,
    "pattern": _section_pattern,
    "risk":    _section_risk,
}


def run(section: Optional[str] = None) -> int:
    """
    Run Trino transaction analytics queries.

    Args:
        section: "kpi", "volume", "pattern", "risk", or None (= all sections)
    Returns:
        0 on success, non-zero on failure
    """
    print_section("TRINO TRANSACTIONS CLIENT")
    log.info("Started at: %s", datetime.now().isoformat())

    try:
        with _connect() as conn:
            # Sanity check: iceberg catalog + bronze schema
            df = _query(conn, "SHOW CATALOGS")
            catalogs = df["Catalog"].tolist() if df is not None and not df.empty else []
            log.info("Available catalogs: %s", catalogs)

            if "iceberg" not in catalogs:
                log.error("'iceberg' catalog not found in Trino")
                log.error("Check Trino configmap — iceberg.properties must be mounted")
                return 1

            df = _query(conn, "SHOW SCHEMAS FROM iceberg")
            schemas = df["Schema"].tolist() if df is not None and not df.empty else []
            log.info("Schemas: %s", schemas)

            if "bronze" not in schemas:
                log.error("'bronze' schema not found")
                log.error("Run kafka consumer first: --mode kafka")
                return 1

            # Sanity check: finacle_transactions exists and has data
            if not _table_exists(conn, "bronze", "finacle_transactions"):
                log.error("Table %s does not exist", TXN_TABLE)
                log.error("Run kafka consumer first to populate Bronze")
                return 1

            df = _query(conn, f"SELECT COUNT(*) AS n FROM {TXN_TABLE}")
            count = int(df.iloc[0, 0]) if df is not None and not df.empty else 0
            log.info("%s: %d rows", TXN_TABLE, count)
            if count == 0:
                log.warning("Table is empty — kafka consumer hasn't written any rows yet")
                return 0

            if section:
                if section not in _SECTIONS:
                    log.error("Unknown section: %s (valid: %s)",
                              section, list(_SECTIONS.keys()))
                    return 1
                log.info("Running section: %s", section)
                _SECTIONS[section](conn)
            else:
                log.info("Running ALL sections")
                for name, fn in _SECTIONS.items():
                    log.info("========== %s ==========", name)
                    try:
                        fn(conn)
                    except Exception as e:
                        log.error("Section '%s' crashed: %s", name, e)

        log.info("✓ Done")
        return 0

    except Exception as e:
        log.exception("Fatal: %s", e)
        return 2



# """
# ================================================================
#  trino_client.py — Query Bronze tables via Trino
# ================================================================
#  Pure-Python client. Connects to Trino, runs analytical queries,
#  prints formatted results. Sections: counts, txn, risk.
# ================================================================
# """
# from contextlib import contextmanager
# from datetime import datetime
# from typing import Optional

# from app import config
# from app.utils import get_logger, print_section, print_subsection

# log = get_logger("trino")


# # ----------------------------------------------------------------
# # CONNECTION
# # ----------------------------------------------------------------
# @contextmanager
# def _connect():
#     """Context-managed Trino connection."""
#     try:
#         from trino.dbapi import connect
#     except ImportError:
#         log.error("trino package missing. Install: pip install trino")
#         raise

#     log.info("Trino: %s://%s:%d (catalog=%s, schema=%s)",
#              config.TRINO_HTTP_SCHEME, config.TRINO_HOST, config.TRINO_PORT,
#              config.TRINO_CATALOG, config.TRINO_SCHEMA)

#     conn = connect(
#         host=config.TRINO_HOST,
#         port=config.TRINO_PORT,
#         user=config.TRINO_USER,
#         catalog=config.TRINO_CATALOG,
#         schema=config.TRINO_SCHEMA,
#         http_scheme=config.TRINO_HTTP_SCHEME,
#     )
#     try:
#         yield conn
#     finally:
#         conn.close()


# def _query(conn, sql: str):
#     """Run SQL → pandas DataFrame."""
#     import pandas as pd
#     cur = conn.cursor()
#     cur.execute(sql)
#     rows = cur.fetchall()
#     cols = [d[0] for d in cur.description] if cur.description else []
#     return pd.DataFrame(rows, columns=cols)


# def _show(df, title: str, max_rows: int = 20):
#     """Pretty-print DataFrame."""
#     from tabulate import tabulate
#     print_subsection(title)
#     if df is None or df.empty:
#         print("(no rows)")
#         return
#     print(tabulate(df.head(max_rows), headers="keys", tablefmt="psql",
#                    showindex=False))
#     if len(df) > max_rows:
#         print(f"... ({len(df) - max_rows} more rows)")


# def _run_and_show(conn, sql: str, title: str, max_rows: int = 20):
#     """Query + print + graceful error handling."""
#     try:
#         from trino.exceptions import TrinoQueryError, TrinoUserError
#         try:
#             df = _query(conn, sql)
#             _show(df, title, max_rows=max_rows)
#             return df
#         except (TrinoUserError, TrinoQueryError) as e:
#             msg = e.message if hasattr(e, "message") else str(e)
#             log.warning("Query failed [%s]: %s", title, msg[:200])
#             return None
#     except Exception as e:
#         log.error("Unexpected error [%s]: %s", title, e)
#         return None


# def _table_exists(conn, schema: str, table: str) -> bool:
#     """Check if table exists (avoid query errors on missing tables)."""
#     try:
#         df = _query(conn, f"SHOW TABLES FROM iceberg.{schema} LIKE '{table}'")
#         return df is not None and not df.empty
#     except Exception:
#         return False


# # ----------------------------------------------------------------
# # QUERY SECTIONS
# # ----------------------------------------------------------------
# def _section_counts(conn):
#     print_section("SECTION 1: Bronze — Discovery & row counts")

#     _run_and_show(conn,
#         "SHOW TABLES FROM iceberg.bronze",
#         "Tables in Bronze")

#     log.info("Counting rows in each known table...")
#     for tbl in config.BRONZE_TABLES:
#         if _table_exists(conn, "bronze", tbl):
#             _run_and_show(conn,
#                 f"SELECT COUNT(*) AS row_count FROM iceberg.bronze.{tbl}",
#                 f"Row count — {tbl}")
#         else:
#             print(f"\n--- {tbl} ---  (table does not exist yet; skipping)")

#     if _table_exists(conn, "bronze", "finacle_transactions"):
#         _run_and_show(conn, """
#             SELECT column_name, data_type
#             FROM iceberg.information_schema.columns
#             WHERE table_schema = 'bronze'
#               AND table_name = 'finacle_transactions'
#             ORDER BY ordinal_position
#         """, "Schema of finacle_transactions")


# def _section_txn(conn):
#     print_section("SECTION 2: Bronze — Transaction analytics")

#     if not _table_exists(conn, "bronze", "finacle_transactions"):
#         log.warning("finacle_transactions doesn't exist. Skipping section.")
#         return

#     _run_and_show(conn, """
#         SELECT
#             UPPER(TRIM(channel)) AS channel,
#             COUNT(*) AS txn_count,
#             ROUND(SUM(TRY_CAST(amount AS DECIMAL(18,2))), 2) AS total_amount,
#             ROUND(AVG(TRY_CAST(amount AS DECIMAL(18,2))), 2) AS avg_amount,
#             COUNT(DISTINCT customer_id) AS unique_customers
#         FROM iceberg.bronze.finacle_transactions
#         WHERE TRY_CAST(amount AS DECIMAL(18,2)) IS NOT NULL
#         GROUP BY UPPER(TRIM(channel))
#         ORDER BY total_amount DESC
#     """, "Channel distribution")

#     _run_and_show(conn, """
#         SELECT
#             branch_code,
#             UPPER(TRIM(txn_type)) AS txn_type,
#             COUNT(*) AS txn_count,
#             ROUND(SUM(TRY_CAST(amount AS DECIMAL(18,2))), 2) AS total_amount,
#             ROUND(MAX(TRY_CAST(amount AS DECIMAL(18,2))), 2) AS max_amount
#         FROM iceberg.bronze.finacle_transactions
#         WHERE TRY_CAST(amount AS DECIMAL(18,2)) IS NOT NULL
#         GROUP BY branch_code, UPPER(TRIM(txn_type))
#         ORDER BY total_amount DESC
#         LIMIT 15
#     """, "Top 15 branch + txn_type combinations")

#     _run_and_show(conn, """
#         SELECT
#             HOUR(TRY_CAST(txn_date AS TIMESTAMP)) AS txn_hour,
#             COUNT(*) AS txn_count,
#             ROUND(SUM(TRY_CAST(amount AS DECIMAL(18,2))), 2) AS total_amount
#         FROM iceberg.bronze.finacle_transactions
#         WHERE TRY_CAST(txn_date AS TIMESTAMP) IS NOT NULL
#           AND TRY_CAST(amount AS DECIMAL(18,2)) IS NOT NULL
#         GROUP BY HOUR(TRY_CAST(txn_date AS TIMESTAMP))
#         ORDER BY txn_hour
#     """, "Hourly transaction pattern")

#     _run_and_show(conn, """
#         SELECT
#             COUNT(*) AS total_rows,
#             SUM(CASE WHEN TRY_CAST(amount AS DECIMAL(18,2)) IS NULL THEN 1 ELSE 0 END) AS bad_amount,
#             SUM(CASE WHEN TRY_CAST(txn_date AS TIMESTAMP) IS NULL THEN 1 ELSE 0 END) AS bad_txn_date,
#             SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS missing_customer
#         FROM iceberg.bronze.finacle_transactions
#     """, "Data quality checks")


# def _section_risk(conn):
#     print_section("SECTION 3: Bronze — Risk views (AML / NPA / CIBIL)")

#     if _table_exists(conn, "bronze", "finacle_transactions"):
#         _run_and_show(conn, """
#             SELECT
#                 txn_id, customer_id,
#                 UPPER(TRIM(channel)) AS channel,
#                 TRY_CAST(amount AS DECIMAL(18,2)) AS amount,
#                 narration, txn_date
#             FROM iceberg.bronze.finacle_transactions
#             WHERE UPPER(TRIM(channel)) = 'CASH'
#               AND TRY_CAST(amount AS DECIMAL(18,2)) >= 1000000
#             ORDER BY TRY_CAST(amount AS DECIMAL(18,2)) DESC
#             LIMIT 20
#         """, "High-value CASH transactions (>= INR 10 lakh)")

#     if _table_exists(conn, "bronze", "aml_alerts"):
#         _run_and_show(conn, """
#             SELECT
#                 alert_id, customer_id, customer_name, alert_type,
#                 TRY_CAST(risk_score AS INTEGER) AS risk_score,
#                 TRY_CAST(total_amount AS DECIMAL(18,2)) AS total_amount,
#                 status, priority, alert_date
#             FROM iceberg.bronze.aml_alerts
#             WHERE UPPER(TRIM(status)) = 'OPEN'
#             ORDER BY TRY_CAST(risk_score AS INTEGER) DESC
#             LIMIT 20
#         """, "Open AML alerts")

#     if _table_exists(conn, "bronze", "npa_report"):
#         _run_and_show(conn, """
#             SELECT
#                 UPPER(TRIM(npa_status)) AS npa_status,
#                 COUNT(*) AS accounts,
#                 ROUND(SUM(TRY_CAST(outstanding_principal AS DECIMAL(18,2))), 2) AS total_outstanding,
#                 ROUND(AVG(TRY_CAST(dpd AS INTEGER)), 0) AS avg_dpd
#             FROM iceberg.bronze.npa_report
#             WHERE TRY_CAST(outstanding_principal AS DECIMAL(18,2)) IS NOT NULL
#             GROUP BY UPPER(TRIM(npa_status))
#             ORDER BY total_outstanding DESC
#         """, "NPA classification summary")

#     if _table_exists(conn, "bronze", "cibil_bureau"):
#         _run_and_show(conn, """
#             SELECT
#                 customer_id, customer_name,
#                 TRY_CAST(cibil_score AS INTEGER) AS cibil_score,
#                 TRY_CAST(total_outstanding AS DECIMAL(18,2)) AS total_outstanding,
#                 TRY_CAST(dpd_90_count AS INTEGER) AS dpd_90_count,
#                 wilful_defaulter, report_pull_date
#             FROM iceberg.bronze.cibil_bureau
#             WHERE TRY_CAST(cibil_score AS INTEGER) < 600
#             ORDER BY TRY_CAST(cibil_score AS INTEGER) ASC
#             LIMIT 20
#         """, "Low CIBIL customers (<600)")


# _SECTIONS = {
#     "counts": _section_counts,
#     "txn":    _section_txn,
#     "risk":   _section_risk,
# }


# # ----------------------------------------------------------------
# # PUBLIC ENTRY POINT
# # ----------------------------------------------------------------
# def run(section: Optional[str] = None) -> int:
#     """
#     Run Trino analytics queries.

#     Args:
#         section: "counts", "txn", "risk", or None (= all sections)
#     Returns:
#         0 on success, non-zero on failure
#     """
#     print_section(f"TRINO QUERY CLIENT")
#     log.info("Started at: %s", datetime.now().isoformat())

#     try:
#         with _connect() as conn:
#             # Sanity check: iceberg catalog
#             df = _query(conn, "SHOW CATALOGS")
#             catalogs = df["Catalog"].tolist() if df is not None and not df.empty else []
#             log.info("Available catalogs: %s", catalogs)

#             if "iceberg" not in catalogs:
#                 log.error("'iceberg' catalog not found in Trino")
#                 log.error("Check Trino configmap — iceberg.properties must be mounted")
#                 return 1

#             # Sanity check: bronze schema
#             df = _query(conn, "SHOW SCHEMAS FROM iceberg")
#             schemas = df["Schema"].tolist() if df is not None and not df.empty else []
#             log.info("Schemas: %s", schemas)

#             if "bronze" not in schemas:
#                 log.error("'bronze' schema not found")
#                 log.error("Run kafka consumer first: --mode kafka")
#                 return 1

#             log.info("Bronze schema found — proceeding")

#             if section:
#                 log.info("Running section: %s", section)
#                 _SECTIONS[section](conn)
#             else:
#                 log.info("Running ALL sections")
#                 for name, fn in _SECTIONS.items():
#                     log.info("========== %s ==========", name)
#                     try:
#                         fn(conn)
#                     except Exception as e:
#                         log.error("Section '%s' crashed: %s", name, e)

#         log.info("✓ Done")
#         return 0

#     except Exception as e:
#         log.exception("Fatal: %s", e)
#         return 2