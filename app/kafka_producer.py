"""
================================================================
 kafka_producer.py — Generate realistic banking transactions
 and publish them to Kafka topics for testing / bulk loading.
================================================================
"""
import json
import os
import random
import time
from datetime import datetime, timedelta
from typing import List, Dict

from app import config
from app.utils import get_logger, print_section

log = get_logger("producer")


# ----------------------------------------------------------------
# Static data pools
# ----------------------------------------------------------------
BRANCHES = ["BR001", "BR002", "BR003", "BR004", "BR005"]
CHANNELS = ["NEFT", "RTGS", "UPI", "IMPS", "ATM", "POS", "CASH", "INTERNET_BANKING"]
STATUSES = ["SUCCESS"] * 8 + ["FLAGGED", "FAILED"]   # 80/10/10
TXN_TYPES = ["CREDIT", "DEBIT"]
CUSTOMER_IDS = [f"CUST{str(i).zfill(6)}" for i in range(1, 501)]
ACCOUNT_IDS = [f"ACCT{str(i).zfill(6)}" for i in range(1, 501)]

NARRATIONS_CREDIT = [
    "SALARY {month} 2026 - {company}",
    "NEFT CREDIT FROM {name}",
    "RENTAL INCOME",
    "FD MATURITY CREDIT",
    "MUTUAL FUND REDEMPTION",
    "BUSINESS INCOME",
    "GST REFUND",
    "INSURANCE CLAIM SETTLEMENT",
    "PENSION CREDIT",
    "DIVIDEND INCOME",
]
NARRATIONS_DEBIT = [
    "EMI PAYMENT - {loan_type}",
    "RENT PAYMENT",
    "ELECTRICITY BILL - {provider}",
    "UPI TRANSFER TO {name}",
    "ATM CASH WITHDRAWAL",
    "INSURANCE PREMIUM - {company}",
    "CREDIT CARD BILL PAYMENT",
    "SIP INVESTMENT",
    "GST PAYMENT",
    "SUPPLIER PAYMENT",
    "SCHOOL FEE",
    "MEDICAL BILL",
    "GROCERY PURCHASE",
    "FUEL STATION",
]

COMPANIES = ["INFOSYS", "TCS", "WIPRO", "HCL", "COGNIZANT", "ACCENTURE",
             "MINDTREE", "TECH MAHINDRA", "MPHASIS", "L&T INFOTECH"]
NAMES = ["Rajesh", "Priya", "Arun", "Kavitha", "Suresh", "Deepa",
         "Venkatesh", "Lakshmi", "Karthik", "Anjali", "Mohammed", "Pooja"]
PROVIDERS = ["BESCOM", "TANGEDCO", "TSSPDCL", "CESC", "BSES", "MSEDCL"]
LOAN_TYPES = ["HOME LOAN", "PERSONAL LOAN", "CAR LOAN", "BUSINESS LOAN"]
MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
          "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]

CHANNEL_RANGES = {
    "ATM":              (500, 25000),
    "UPI":              (10, 100000),
    "POS":              (100, 50000),
    "IMPS":             (100, 200000),
    "NEFT":             (1000, 1000000),
    "RTGS":             (200000, 10000000),
    "CASH":             (1000, 500000),
    "INTERNET_BANKING": (100, 500000),
}


# ----------------------------------------------------------------
# Generators
# ----------------------------------------------------------------
def _generate_amount(channel: str) -> float:
    """Realistic amount per channel; 2% chance of suspicious high-value."""
    low, high = CHANNEL_RANGES.get(channel, (100, 100000))
    amount = round(random.uniform(low, high), 2)
    if random.random() < 0.02:
        amount = round(random.uniform(900000, 5000000), 2)
    return amount


def _generate_narration(txn_type: str) -> str:
    template = random.choice(NARRATIONS_CREDIT if txn_type == "CREDIT" else NARRATIONS_DEBIT)
    return template.format(
        month=random.choice(MONTHS),
        company=random.choice(COMPANIES),
        name=random.choice(NAMES),
        provider=random.choice(PROVIDERS),
        loan_type=random.choice(LOAN_TYPES),
    )


def _generate_transaction(seq_num: int, base_date: datetime) -> Dict:
    """One realistic Finacle-style transaction."""
    txn_type = random.choice(TXN_TYPES)
    channel = random.choice(CHANNELS)
    idx = random.randint(0, len(CUSTOMER_IDS) - 1)

    offset = random.randint(0, 86400)
    txn_time = base_date + timedelta(seconds=offset)

    amount = _generate_amount(channel)
    balance = round(random.uniform(10000, 5000000), 2)
    status = random.choice(STATUSES)
    if channel == "CASH" and amount > 500000:
        status = "FLAGGED"

    return {
        "txn_id":        f"TXN{base_date.strftime('%Y%m%d')}{str(seq_num).zfill(6)}",
        "account_id":    ACCOUNT_IDS[idx],
        "customer_id":   CUSTOMER_IDS[idx],
        "txn_type":      txn_type,
        "amount":        str(amount),
        "currency":      "INR",
        "txn_date":      txn_time.strftime("%Y-%m-%dT%H:%M:%S"),
        "value_date":    txn_time.strftime("%Y-%m-%d"),
        "branch_code":   random.choice(BRANCHES),
        "channel":       channel,
        "narration":     _generate_narration(txn_type),
        "balance_after": str(balance),
        "status":        status,
        "ref_number":    f"{channel}{base_date.strftime('%Y%m%d')}{str(seq_num).zfill(6)}",
    }


def _generate_aml_alert(txn: Dict) -> Dict:
    """AML alert record when a transaction is FLAGGED."""
    alert_types = [
        ("STRUCTURING",      "RULE_CTR_SPLIT",       "Multiple transactions structured to avoid CTR threshold"),
        ("HIGH_VALUE",       "RULE_HIGH_VALUE",      f"High value {txn['channel']} of INR {txn['amount']}"),
        ("VELOCITY",         "RULE_HIGH_VELOCITY",   "Unusual transaction velocity detected on account"),
        ("UNUSUAL_PATTERN",  "RULE_PATTERN_BREAK",   "Transaction pattern deviates from customer profile"),
    ]
    alert_type, rule, desc = random.choice(alert_types)
    return {
        "alert_id":        f"AML{txn['txn_date'][:10].replace('-', '')}{random.randint(10000, 99999)}",
        "customer_id":     txn["customer_id"],
        "customer_name":   random.choice(NAMES),
        "alert_date":      txn["txn_date"],
        "alert_type":      alert_type,
        "risk_score":      str(random.randint(60, 99)),
        "rule_triggered":  rule,
        "transaction_ids": txn["txn_id"],
        "total_amount":    txn["amount"],
        "currency":        "INR",
        "description":     desc,
        "status":          random.choice(["OPEN", "UNDER_REVIEW"]),
        "priority":        random.choice(["CRITICAL", "HIGH", "MEDIUM"]),
    }


# ----------------------------------------------------------------
# Kafka producer
# ----------------------------------------------------------------
def _build_producer():
    """Lazy import + configure KafkaProducer from env config."""
    try:
        from kafka import KafkaProducer
    except ImportError:
        log.error("kafka-python not installed. pip install kafka-python")
        raise

    log.info("Connecting to Kafka: %s", config.KAFKA_BOOTSTRAP_SERVERS)

    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        security_protocol=config.KAFKA_SECURITY_PROTOCOL,
        sasl_mechanism=config.KAFKA_SASL_MECHANISM,
        sasl_plain_username=config.KAFKA_USERNAME,
        sasl_plain_password=config.KAFKA_PASSWORD,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=3,
        batch_size=32768,
        linger_ms=10,
        api_version_auto_timeout_ms=30000,
        request_timeout_ms=60000,
        compression_type="gzip",
    )


# ----------------------------------------------------------------
# PUBLIC ENTRY POINT
# ----------------------------------------------------------------
def run(count: int = 10000, days: int = 30, dry_run: bool = False) -> int:
    """
    Generate `count` synthetic transactions spread across `days`, and
    publish them (and any resulting AML alerts) to Kafka.

    Returns: 0 on success, non-zero on failure.
    """
    print_section(f"KAFKA PRODUCER — generating {count} transactions across {days} days")
    log.info("Started at: %s", datetime.now().isoformat())

    base_date = datetime(2026, 3, 15)
    transactions: List[Dict] = []
    aml_alerts: List[Dict] = []

    log.info("Generating %d transactions...", count)
    for i in range(1, count + 1):
        day_offset = random.randint(0, days - 1)
        txn_date = base_date + timedelta(days=day_offset)
        txn = _generate_transaction(i, txn_date)
        transactions.append(txn)

        if txn["status"] == "FLAGGED":
            aml_alerts.append(_generate_aml_alert(txn))

        if i % 5000 == 0:
            log.info("  ...%d generated", i)

    log.info("Generated %d transactions, %d AML alerts",
             len(transactions), len(aml_alerts))

    # Dry-run — just print sample
    if dry_run:
        log.info("DRY RUN: showing first 3 transactions, not publishing")
        for txn in transactions[:3]:
            print(json.dumps(txn, indent=2))
        log.info("... and %d more. AML alerts: %d", len(transactions) - 3, len(aml_alerts))
        return 0

    # Publish to Kafka
    try:
        producer = _build_producer()
    except Exception as e:
        log.error("Failed to create Kafka producer: %s", e)
        return 1

    start = time.time()
    try:
        log.info("Publishing %d transactions to 'finacle-transactions'...", len(transactions))
        for txn in transactions:
            producer.send("finacle-transactions", key=txn["txn_id"], value=txn)

        if aml_alerts:
            log.info("Publishing %d alerts to 'aml-alerts'...", len(aml_alerts))
            for alert in aml_alerts:
                producer.send("aml-alerts", key=alert["alert_id"], value=alert)

        log.info("Flushing...")
        producer.flush()
        producer.close()

    except Exception as e:
        log.error("Publish failed: %s", e)
        return 2

    elapsed = time.time() - start
    throughput = len(transactions) / elapsed if elapsed > 0 else 0

    print("\n" + "=" * 72)
    log.info("✓ Published successfully")
    log.info("  Transactions: %d → finacle-transactions", len(transactions))
    log.info("  AML Alerts:   %d → aml-alerts", len(aml_alerts))
    log.info("  Time:         %.2fs", elapsed)
    log.info("  Throughput:   %.0f msg/s", throughput)
    print("=" * 72)
    return 0