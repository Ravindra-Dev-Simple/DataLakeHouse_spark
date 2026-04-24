"""
================================================================
 index.py — CLI entrypoint
================================================================
 Routes between produce, kafka consumer, and trino client.

 Usage:
   python -m app.index --mode produce --count 10000
   python -m app.index --mode kafka --topic finacle-transactions
   python -m app.index --mode trino --section counts
================================================================
"""
import argparse
import sys

from app import config
from app.utils import get_logger

log = get_logger("index")


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="lakehouse",
        description="Banking Lakehouse — producer + consumer + query client",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  --mode produce --count 10000                   # generate 10k test transactions
  --mode produce --count 50000 --days 60         # 50k over 60 days
  --mode produce --count 100 --dry-run           # print samples, don't publish
  --mode kafka                                   # Kafka → Iceberg (batch)
  --mode kafka --topic finacle-transactions      # Specific topic
  --mode trino                                   # All Trino sections
  --mode trino --section counts                  # One section

Environment variables override all defaults.
        """
    )

    parser.add_argument(
        "--mode", required=True,
        choices=["produce", "kafka", "trino"],
        help="Pipeline mode: 'produce' (generate) / 'kafka' (ingest) / 'trino' (query)"
    )

    # Producer-mode options
    parser.add_argument("--count", type=int, default=10000,
                        help="[produce] Number of transactions to generate (default: 10000)")
    parser.add_argument("--days", type=int, default=30,
                        help="[produce] Spread transactions across N days (default: 30)")
    parser.add_argument("--dry-run", action="store_true",
                        help="[produce] Print samples instead of publishing")

    # Kafka-mode options
    parser.add_argument("--topic", default=None,
                        help="[kafka] Specific Kafka topic (default: from KAFKA_TOPIC env)")
    parser.add_argument("--consume-mode", default="batch",
                        choices=["batch", "streaming"],
                        help="[kafka] batch = read all then stop; streaming = continuous")
    parser.add_argument("--checkpoint-dir", default="/tmp/kafka-iceberg-checkpoints",
                        help="[kafka] Checkpoint dir for streaming")

    # Trino-mode options
    parser.add_argument("--section", default=None,
                        choices=["counts", "txn", "risk"],
                        help="[trino] Single section to run (default: all)")

    parser.add_argument("--show-config", action="store_true",
                        help="Print resolved configuration and exit")

    args = parser.parse_args()

    if args.show_config:
        config.print_config()
        return 0

    config.print_config()

    # Dispatch
    if args.mode == "produce":
        from app import kafka_producer
        return kafka_producer.run(
            count=args.count,
            days=args.days,
            dry_run=args.dry_run,
        )

    elif args.mode == "kafka":
        from app import kafka_consumer
        return kafka_consumer.run(
            mode=args.consume_mode,
            topic=args.topic,
            checkpoint_dir=args.checkpoint_dir,
        )

    elif args.mode == "trino":
        from app import trino_client
        return trino_client.run(section=args.section)

    else:
        log.error("Unknown mode: %s", args.mode)
        return 1


if __name__ == "__main__":
    sys.exit(main())