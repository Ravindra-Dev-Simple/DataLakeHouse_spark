"""
================================================================
 index.py — CLI entrypoint
================================================================
 Routes between kafka consumer and trino client based on --mode.

 Usage:
   python -m app.index --mode kafka
   python -m app.index --mode kafka --topic finacle-transactions
   python -m app.index --mode kafka --consume-mode streaming
   python -m app.index --mode trino
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
        description="Banking Lakehouse — Kafka consumer + Trino query client",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  --mode kafka                                    # Kafka → Iceberg (batch)
  --mode kafka --topic finacle-transactions       # Specific topic
  --mode kafka --consume-mode streaming           # Continuous stream
  --mode trino                                    # All Trino sections
  --mode trino --section counts                   # One section

Environment variables override all defaults.
See config.py for the full list.
        """
    )

    parser.add_argument("--mode", required=True, choices=["kafka", "trino"],
                        help="Pipeline mode: 'kafka' (ingest) or 'trino' (query)")

    # Kafka-mode options
    parser.add_argument("--topic", default=None,
                        help="[kafka] Specific Kafka topic (default: from KAFKA_TOPIC env)")
    parser.add_argument("--consume-mode", default="batch", choices=["batch", "streaming"],
                        help="[kafka] batch = read all then stop; streaming = continuous")
    parser.add_argument("--checkpoint-dir", default="/tmp/kafka-iceberg-checkpoints",
                        help="[kafka] Checkpoint dir for streaming")

    # Trino-mode options
    parser.add_argument("--section", default=None, choices=["counts", "txn", "risk"],
                        help="[trino] Single section to run (default: all)")

    # Global options
    parser.add_argument("--show-config", action="store_true",
                        help="Print resolved configuration and exit")

    args = parser.parse_args()

    # Show-config short-circuit
    if args.show_config:
        config.print_config()
        return 0

    # Always show config at startup
    config.print_config()

    # Dispatch
    if args.mode == "kafka":
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