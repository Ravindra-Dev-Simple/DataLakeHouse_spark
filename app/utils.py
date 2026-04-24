"""
================================================================
 utils.py — Shared helpers (logging, error formatting)
================================================================
"""
import logging
import sys
from functools import lru_cache


@lru_cache(maxsize=1)
def get_logger(name: str = "lakehouse") -> logging.Logger:
    """
    Pre-configured logger. LRU cache ensures one instance per name.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%H:%M:%S"
    )
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def print_section(title: str, width: int = 72, char: str = "#"):
    """Section header print karo terminal mein."""
    print("\n" + char * width)
    print(f"{char} {title}")
    print(char * width)


def print_subsection(title: str, width: int = 72):
    """Smaller subsection header."""
    print(f"\n{'=' * width}\n>>> {title}\n{'=' * width}")


def format_number(n) -> str:
    """Large numbers formatted with commas (Indian style)."""
    try:
        return f"{int(n):,}"
    except (TypeError, ValueError):
        return str(n)