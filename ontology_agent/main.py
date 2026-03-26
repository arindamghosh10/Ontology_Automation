"""
CLI entry point for the Ontology Automation Agent.
Usage: python -m ontology_agent --file merchants.xlsx [--batch-size 20] [--start-row 2]
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from .config import Config
from .pipeline import run_pipeline


def setup_logging(verbose: bool = False):
    """Configure logging for the agent."""
    level = logging.DEBUG if verbose else logging.INFO

    # Create formatters
    console_fmt = logging.Formatter(
        "%(asctime)s │ %(levelname)-7s │ %(message)s",
        datefmt="%H:%M:%S"
    )
    file_fmt = logging.Formatter(
        "%(asctime)s │ %(name)-20s │ %(levelname)-7s │ %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(console_fmt)

    # File handler
    file_handler = logging.FileHandler("ontology_agent.log", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_fmt)

    # Root logger
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(console)
    root.addHandler(file_handler)

    # Suppress noisy loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("playwright").setLevel(logging.WARNING)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Ontology Automation Agent — Merchant Data Enrichment Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m ontology_agent --file merchants.xlsx
  python -m ontology_agent --file data.xlsx --batch-size 10 --start-row 5
  python -m ontology_agent --file data.xlsx --sheet "Sheet1" --verbose
        """,
    )
    parser.add_argument(
        "--file", "-f",
        required=True,
        help="Path to the Excel file containing merchant records",
    )
    parser.add_argument(
        "--sheet", "-s",
        default="Sheet1",
        help="Sheet name to read merchants from (default: Sheet1)",
    )
    parser.add_argument(
        "--batch-size", "-b",
        type=int,
        default=20,
        help="Number of merchants per batch (default: 20)",
    )
    parser.add_argument(
        "--start-row", "-r",
        type=int,
        default=0,
        help="Start from this Excel row number (0 = start from beginning)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose/debug logging",
    )
    return parser.parse_args()


async def async_main():
    """Async main entry point."""
    args = parse_args()
    setup_logging(args.verbose)

    logger = logging.getLogger(__name__)

    # Validate file exists
    filepath = Path(args.file).resolve()
    if not filepath.exists():
        logger.error(f"File not found: {filepath}")
        sys.exit(1)

    if not filepath.suffix.lower() in (".xlsx", ".xls"):
        logger.error(f"File must be .xlsx or .xls: {filepath}")
        sys.exit(1)

    # Load config
    config = Config.load()
    config.pipeline.batch_size = args.batch_size

    # Show config warnings
    warnings = config.validate()
    for w in warnings:
        logger.warning(f"⚠ {w}")

    if not config.llm.api_key:
        logger.error("OPENROUTER_API_KEY not set. Please set it in .env or environment.")
        logger.error("Example: OPENROUTER_API_KEY=sk-or-v1-xxxx")
        sys.exit(1)

    logger.info(f"File: {filepath}")
    logger.info(f"Sheet: {args.sheet}")
    logger.info(f"Batch size: {args.batch_size}")
    logger.info(f"LLM model: {config.llm.model}")

    # Run pipeline
    summary = await run_pipeline(
        filepath=str(filepath),
        config=config,
        start_row=args.start_row,
        sheet_name=args.sheet,
    )

    return summary


def main():
    """Sync main entry point."""
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Exiting...")
        sys.exit(0)
    except Exception as e:
        print(f"\nFatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
