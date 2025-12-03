"""
Command-line interface for duckdb_benchmark.

Provides CLI entry point with explicit configuration requirement.
"""

import argparse
import json
import sys
from pathlib import Path

from . import __version__
from .benchmark import Benchmark
from .config import load_config
from .data_generator import DataGenerator


def create_parser() -> argparse.ArgumentParser:
    """Create and return the argument parser."""
    parser = argparse.ArgumentParser(
        prog="duckdb-benchmark",
        description="DuckDB TPC-H benchmark tool - requires explicit configuration",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Generate data command
    generate_parser = subparsers.add_parser(
        "generate",
        help="Generate TPC-H data",
    )
    generate_parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to configuration file (required)",
    )

    # Run benchmark command
    run_parser = subparsers.add_parser(
        "run",
        help="Run TPC-H benchmark",
    )
    run_parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to configuration file (required)",
    )

    # Init config command - creates a sample config file
    init_parser = subparsers.add_parser(
        "init",
        help="Create a sample configuration file",
    )
    init_parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Path to write sample configuration file",
    )

    return parser


def cmd_generate(config_path: Path) -> int:
    """Execute the generate command."""
    try:
        config = load_config(config_path)
        generator = DataGenerator(config)

        if generator.data_exists():
            print(f"Data already exists at {generator.get_db_path()}")
            return 0

        db_path = generator.generate()
        print(f"Data generated at {db_path}")
        return 0
    except FileExistsError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_run(config_path: Path) -> int:
    """Execute the run command."""
    try:
        config = load_config(config_path)
        benchmark = Benchmark(config)

        print(f"Running TPC-H benchmark with scale factor {config.scale_factor}...")
        results = benchmark.run()

        # Print summary
        success_count = sum(1 for r in results if r.success)
        print(f"Executed {len(results)} query iterations ({success_count} successful)")

        output_file = benchmark.save_results()
        print(f"Results saved to {output_file}")
        return 0
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def cmd_init(output_path: Path) -> int:
    """Create a sample configuration file."""
    sample_config = {
        "scale_factor": 1.0,
        "data_path": "./data",
        "output_path": "./results",
        "iterations": 3,
        "queries": list(range(1, 23)),
        "tpch_extension_path": None,
    }

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(sample_config, f, indent=2)
        print(f"Sample configuration written to {output_path}")
        print("Edit this file to customize your benchmark settings.")
        return 0
    except Exception as e:
        print(f"Error writing config: {e}", file=sys.stderr)
        return 1


def main(argv: list[str] | None = None) -> int:
    """Main entry point for the CLI."""
    parser = create_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 0

    if args.command == "generate":
        return cmd_generate(args.config)
    elif args.command == "run":
        return cmd_run(args.config)
    elif args.command == "init":
        return cmd_init(args.output)

    return 0


if __name__ == "__main__":
    sys.exit(main())
