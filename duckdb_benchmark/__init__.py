"""
duckdb_benchmark - A clean, decoupled Python module for DuckDB TPC-H benchmarks.

This module provides configurable TPC-H data generation and benchmarking
workflows for DuckDB. It supports configurable data persistence, benchmark
output paths, query/iteration settings, and TPC-H extension loading.
"""

__version__ = "0.1.0"

from duckdb_benchmark.benchmark import Benchmark
from duckdb_benchmark.config import BenchmarkConfig, load_config
from duckdb_benchmark.data_generator import DataGenerator
from duckdb_benchmark.load_tpch_extension import (
    download_tpch_extension,
    install_and_load_tpch,
    load_tpch_extension_from_path,
)

__all__ = [
    "__version__",
    "BenchmarkConfig",
    "load_config",
    "DataGenerator",
    "Benchmark",
    "download_tpch_extension",
    "install_and_load_tpch",
    "load_tpch_extension_from_path",
]
