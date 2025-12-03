"""
duckdb_benchmark - A clean, decoupled Python module for DuckDB TPC-H benchmarks.

This module provides configurable TPC-H data generation and benchmarking
workflows for DuckDB. It supports configurable data persistence, benchmark
output paths, query/iteration settings, and TPC-H extension loading.
"""

__version__ = "0.1.0"

from .benchmark import Benchmark
from .config import BenchmarkConfig, load_config
from .data_generator import DataGenerator
from .load_tpch_extension import load_tpch_extension

__all__ = [
    "__version__",
    "BenchmarkConfig",
    "load_config",
    "DataGenerator",
    "Benchmark",
    "load_tpch_extension",
]
