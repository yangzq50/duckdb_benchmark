"""
Configuration module for duckdb_benchmark.

Provides a configuration class and loader with no hidden defaults.
All configuration values must be explicitly provided.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
import json


@dataclass
class BenchmarkConfig:
    """
    Configuration for DuckDB TPC-H benchmarks.
    
    All fields are required - no hidden defaults to ensure explicit configuration.
    
    Attributes:
        scale_factor: TPC-H scale factor (e.g., 1, 10, 100)
        data_path: Path where TPC-H data files will be stored
        output_path: Path where benchmark results will be written
        iterations: Number of benchmark iterations per query
        queries: List of TPC-H query numbers to run (1-22)
        load_tpch_extension: Whether to load the TPC-H extension
        in_memory: Whether to run DuckDB in memory-only mode
    """
    scale_factor: float
    data_path: Path
    output_path: Path
    iterations: int
    queries: list[int]
    load_tpch_extension: bool
    in_memory: bool
    
    def __post_init__(self) -> None:
        """Validate and convert configuration values."""
        self.data_path = Path(self.data_path)
        self.output_path = Path(self.output_path)
        
        if self.scale_factor <= 0:
            raise ValueError("scale_factor must be positive")
        if self.iterations <= 0:
            raise ValueError("iterations must be positive")
        if not self.queries:
            raise ValueError("queries list cannot be empty")
        for q in self.queries:
            if not 1 <= q <= 22:
                raise ValueError(f"query {q} must be between 1 and 22")


def load_config(config_path: Path) -> BenchmarkConfig:
    """
    Load benchmark configuration from a JSON file.
    
    Args:
        config_path: Path to the JSON configuration file
        
    Returns:
        BenchmarkConfig instance with loaded values
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        json.JSONDecodeError: If config file is not valid JSON
        TypeError: If required fields are missing
        ValueError: If field values are invalid
    """
    with open(config_path, "r") as f:
        data = json.load(f)
    
    return BenchmarkConfig(
        scale_factor=data["scale_factor"],
        data_path=Path(data["data_path"]),
        output_path=Path(data["output_path"]),
        iterations=data["iterations"],
        queries=data["queries"],
        load_tpch_extension=data["load_tpch_extension"],
        in_memory=data["in_memory"],
    )
